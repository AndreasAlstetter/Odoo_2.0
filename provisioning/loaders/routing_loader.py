"""
routing_loader.py - Manufacturing Routing Orchestration

Lädt Fertigungspläne (Routings) mit:
- Workcenter-Kapazitäten
- Operations mit Sequenzierung
- Varianten-Support
- Validierung des kompletten Produktionsflusses
- Audit-Trail für Compliance
"""

import csv
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple, Set
from decimal import Decimal, InvalidOperation
from datetime import datetime

from client import OdooClient, RecordAmbiguousError, ValidationError
from config import (
    DataPaths,
    ManufacturingConfig,
    ProductTemplates,
    CSVConfig,
)
from utils import log_header, log_info, log_success, log_warn, log_error


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class RoutingError(Exception):
    """Base exception for routing operations."""
    pass


class WorkcenterNotFoundError(RoutingError):
    """Workcenter not found."""
    pass


class RoutingValidationError(RoutingError):
    """Routing validation failed."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# WORKCENTER MAPPER
# ═══════════════════════════════════════════════════════════════════════════════

class WorkcenterMapper:
    """Map Workcenter codes from CSV to Odoo Workcenter IDs."""
    
    @staticmethod
    def load_mapping_from_config() -> Dict[str, str]:
        """
        Load Workcenter mapping from config.
        
        Returns:
            Dict: csv_code → workcenter_name
            
        Example:
            {'WC-3D': '3D-Drucker', 'WC-LC': 'Lasercutter', ...}
        """
        return ManufacturingConfig.WORKCENTERS.keys()
    
    @staticmethod
    def load_mapping_from_csv(csv_path: Path) -> Dict[str, str]:
        """
        Load Workcenter mapping from external CSV.
        
        CSV Format:
            workcenter_code,workcenter_name
            WC-3D,3D-Drucker
            WC-LC,Lasercutter
        
        Args:
            csv_path: Path to workcenter_mapping.csv
        
        Returns:
            Dict: code → name
        
        Raises:
            FileNotFoundError: Wenn Datei nicht existiert
        """
        if not csv_path.exists():
            raise FileNotFoundError(f"Workcenter mapping CSV missing: {csv_path}")
        
        mapping = {}
        
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                
                for row_idx, row in enumerate(reader, start=2):
                    code = row.get('workcenter_code', '').strip()
                    name = row.get('workcenter_name', '').strip()
                    
                    if not code or not name:
                        logger.warning(f"Workcenter mapping row {row_idx}: empty fields")
                        continue
                    
                    mapping[code] = name
        
        except csv.Error as e:
            raise RoutingError(f"Workcenter mapping CSV error: {e}")
        
        return mapping


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTING VALIDATOR
# ═══════════════════════════════════════════════════════════════════════════════

class RoutingValidator:
    """Validate routing completeness and consistency."""
    
    @staticmethod
    def validate_routing_completeness(
        routing_id: int,
        operations: List[Dict[str, Any]],
        client: OdooClient,
    ) -> Tuple[bool, List[str]]:
        """
        Validate dass Routing komplett ist.
        
        Args:
            routing_id: Odoo Routing ID
            operations: Expected operations
            client: Odoo client
        
        Returns:
            (is_valid, error_messages)
        """
        errors = []
        
        # Check: Operation sequence hat keine Lücken
        sequences = sorted([op['sequence'] for op in operations if 'sequence' in op])
        
        if sequences:
            # 1, 2, 3, 4, 5 OK
            # 1, 3, 5 → Lücken!
            expected_sequences = list(range(min(sequences), max(sequences) + 1))
            if sequences != expected_sequences:
                missing = set(expected_sequences) - set(sequences)
                errors.append(f"Missing operation sequences: {missing}")
        
        # Check: Alle Workcenters existieren
        for op in operations:
            if 'workcenter_id' not in op or not op['workcenter_id']:
                errors.append(f"Operation {op.get('name')}: no workcenter assigned")
        
        # Check: Kapazität ausreichend für Produktionsvolumen
        for op in operations:
            capacity = op.get('capacity', 1)
            if capacity < 5:  # Minimum für >500 Drohnen/Tag
                errors.append(
                    f"Operation {op.get('name')}: "
                    f"capacity {capacity} too low for 500 units/day"
                )
        
        return len(errors) == 0, errors


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTING LOADER
# ═══════════════════════════════════════════════════════════════════════════════

class RoutingLoader:
    """Manufacturing Routing Orchestration."""
    
    def __init__(self, client: OdooClient, base_data_dir: str):
        self.client = client
        self.base_data_dir = Path(base_data_dir)
        self.routing_dir = self.base_data_dir / 'routing_data'
        
        # Get company
        companies = self.client.search_read('res.company', [], ['id'], limit=1)
        if not companies:
            raise RuntimeError("No company found in Odoo")
        self.company_id = companies[0]['id']
        
        # Statistics
        self.stats = {
            'workcenters_created': 0,
            'workcenters_updated': 0,
            'routings_created': 0,
            'routings_updated': 0,
            'operations_created': 0,
            'operations_updated': 0,
            'validation_errors': 0,
        }
        
        # Audit log
        self.audit_log: List[Dict[str, Any]] = []
        
        logger.info(f"RoutingLoader initialized: company_id={self.company_id}")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PRODUCT LOOKUP
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _find_products_by_codes(self, codes: List[str]) -> Dict[str, int]:
        """Batch lookup products by code."""
        if not codes:
            return {}
        
        products = self.client.search_read(
            'product.template',
            [('default_code', 'in', codes)],
            ['default_code', 'id'],
            limit=len(codes) * 2  # Safety margin
        )
        
        return {p['default_code']: p['id'] for p in products}
    
    def _find_bom_by_product_id(self, product_id: int) -> Optional[int]:
        """Find BoM for given product."""
        boms = self.client.search_read(
            'mrp.bom',
            [('product_tmpl_id', '=', product_id)],
            ['id'],
            limit=1
        )
        
        if boms:
            return boms[0]['id']
        return None
    
    # ═══════════════════════════════════════════════════════════════════════════
    # WORKCENTER MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _load_workcenters_from_csv(self) -> Dict[str, int]:
        """Load workcenters from CSV with validation."""
        wc_path = self.routing_dir / 'workcenter.csv'
        
        if not wc_path.exists():
            log_warn(f"Workcenter CSV missing: {wc_path}, using config defaults")
            return self._create_workcenters_from_config()
        
        log_header("Loading Workcenters from CSV")
        
        workcenters = {}
        
        try:
            with open(wc_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=CSVConfig.DELIMITER_PRIMARY)
                
                for row_idx, row in enumerate(reader, start=2):
                    wc_name = row.get('name', '').strip()
                    
                    if not wc_name:
                        logger.warning(f"Row {row_idx}: missing name")
                        continue
                    
                    try:
                        # Parse numeric fields
                        try:
                            costs_hour = Decimal(row.get('cost_per_hour', '0'))
                            capacity = Decimal(row.get('capacity', '1'))
                            efficiency = Decimal(row.get('time_efficiency', '1'))
                        except InvalidOperation as e:
                            logger.warning(f"Row {row_idx}: numeric parse error: {e}")
                            continue
                        
                        # Validate
                        if capacity <= 0 or capacity > 1000:
                            logger.warning(f"Row {row_idx}: invalid capacity {capacity}")
                            continue
                        
                        # Create/Update
                        vals = {
                            'name': wc_name,
                            'code': row.get('code', '').strip()[:20],
                            'costs_hour': float(costs_hour),
                            'capacity': float(capacity),
                            'time_efficiency': float(efficiency),
                            'blocking': row.get('blocking', 'no').strip(),
                            'company_id': self.company_id,
                        }
                        
                        wc_id, is_new = self.client.ensure_record(
                            'mrp.workcenter',
                            [('name', '=', wc_name), ('company_id', '=', self.company_id)],
                            vals,
                            vals,
                        )
                        
                        workcenters[wc_name] = wc_id
                        
                        if is_new:
                            self.stats['workcenters_created'] += 1
                            log_success(f"Created workcenter: {wc_name} → {wc_id}")
                        else:
                            self.stats['workcenters_updated'] += 1
                            log_info(f"Updated workcenter: {wc_name} → {wc_id}")
                        
                        self._audit_log({
                            'action': 'workcenter_created' if is_new else 'workcenter_updated',
                            'workcenter_name': wc_name,
                            'workcenter_id': wc_id,
                            'capacity': float(capacity),
                        })
                    
                    except Exception as e:
                        logger.error(f"Row {row_idx}: {e}", exc_info=True)
                        continue
        
        except csv.Error as e:
            raise RoutingError(f"Workcenter CSV error: {e}")
        
        return workcenters
    
    def _create_workcenters_from_config(self) -> Dict[str, int]:
        """Create workcenters from config if CSV missing."""
        log_header("Creating Workcenters from Config")
        
        workcenters = {}
        
        for wc_code, wc_config in ManufacturingConfig.WORKCENTERS.items():
            wc_name = wc_config['name']
            capacity = wc_config['capacity']
            efficiency = wc_config['efficiency']
            
            vals = {
                'name': wc_name,
                'code': wc_code,
                'capacity': capacity,
                'time_efficiency': efficiency,
                'company_id': self.company_id,
            }
            
            wc_id, is_new = self.client.ensure_record(
                'mrp.workcenter',
                [('name', '=', wc_name), ('company_id', '=', self.company_id)],
                vals,
                vals,
            )
            
            workcenters[wc_name] = wc_id
            
            if is_new:
                self.stats['workcenters_created'] += 1
            
            log_success(f"{'[NEW]' if is_new else '[UPD]'} {wc_name}")
        
        return workcenters
    
    # ═══════════════════════════════════════════════════════════════════════════
    # ROUTING MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _ensure_routing_for_bom(self, bom_id: int) -> int:
        """Get or create Routing for BoM."""
        # Routing 1:1 zu BoM
        routings = self.client.search_read(
            'mrp.routing',
            [('bom_id', '=', bom_id)],
            ['id'],
            limit=1
        )
        
        if routings:
            return routings[0]['id']
        
        # Create new routing
        routing_id = self.client.create(
            'mrp.routing',
            {
                'name': f'Routing for BoM {bom_id}',
                'bom_id': bom_id,
                'company_id': self.company_id,
            }
        )
        
        self.stats['routings_created'] += 1
        self._audit_log({
            'action': 'routing_created',
            'bom_id': bom_id,
            'routing_id': routing_id,
        })
        
        logger.info(f"Created routing: {routing_id} for BoM {bom_id}")
        
        return routing_id
    
    # ═══════════════════════════════════════════════════════════════════════════
    # OPERATION MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _load_operations_from_csv(
        self,
        routing_id: int,
        workcenters: Dict[str, int],
    ) -> List[Dict[str, Any]]:
        """Load operations for routing from CSV."""
        ops_path = self.routing_dir / 'operations.csv'
        
        if not ops_path.exists():
            log_warn(f"Operations CSV missing: {ops_path}")
            return []
        
        operations = []
        fallback_wc_id = self._get_fallback_workcenter_id(workcenters)
        
        try:
            with open(ops_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=CSVConfig.DELIMITER_PRIMARY)
                
                for row_idx, row in enumerate(reader, start=2):
                    op_name = row.get('name', '').strip()
                    
                    if not op_name:
                        logger.warning(f"Row {row_idx}: missing operation name")
                        continue
                    
                    try:
                        # Parse fields
                        sequence = int(row.get('sequence', 999))
                        wc_name = row.get('workcenter_name', '').strip()
                        
                        # Resolve workcenter
                        wc_id = workcenters.get(wc_name)
                        if not wc_id:
                            if not wc_name:
                                logger.warning(f"Row {row_idx}: no workcenter specified, using fallback")
                            else:
                                logger.warning(
                                    f"Row {row_idx}: workcenter '{wc_name}' not found, using fallback"
                                )
                            wc_id = fallback_wc_id
                        
                        # Parse time cycle (optional)
                        time_cycle = None
                        if row.get('time_cycle_manual'):
                            try:
                                time_cycle = float(row['time_cycle_manual'])
                            except ValueError:
                                logger.warning(f"Row {row_idx}: invalid time_cycle")
                        
                        op_vals = {
                            'name': op_name[:64],
                            'routing_id': routing_id,
                            'workcenter_id': wc_id,
                            'sequence': sequence,
                            'blocking': row.get('blocking', 'no').strip(),
                            'company_id': self.company_id,
                        }
                        
                        if time_cycle:
                            op_vals['time_cycle_manual'] = time_cycle
                        
                        operations.append(op_vals)
                    
                    except Exception as e:
                        logger.error(f"Row {row_idx}: {e}", exc_info=True)
                        continue
        
        except csv.Error as e:
            raise RoutingError(f"Operations CSV error: {e}")
        
        return operations
    
    def _get_fallback_workcenter_id(self, workcenters: Dict[str, int]) -> int:
        """Get fallback workcenter ID."""
        for wc_name in ManufacturingConfig.FALLBACK_WORKCENTERS:
            if wc_name in workcenters:
                logger.info(f"Fallback workcenter: {wc_name}")
                return workcenters[wc_name]
        
        # Last resort: first in system
        wcs = self.client.search_read(
            'mrp.workcenter',
            [('company_id', '=', self.company_id)],
            ['id'],
            limit=1
        )
        
        if not wcs:
            raise WorkcenterNotFoundError("No workcenters found in system")
        
        return wcs[0]['id']
    
    def _batch_create_operations(
        self,
        routing_id: int,
        operations: List[Dict[str, Any]],
    ) -> None:
        """Batch create operations for routing."""
        if not operations:
            return
        
        log_header(f"Creating {len(operations)} Operations")
        
        for op_vals in operations:
            try:
                # Ensure operation (unique by name + sequence + routing)
                op_id, is_new = self.client.ensure_record(
                    'mrp.routing.workcenter',
                    [
                        ('routing_id', '=', routing_id),
                        ('sequence', '=', op_vals['sequence']),
                        ('name', '=', op_vals['name']),
                    ],
                    op_vals,
                    op_vals,
                )
                
                if is_new:
                    self.stats['operations_created'] += 1
                    log_success(
                        f"[NEW] {op_vals['name']}:seq{op_vals['sequence']} → {op_id}"
                    )
                else:
                    self.stats['operations_updated'] += 1
                
                self._audit_log({
                    'action': 'operation_created' if is_new else 'operation_updated',
                    'operation_id': op_id,
                    'operation_name': op_vals['name'],
                    'sequence': op_vals['sequence'],
                })
            
            except Exception as e:
                logger.error(f"Failed to create operation {op_vals['name']}: {e}")
                self.stats['validation_errors'] += 1
    
    # ═══════════════════════════════════════════════════════════════════════════
    # AUDIT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _audit_log(self, data: Dict[str, Any]) -> None:
        """Add audit entry."""
        data['timestamp'] = datetime.now().isoformat()
        self.audit_log.append(data)
    
    def _persist_audit_log(self) -> None:
        """Write audit log to file."""
        import json
        
        audit_path = self.base_data_dir / 'audit' / 'routing_audit.json'
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(audit_path, 'w', encoding='utf-8') as f:
                json.dump(self.audit_log, f, indent=2, default=str)
            logger.info(f"Audit log: {audit_path}")
        except Exception as e:
            logger.error(f"Failed to write audit log: {e}")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # MAIN ORCHESTRATION
    # ═══════════════════════════════════════════════════════════════════════════
    
    def run(self) -> Dict[str, int]:
        """Main entry point."""
        try:
            log_header("ROUTING LOADER")
            
            # 1) Find products
            log_info("Finding product templates...")
            product_codes = ProductTemplates.ALL_CODES
            products = self._find_products_by_codes(product_codes)
            
            if not products:
                missing = set(product_codes) - set(products.keys())
                raise RoutingError(f"Products not found: {missing}")
            
            log_success(f"Found {len(products)} products")
            
            # 2) Load workcenters
            workcenters = self._load_workcenters_from_csv()
            log_success(f"Loaded {len(workcenters)} workcenters")
            
            # 3) For each product: create routing + operations
            for product_code, product_id in products.items():
                try:
                    log_info(f"Processing product: {product_code}")
                    
                    # Find or create BoM
                    bom_id = self._find_bom_by_product_id(product_id)
                    if not bom_id:
                        logger.warning(f"No BoM found for product {product_code}")
                        continue
                    
                    # Ensure routing
                    routing_id = self._ensure_routing_for_bom(bom_id)
                    
                    # Load and create operations
                    operations = self._load_operations_from_csv(routing_id, workcenters)
                    self._batch_create_operations(routing_id, operations)
                
                except Exception as e:
                    logger.error(f"Failed to process product {product_code}: {e}")
                    self.stats['validation_errors'] += 1
            
            # 4) Persist audit
            self._persist_audit_log()
            
            # Log summary
            log_success("Routing loader completed")
            log_info("Statistics:")
            for key, value in self.stats.items():
                log_info(f"  {key}: {value}")
            
            return self.stats
        
        except Exception as e:
            log_error(f"Routing loader failed: {e}", exc_info=True)
            raise

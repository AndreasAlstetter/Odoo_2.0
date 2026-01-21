"""
bom_loader.py - Bill of Materials Loader

Lädt BoMs aus CSV mit:
- Korrektessehandling von Hierarchie
- Duplikat-Erkennung
- Sequenzen für MRP
- Batch-Verarbeitung mit Fehlerresilienz
- Audit-Trail
"""

import csv
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Tuple
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from datetime import datetime

from client import OdooClient, RecordAmbiguousError
from config import BOM_CONFIG, BATCH_SIZE
from utils import log_header, log_success, log_info, log_warn, log_error


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class BomError(Exception):
    """Base exception for BoM operations."""
    pass


class BomValidationError(BomError):
    """BoM data validation error."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATOR
# ═══════════════════════════════════════════════════════════════════════════════

class BomValidator:
    """Validate BoM data."""
    
    @staticmethod
    def validate_quantity(qty_str: str, min_qty: Decimal = Decimal('0.001')) -> Decimal:
        """
        Validate and parse quantity.
        
        Args:
            qty_str: String quantity
            min_qty: Minimum valid quantity (default: 0.001)
        
        Returns:
            Decimal quantity
        
        Raises:
            BomValidationError: Wenn ungültig
        """
        if not qty_str or not qty_str.strip():
            raise BomValidationError("Quantity is empty")
        
        try:
            qty = Decimal(qty_str.strip())
        except (ValueError, InvalidOperation) as e:
            raise BomValidationError(f"Invalid quantity format: {qty_str}")
        
        if qty < min_qty:
            raise BomValidationError(f"Quantity must be >= {min_qty}, got: {qty}")
        
        return qty
    
    @staticmethod
    def validate_sequence(seq_str: str) -> int:
        """Validate sequence number."""
        if not seq_str or not seq_str.strip():
            return 10  # Default sequence
        
        try:
            seq = int(seq_str.strip())
        except ValueError:
            raise BomValidationError(f"Invalid sequence: {seq_str}")
        
        if seq < 1:
            raise BomValidationError(f"Sequence must be >= 1, got: {seq}")
        
        return seq


# ═══════════════════════════════════════════════════════════════════════════════
# CSV READER
# ═══════════════════════════════════════════════════════════════════════════════

class BomCsvReader:
    """Read and validate BoM CSV."""
    
    # Expected CSV columns
    REQUIRED_COLUMNS = {
        'bom_id',           # e.g., 'BOM-001'
        'bom_name',         # e.g., 'Drohne Standard'
        'product_code',     # e.g., '029.3.000'
        'product_qty',      # e.g., '1'
        'component_code',   # e.g., '009.1.000'
        'component_qty',    # e.g., '2'
        'sequence',         # e.g., '10' (optional)
    }
    
    @staticmethod
    def read_and_validate(csv_path: Path) -> List[Dict[str, str]]:
        """
        Read CSV with schema validation.
        
        Args:
            csv_path: Path to BoM CSV
        
        Returns:
            List of validated rows
        
        Raises:
            BomError: Wenn CSV ungültig oder fehlt
        """
        if not csv_path.exists():
            raise BomError(f"BoM CSV not found: {csv_path}")
        
        rows = []
        
        for encoding in ['utf-8-sig', 'utf-8', 'latin-1']:
            try:
                with open(csv_path, 'r', encoding=encoding, newline='') as f:
                    reader = csv.DictReader(f, delimiter=';')
                    
                    # Validate header
                    if not reader.fieldnames:
                        raise BomError("CSV is empty (no header)")
                    
                    missing_cols = BomCsvReader.REQUIRED_COLUMNS - set(reader.fieldnames)
                    if missing_cols:
                        raise BomError(f"CSV missing columns: {missing_cols}")
                    
                    rows = list(reader)
                
                logger.info(f"Read {len(rows)} BoM rows (encoding: {encoding})")
                return rows
            
            except UnicodeDecodeError:
                continue
            except csv.Error as e:
                raise BomError(f"CSV parse error: {e}")
        
        raise BomError(f"Failed to read {csv_path}")


# ═══════════════════════════════════════════════════════════════════════════════
# BoM LOADER
# ═══════════════════════════════════════════════════════════════════════════════

class BomLoader:
    """Load Bill of Materials from CSV."""
    
    def __init__(self, client: OdooClient, base_data_dir: str):
        self.client = client
        self.base_data_dir = Path(base_data_dir)
        self.bom_dir = self.base_data_dir / BOM_CONFIG.get('bom_dir', 'boms')
        
        # UoM (should be 'Units')
        self.uom_id = self._get_or_create_uom()
        
        # Cache: code → id
        self.product_tmpl_cache: Dict[str, int] = {}
        self.product_variant_cache: Dict[str, int] = {}
        self.bom_cache: Dict[str, int] = {}
        
        # Statistics
        self.stats = {
            'bom_created': 0,
            'bom_updated': 0,
            'bom_skipped': 0,
            'bom_line_created': 0,
            'bom_line_updated': 0,
            'bom_line_skipped': 0,
            'errors_product_not_found': 0,
            'errors_component_not_found': 0,
            'errors_invalid_quantity': 0,
            'errors': 0,
        }
        
        # Audit log
        self.audit_log: List[Dict[str, Any]] = []
        
        logger.info(f"BomLoader initialized: {self.bom_dir}")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # SETUP
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _get_or_create_uom(self) -> int:
        """Get or create UoM."""
        uom_name = BOM_CONFIG.get('uom_name', 'Units')
        
        uoms = self.client.search_read(
            'uom.uom',
            [('name', '=', uom_name)],
            ['id'],
            limit=1
        )
        
        if uoms:
            return uoms[0]['id']
        
        # Create default UoM
        logger.warning(f"UoM '{uom_name}' not found, creating...")
        
        uom_id = self.client.create(
            'uom.uom',
            {
                'name': uom_name,
                'category_id': 1,  # Unit
                'rounding': 0.01,
            }
        )
        
        return uom_id
    
    def _prefetch_products(self, rows: List[Dict[str, str]]) -> None:
        """Pre-fetch all products in batch."""
        # Collect all codes
        tmpl_codes: Set[str] = set()
        variant_codes: Set[str] = set()
        
        for row in rows:
            product_code = row.get('product_code', '').strip()
            component_code = row.get('component_code', '').strip()
            
            if product_code:
                tmpl_codes.add(product_code)
            if component_code:
                variant_codes.add(component_code)
        
        # Fetch templates
        if tmpl_codes:
            products = self.client.search_read(
                'product.template',
                [('default_code', 'in', list(tmpl_codes))],
                ['default_code', 'id'],
            )
            
            for product in products:
                self.product_tmpl_cache[product['default_code']] = product['id']
        
        # Fetch variants
        if variant_codes:
            products = self.client.search_read(
                'product.product',
                [('default_code', 'in', list(variant_codes))],
                ['default_code', 'id'],
            )
            
            for product in products:
                self.product_variant_cache[product['default_code']] = product['id']
        
        logger.info(
            f"Prefetched: {len(self.product_tmpl_cache)} templates, "
            f"{len(self.product_variant_cache)} variants"
        )
    
    # ═══════════════════════════════════════════════════════════════════════════
    # LOADING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _load_boms(self, rows: List[Dict[str, str]]) -> None:
        """Load BoMs from CSV rows."""
        if not rows:
            log_warn("No BoM rows to load")
            return
        
        log_header("Loading Bills of Materials")
        
        # Prefetch products
        self._prefetch_products(rows)
        
        # Group by BoM ID
        bom_groups: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        
        for row in rows:
            bom_id = row.get('bom_id', '').strip()
            if bom_id:
                bom_groups[bom_id].append(row)
        
        logger.info(f"Processing {len(bom_groups)} BoMs")
        
        # Process each BoM
        for bom_id, group_rows in bom_groups.items():
            try:
                self._process_bom(bom_id, group_rows)
            except Exception as e:
                logger.error(f"Failed to process BoM {bom_id}: {e}", exc_info=True)
                self.stats['errors'] += 1
        
        log_success(
            f"BoM loading completed: "
            f"{self.stats['bom_created']} created, "
            f"{self.stats['bom_updated']} updated, "
            f"{self.stats['bom_skipped']} skipped"
        )
    
    def _process_bom(self, bom_id: str, rows: List[Dict[str, str]]) -> None:
        """Process single BoM (header + lines)."""
        if not rows:
            return
        
        # Get header from first row
        header_row = rows[0]
        product_code = header_row.get('product_code', '').strip()
        bom_name = header_row.get('bom_name', '').strip() or f"BoM {bom_id}"
        
        if not product_code:
            logger.warning(f"BoM {bom_id}: missing product_code")
            self.stats['bom_skipped'] += 1
            return
        
        # Find product template
        product_tmpl_id = self.product_tmpl_cache.get(product_code)
        if not product_tmpl_id:
            logger.warning(f"BoM {bom_id}: product '{product_code}' not found")
            self.stats['errors_product_not_found'] += 1
            self.stats['bom_skipped'] += 1
            return
        
        try:
            # Validate product qty
            product_qty_str = header_row.get('product_qty', '').strip()
            product_qty = BomValidator.validate_quantity(product_qty_str or '1')
        except BomValidationError as e:
            logger.warning(f"BoM {bom_id}: {e}")
            self.stats['errors_invalid_quantity'] += 1
            self.stats['bom_skipped'] += 1
            return
        
        # Get or create BoM header
        bom_header_id, is_new = self._ensure_bom_header(
            bom_id,
            bom_name,
            product_tmpl_id,
            product_qty,
        )
        
        if is_new:
            self.stats['bom_created'] += 1
            log_success(f"[BOM:NEW] {bom_id} ({bom_name}) → {bom_header_id}")
        else:
            self.stats['bom_updated'] += 1
            log_info(f"[BOM:UPD] {bom_id} ({bom_name}) → {bom_header_id}")
        
        self.bom_cache[bom_id] = bom_header_id
        
        # Process lines
        self._process_bom_lines(bom_header_id, bom_id, rows)
    
    def _ensure_bom_header(
        self,
        bom_id: str,
        bom_name: str,
        product_tmpl_id: int,
        product_qty: Decimal,
    ) -> Tuple[int, bool]:
        """Get or create BoM header."""
        domain = [('product_tmpl_id', '=', product_tmpl_id)]
        
        boms = self.client.search_read(
            'mrp.bom',
            domain,
            ['id'],
            limit=1
        )
        
        if boms:
            # Update existing
            bom_header_id = boms[0]['id']
            
            vals = {
                'product_qty': float(product_qty),
                'product_uom_id': self.uom_id,
            }
            
            self.client.write('mrp.bom', [bom_header_id], vals)
            
            return bom_header_id, False
        
        # Create new
        vals = {
            'product_tmpl_id': product_tmpl_id,
            'product_qty': float(product_qty),
            'product_uom_id': self.uom_id,
            'type': BOM_CONFIG.get('bom_type', 'normal'),
        }
        
        bom_header_id = self.client.create('mrp.bom', vals)
        
        return bom_header_id, True
    
    def _process_bom_lines(
        self,
        bom_header_id: int,
        bom_id: str,
        rows: List[Dict[str, str]],
    ) -> None:
        """Process BoM lines (components)."""
        # Deduplicate by component code
        seen_components: Set[str] = set()
        line_vals_list: List[Dict[str, Any]] = []
        
        for row_idx, row in enumerate(rows, start=1):
            component_code = row.get('component_code', '').strip()
            
            if not component_code:
                logger.debug(f"BoM {bom_id}: missing component_code")
                self.stats['bom_line_skipped'] += 1
                continue
            
            # Deduplicate
            if component_code in seen_components:
                logger.warning(
                    f"BoM {bom_id}: duplicate component '{component_code}'"
                )
                self.stats['bom_line_skipped'] += 1
                continue
            
            seen_components.add(component_code)
            
            # Find component
            component_id = self.product_variant_cache.get(component_code)
            if not component_id:
                logger.warning(
                    f"BoM {bom_id}: component '{component_code}' not found"
                )
                self.stats['errors_component_not_found'] += 1
                self.stats['bom_line_skipped'] += 1
                continue
            
            # Validate quantity
            try:
                component_qty_str = row.get('component_qty', '').strip()
                component_qty = BomValidator.validate_quantity(component_qty_str or '1')
            except BomValidationError as e:
                logger.warning(f"BoM {bom_id}, component {component_code}: {e}")
                self.stats['errors_invalid_quantity'] += 1
                self.stats['bom_line_skipped'] += 1
                continue
            
            # Validate sequence
            try:
                sequence_str = row.get('sequence', '').strip()
                sequence = BomValidator.validate_sequence(sequence_str)
            except BomValidationError as e:
                logger.warning(f"BoM {bom_id}, component {component_code}: {e}")
                sequence = 10
            
            # Build line vals
            line_vals = {
                'bom_id': bom_header_id,
                'product_id': component_id,
                'product_qty': float(component_qty),
                'product_uom_id': self.uom_id,
                'sequence': sequence,
            }
            
            line_vals_list.append(line_vals)
        
        # Batch create lines
        if line_vals_list:
            self._batch_create_lines(bom_header_id, bom_id, line_vals_list)
    
    def _batch_create_lines(
        self,
        bom_header_id: int,
        bom_id: str,
        line_vals_list: List[Dict[str, Any]],
    ) -> None:
        """Batch create BoM lines."""
        try:
            # Delete existing lines (clean slate)
            self.client.search_write(
                'mrp.bom.line',
                [('bom_id', '=', bom_header_id)],
                {},  # Empty write just to get count
            )
            
            existing = self.client.search(
                'mrp.bom.line',
                [('bom_id', '=', bom_header_id)],
            )
            
            if existing:
                self.client.unlink('mrp.bom.line', existing)
                logger.debug(f"Deleted {len(existing)} old lines for BoM {bom_id}")
            
            # Create new lines in batches
            for batch_start in range(0, len(line_vals_list), BATCH_SIZE):
                batch = line_vals_list[batch_start:batch_start + BATCH_SIZE]
                
                try:
                    # Batch create
                    created_ids = self.client.create_batch('mrp.bom.line', batch)
                    
                    self.stats['bom_line_created'] += len(created_ids)
                    
                    logger.debug(
                        f"Created {len(created_ids)} lines for BoM {bom_id}"
                    )
                
                except Exception as e:
                    logger.error(f"Failed to create batch for BoM {bom_id}: {e}")
                    self.stats['errors'] += 1
        
        except Exception as e:
            logger.error(f"Failed to process lines for BoM {bom_id}: {e}")
            self.stats['errors'] += 1
    
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
        
        audit_path = self.base_data_dir / 'audit' / 'bom_audit.json'
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(audit_path, 'w') as f:
                json.dump(self.audit_log, f, indent=2, default=str)
            logger.info(f"Audit log: {audit_path}")
        except Exception as e:
            logger.error(f"Failed to write audit log: {e}")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # MAIN
    # ═══════════════════════════════════════════════════════════════════════════
    
    def run(self, filename: str = 'boms.csv') -> Dict[str, int]:
        """Main entry point."""
        try:
            log_header("BOM LOADER")
            
            # Read CSV
            csv_path = self.bom_dir / filename
            rows = BomCsvReader.read_and_validate(csv_path)
            
            if not rows:
                log_warn("No BoM rows found")
                return {'skipped': True}
            
            # Load
            self._load_boms(rows)
            
            # Persist audit
            self._persist_audit_log()
            
            # Summary
            log_success("BoM loader completed")
            log_info("Statistics:")
            for key, value in self.stats.items():
                log_info(f"  {key}: {value}")
            
            return self.stats
        
        except Exception as e:
            log_error(f"BoM loader failed: {e}", exc_info=True)
            raise

"""
manufacturing_config_loader.py - Manufacturing System Configuration

Setup für Fertigungssteuerung:
1. Sequences für Manufacturing Orders (MO)
2. Picking Types für MRP Operations
3. Tracking-Einstellungen für Seriennummern/Chargen

Optimiert für >500 Drohnen/Tag mit Batch-Sequenzen (no_gap=false für Performance)
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

from provisioning.client import OdooClient, ValidationError
from provisioning.config import ManufacturingConfig, get_odoo_config
from provisioning.utils import log_header, log_success, log_info, log_warn, log_error


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class ManufacturingConfigError(Exception):
    """Base exception for manufacturing config."""
    pass


class SequenceError(ManufacturingConfigError):
    """Sequence creation/update error."""
    pass


class PickingTypeError(ManufacturingConfigError):
    """Picking type configuration error."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# SEQUENCE MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class SequenceManager:
    """Manage ir.sequence for Manufacturing Orders."""
    
    def __init__(self, client: OdooClient, company_id: int):
        self.client = client
        self.company_id = company_id
    
    def ensure_mo_sequence(
        self,
        prefix: str = ManufacturingConfig.MO_SEQUENCE_PREFIX,
        padding: int = ManufacturingConfig.MO_SEQUENCE_PADDING,
        no_gap: bool = False,  # Performance: False für High-Volume!
    ) -> int:
        """
        Get or create Manufacturing Order Sequence.
        
        Args:
            prefix: Sequence prefix (default: 'MO')
            padding: Number padding (default: 7 → MO0000001 bis MO9999999)
            no_gap: Enforce no gaps in sequence (False recommended für >500/day)
        
        Returns:
            Sequence ID
        
        Raises:
            SequenceError: Wenn Sequence creation fehlschlägt
        """
        seq_code = ManufacturingConfig.MRP_SEQUENCE_CODE
        seq_name = f"Manufacturing Order ({prefix})"
        
        try:
            # Search (global + company-specific)
            sequences = self.client.search_read(
                'ir.sequence',
                [
                    ('code', '=', seq_code),
                    '|',
                    ('company_id', '=', self.company_id),
                    ('company_id', '=', False),
                ],
                ['id', 'name', 'prefix', 'padding', 'company_id'],
                limit=1
            )
            
            if sequences:
                seq = sequences[0]
                logger.info(
                    f"Sequence exists: '{seq['name']}' "
                    f"(ID: {seq['id']}, company_id: {seq.get('company_id')})"
                )
                
                # Update if needed
                if seq['prefix'] != prefix or seq['padding'] != padding:
                    logger.info(f"Updating sequence: {prefix}{padding}")
                    self.client.write(
                        'ir.sequence',
                        [seq['id']],
                        {'prefix': prefix, 'padding': padding}
                    )
                
                return seq['id']
            
            # Create new (company-specific)
            vals = {
                'name': seq_name,
                'code': seq_code,
                'prefix': prefix,
                'padding': padding,
                'suffix': '',
                'number_next': 1,
                'company_id': self.company_id,
                'implementation': 'no_gap' if no_gap else 'standard',
                'number_increment': 1,
            }
            
            seq_id = self.client.create('ir.sequence', vals)
            
            logger.info(
                f"Created sequence: '{seq_name}' "
                f"(ID: {seq_id}, prefix: {prefix}, padding: {padding})"
            )
            
            return seq_id
        
        except Exception as e:
            raise SequenceError(f"Failed to ensure MO sequence: {e}") from e
    
    def list_all_sequences(self) -> List[Dict[str, Any]]:
        """List all manufacturing sequences."""
        return self.client.search_read(
            'ir.sequence',
            [('code', '=', ManufacturingConfig.MRP_SEQUENCE_CODE)],
            ['id', 'name', 'prefix', 'padding', 'company_id'],
        )


# ═══════════════════════════════════════════════════════════════════════════════
# PICKING TYPE MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class PickingTypeManager:
    """Manage stock.picking.type for MRP operations."""
    
    def __init__(self, client: OdooClient, company_id: int):
        self.client = client
        self.company_id = company_id
    
    def ensure_mrp_picking_types(
        self,
        sequence_id: int,
    ) -> Tuple[int, int]:
        """
        Ensure MRP picking types exist with sequences.
        
        MRP Operations require two picking types:
        1. MRP consumption (raw materials)
        2. MRP production (finished goods)
        
        Args:
            sequence_id: Sequence ID for picking numbers
        
        Returns:
            (consumption_type_id, production_type_id)
        
        Raises:
            PickingTypeError: Wenn Setup fehlschlägt
        """
        try:
           # Odoo 19 Standard MRP Picking Code
            picking_type_code = "mrp_operation"

            
            # Get or create warehouse (required for picking types)
            warehouse_id = self._ensure_warehouse()
            
            # MRP Consumption Picking Type
            consumption = self._ensure_picking_type(
                name="Manufacturing Consumption",
                code=picking_type_code,
                warehouse_id=warehouse_id,
                picking_type_code='incoming',  # Raw materials in
                sequence_id=sequence_id,
            )
            
            # MRP Production Picking Type
            production = self._ensure_picking_type(
                name="Manufacturing Production",
                code=picking_type_code,
                warehouse_id=warehouse_id,
                picking_type_code='internal',  # Finished goods internal move
                sequence_id=sequence_id,
            )
            
            logger.info(
                f"Picking types: consumption={consumption}, production={production}"
            )
            
            return consumption, production
        
        except Exception as e:
            raise PickingTypeError(f"Failed to ensure picking types: {e}") from e
    
    def _ensure_warehouse(self) -> int:
        """Get or create warehouse for company."""
        warehouses = self.client.search_read(
            'stock.warehouse',
            [('company_id', '=', self.company_id)],
            ['id'],
            limit=1
        )
        
        if warehouses:
            return warehouses[0]['id']
        
        # Create if missing (should not happen in normal setup)
        logger.warning("No warehouse found, creating default...")
        
        warehouse_id = self.client.create(
            'stock.warehouse',
            {
                'name': f'Warehouse Company {self.company_id}',
                'code': f'WH{self.company_id}',
                'company_id': self.company_id,
            }
        )
        
        return warehouse_id
    
    def _ensure_picking_type(
        self,
        name: str,
        code: str,
        warehouse_id: int,
        picking_type_code: str,
        sequence_id: int,
    ) -> int:
        """Ensure picking type exists."""
        picking_types = self.client.search_read(
            'stock.picking.type',
            [
                ('name', '=', name),
                ('warehouse_id', '=', warehouse_id),
                ('code', '=', picking_type_code),
            ],
            ['id', 'sequence_id'],
            limit=1
        )
        
        if picking_types:
            pt = picking_types[0]
            
            # Update sequence if not set
            if not pt.get('sequence_id'):
                self.client.write(
                    'stock.picking.type',
                    [pt['id']],
                    {'sequence_id': sequence_id}
                )
                logger.info(f"Updated {name} with sequence {sequence_id}")
            
            return pt['id']
       
        sequence_code = f"{code.upper()}-{warehouse_id}-{picking_type_code[:3].upper()}"

        pt_id = self.client.create(
            'stock.picking.type',
            {
                'name': name,
                'code': picking_type_code,
                'warehouse_id': warehouse_id,
                'sequence_id': sequence_id,
                'sequence_code': sequence_code,  # Unique identifier
                'company_id': self.company_id,
            }
        )
        
        logger.info(f"Created picking type: {name} → {pt_id}")
        
        return pt_id


# ═══════════════════════════════════════════════════════════════════════════════
# PRODUCT TRACKING MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class ProductTrackingManager:
    """
    Manage product tracking (serial numbers, lots).
    
    Für Drohnen-Produktion kritisch:
    - Drohnen selbst: Serial tracking (eindeutige SN)
    - Elektronik-Komponenten: Lot tracking (Charge)
    """
    
    def __init__(self, client: OdooClient):
        self.client = client
    
    def setup_tracking_for_categories(
        self,
        category_tracking_map: Dict[str, str],
    ) -> int:
        """
        Setup tracking (serial/lot) for product categories.
        
        Args:
            category_tracking_map: {category_name: 'serial'|'lot'|'none'}
            
        Example:
            {
                'Drohnen': 'serial',           # Jede Drohne hat SN
                'Elektronik': 'lot',           # Elektronik nach Charge
                'Verpackung': 'none',          # Keine Verfolgung
            }
        
        Returns:
            Anzahl aktualisierter Kategorien
        """
        updated = 0
        
        for category_name, tracking_type in category_tracking_map.items():
            try:
                # Find category
                categories = self.client.search_read(
                    'product.category',
                    [('name', '=', category_name)],
                    ['id'],
                    limit=1
                )
                
                if not categories:
                    logger.warning(f"Category not found: {category_name}")
                    continue
                
                cat_id = categories[0]['id']
                
                # Update all products in category
                products = self.client.search_read(
                    'product.template',
                    [('categ_id', '=', cat_id)],
                    ['id'],
                )
                
                if products:
                    product_ids = [p['id'] for p in products]
                    
                    self.client.write(
                        'product.template',
                        product_ids,
                        {'tracking': tracking_type}
                    )
                    
                    logger.info(
                        f"Updated {len(products)} products in '{category_name}' "
                        f"to tracking='{tracking_type}'"
                    )
                    
                    updated += len(products)
            
            except Exception as e:
                logger.error(f"Failed to setup tracking for {category_name}: {e}")
        
        return updated


# ═══════════════════════════════════════════════════════════════════════════════
# MANUFACTURING CONFIG LOADER
# ═══════════════════════════════════════════════════════════════════════════════

class ManufacturingConfigLoader:
    """
    Complete manufacturing system configuration.
    
    Setzt up:
    1. MO Sequences (für MRP.production numbering)
    2. Picking Types (für Stock operations)
    3. Product Tracking (für Seriennummern/Chargen)
    """
    
    def __init__(self, client: OdooClient, base_data_dir: str):
        self.client = client
        self.base_data_dir = Path(base_data_dir)
        
        # Company
        companies = client.search_read('res.company', [], ['id'], limit=1)
        if not companies:
            raise ManufacturingConfigError("No company found in Odoo")
        self.company_id = companies[0]['id']
        
        # Managers
        self.sequence_mgr = SequenceManager(client, self.company_id)
        self.picking_mgr = PickingTypeManager(client, self.company_id)
        self.tracking_mgr = ProductTrackingManager(client)
        
        # Statistics
        self.stats = {
            'sequences_created': 0,
            'sequences_updated': 0,
            'picking_types_created': 0,
            'picking_types_updated': 0,
            'products_tracking_updated': 0,
            'errors': 0,
        }
        
        logger.info(f"ManufacturingConfigLoader initialized (company_id={self.company_id})")
    
    def run(self) -> Dict[str, int]:
        """Main orchestration."""
        try:
            log_header("MANUFACTURING CONFIG LOADER")
            
            # 1) Setup Sequences
            log_info("Setting up Manufacturing Order Sequences...")
            mo_seq_id = self._setup_mo_sequences()
            
            # 2) Setup Picking Types
            log_info("Setting up MRP Picking Types...")
            consumption_pt, production_pt = self._setup_picking_types(mo_seq_id)
            
            # 3) Setup Product Tracking
            log_info("Setting up Product Tracking...")
            self._setup_product_tracking()
            
            # Summary
            log_success("Manufacturing configuration complete")
            log_info("Statistics:")
            for key, value in self.stats.items():
                log_info(f"  {key}: {value}")
            
            return self.stats
        
        except ManufacturingConfigError as e:
            log_error(f"Manufacturing config failed: {e}")
            raise
        except Exception as e:
            log_error(f"Unexpected error: {e}", exc_info=True)
            raise
    
    def _setup_mo_sequences(self) -> int:
        """Setup Manufacturing Order sequences."""
        try:
            log_header("Manufacturing Order Sequences")
            
            # For >500 units/day: use standard (no no_gap) for performance
            seq_id = self.sequence_mgr.ensure_mo_sequence(
                prefix=ManufacturingConfig.MO_SEQUENCE_PREFIX,
                padding=ManufacturingConfig.MO_SEQUENCE_PADDING,
                no_gap=False,  # Performance: no locks for high volume
            )
            
            # Validate
            sequences = self.sequence_mgr.list_all_sequences()
            log_success(f"MO Sequences: {len(sequences)} found/created")
            
            return seq_id
        
        except Exception as e:
            self.stats['errors'] += 1
            log_error(f"MO sequence setup failed: {e}")
            raise
    
    def _setup_picking_types(self, sequence_id: int) -> Tuple[int, int]:
        """Setup MRP picking types."""
        try:
            log_header("MRP Picking Types")
            
            consumption_pt, production_pt = self.picking_mgr.ensure_mrp_picking_types(
                sequence_id=sequence_id
            )
            
            log_success(
                f"Picking types configured: "
                f"consumption={consumption_pt}, production={production_pt}"
            )
            
            return consumption_pt, production_pt
        
        except Exception as e:
            self.stats['errors'] += 1
            log_error(f"Picking type setup failed: {e}")
            raise
    
    def _setup_product_tracking(self) -> None:
        """Setup product tracking (serial/lot)."""
        try:
            log_header("Product Tracking (Serial/Lot)")
            
            # Mapping: category → tracking type
            tracking_map = {
                'Drohnen': 'serial',           # Jede Drohne eindeutig
                'Elektronik': 'lot',           # Elektronik nach Charge
                'Kernkomponenten': 'lot',      # Kernkomponenten nach Charge
                'Verpackung': 'none',          # Keine Verfolgung nötig
            }
            
            updated = self.tracking_mgr.setup_tracking_for_categories(tracking_map)
            
            self.stats['products_tracking_updated'] = updated
            log_success(f"Product tracking updated: {updated} products")
        
        except Exception as e:
            logger.warning(f"Product tracking setup failed: {e}")
            # Non-critical, nicht abbrechen

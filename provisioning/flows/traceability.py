"""
traceability_manager.py - Product Traceability & Lot Management

Handles:
- Serial number (lot) creation and management
- Batch tracking and association
- Component usage tracking in manufacturing
- Full traceability chain (BOM → MO → Delivery)
- Audit trail and compliance
- Export and reporting
- Proper error handling and statistics

Production-ready with validation, error handling, and comprehensive tracking.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from client import OdooClient
from validation import validate_int, ValidationError
from utils import (
    log_header, log_info, log_success, log_warn, log_error,
)


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS & CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

# Traceability field names (configurable)
TRACEABILITY_FIELDS = {
    'batch_id': 'x_batch_id',
    'used_in_mo': 'x_used_in_mo',
    'created_by': 'x_created_by_user',
    'qc_status': 'x_qc_status',
}


# ═══════════════════════════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class SerialNumber:
    """Serial number/Lot record."""
    id: int
    name: str
    product_id: int
    product_name: str
    batch_id: Optional[str] = None
    used_in_mos: List[int] = None
    
    created_at: Optional[datetime] = None
    qc_status: str = "pending"  # pending, passed, failed
    
    def __post_init__(self):
        if self.used_in_mos is None:
            self.used_in_mos = []


@dataclass
class ComponentUsage:
    """Component usage in manufacturing."""
    id: int
    mo_id: int
    component_id: int
    component_name: str
    serial_number: str
    quantity_used: float
    bom_line_id: Optional[int] = None
    
    created_at: Optional[datetime] = None


@dataclass
class TraceabilityChain:
    """Complete traceability chain for product."""
    product_id: int
    product_name: str
    
    serial_numbers: List[SerialNumber] = None
    manufacturing_orders: List[int] = None
    deliveries: List[int] = None
    
    components: List[ComponentUsage] = None
    
    def __post_init__(self):
        if self.serial_numbers is None:
            self.serial_numbers = []
        if self.manufacturing_orders is None:
            self.manufacturing_orders = []
        if self.deliveries is None:
            self.deliveries = []
        if self.components is None:
            self.components = []


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class TraceabilityError(Exception):
    """Base traceability error."""
    pass


class SerialNumberError(TraceabilityError):
    """Serial number error."""
    pass


class BatchError(TraceabilityError):
    """Batch tracking error."""
    pass


class ComponentError(TraceabilityError):
    """Component usage error."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# TRACEABILITY MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class TraceabilityManager:
    """Manage product traceability and serial numbers."""
    
    def __init__(self, client: OdooClient):
        """Initialize traceability manager."""
        self.client = client
        
        self.stats = {
            'serials_created': 0,
            'serials_found': 0,
            'batches_linked': 0,
            'components_tracked': 0,
            'chains_retrieved': 0,
            'errors': 0,
        }
        
        self.audit_log: List[Dict[str, Any]] = []
        
        logger.info("TraceabilityManager initialized")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # FIELD VALIDATION
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _validate_field_exists(
        self,
        model: str,
        field_name: str,
    ) -> bool:
        """
        Check if custom field exists on model.
        
        Args:
            model: Model name
            field_name: Field name
        
        Returns:
            True if field exists
        """
        try:
            fields = self.client.execute(
                'ir.model.fields',
                'search',
                [
                    ('model', '=', model),
                    ('name', '=', field_name),
                ],
            )
            
            return len(fields) > 0
        
        except Exception as e:
            logger.warning(f"Failed to check field {model}.{field_name}: {e}")
            return False
    
    # ═══════════════════════════════════════════════════════════════════════════
    # SERIAL NUMBER MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def assign_serial_number(
        self,
        product_id: int,
        serial: str,
    ) -> Optional[int]:
        """
        Create or find serial number (lot).
        
        Args:
            product_id: Product ID
            serial: Serial number string
        
        Returns:
            Lot ID or None
        
        Raises:
            SerialNumberError: If validation fails
        """
        try:
            # Validate
            product_id = validate_int(
                product_id,
                min_value=1,
                field_name="product_id",
            )
        except ValidationError as e:
            raise SerialNumberError(f"Invalid product ID: {e}") from e
        
        serial = (serial or "").strip()
        
        if not serial:
            raise SerialNumberError("Serial number cannot be empty")
        
        if len(serial) > 255:
            raise SerialNumberError(f"Serial number too long: {len(serial)} > 255")
        
        try:
            # Check if exists
            lots = self.client.search_read(
                'stock.lot',
                [
                    ('name', '=', serial),
                    ('product_id', '=', product_id),
                ],
                ['id'],
                limit=1,
            )
            
            if lots:
                lot_id = lots[0]['id']
                self.stats['serials_found'] += 1
                
                logger.debug(f"Serial {serial} already exists: {lot_id}")
                
                return lot_id
            
            # Create new
            lot_id = self.client.create(
                'stock.lot',
                {
                    'name': serial,
                    'product_id': product_id,
                }
            )
            
            if isinstance(lot_id, (list, tuple)):
                if not lot_id:
                    raise SerialNumberError("create() returned empty result")
                lot_id = lot_id[0]
            
            lot_id = int(lot_id)
            self.stats['serials_created'] += 1
            
            logger.info(f"Created serial number {serial} (Lot {lot_id})")
            
            self._audit_log({
                'operation': 'serial_created',
                'lot_id': lot_id,
                'serial': serial,
                'product_id': product_id,
            })
            
            return lot_id
        
        except Exception as e:
            logger.error(f"Failed to assign serial {serial}: {e}")
            self.stats['errors'] += 1
            raise SerialNumberError(f"Serial number assignment failed: {e}") from e
    
    # ═══════════════════════════════════════════════════════════════════════════
    # BATCH TRACKING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def link_to_batch(
        self,
        serial_number: str,
        batch_id: str,
    ) -> bool:
        """
        Link serial number to batch.
        
        Args:
            serial_number: Serial number
            batch_id: Batch ID
        
        Returns:
            True if successful
        """
        serial_number = (serial_number or "").strip()
        batch_id = (batch_id or "").strip()
        
        if not serial_number or not batch_id:
            raise BatchError("Serial number and batch ID required")
        
        try:
            # Find lot
            lots = self.client.search_read(
                'stock.lot',
                [('name', '=', serial_number)],
                ['id'],
                limit=1,
            )
            
            if not lots:
                raise BatchError(f"Serial number not found: {serial_number}")
            
            lot_id = lots[0]['id']
            
            # Check field exists
            field_name = TRACEABILITY_FIELDS['batch_id']
            if not self._validate_field_exists('stock.lot', field_name):
                logger.warning(
                    f"Field {field_name} not found on stock.lot, "
                    f"cannot link to batch"
                )
                return False
            
            # Link
            self.client.write(
                'stock.lot',
                [lot_id],
                {field_name: batch_id},
            )
            
            self.stats['batches_linked'] += 1
            
            logger.info(f"Linked serial {serial_number} to batch {batch_id}")
            
            self._audit_log({
                'operation': 'batch_linked',
                'lot_id': lot_id,
                'serial': serial_number,
                'batch_id': batch_id,
            })
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to link batch: {e}")
            self.stats['errors'] += 1
            return False
    
    # ═══════════════════════════════════════════════════════════════════════════
    # COMPONENT USAGE TRACKING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def track_component_usage(
        self,
        mo_id: int,
        component_id: int,
        serial_number: str,
        quantity_used: float = 1.0,
        bom_line_id: Optional[int] = None,
    ) -> bool:
        """
        Track component usage in manufacturing.
        
        Args:
            mo_id: Manufacturing order ID
            component_id: Component product ID
            serial_number: Component serial number
            quantity_used: Quantity used
            bom_line_id: BOM line ID (optional)
        
        Returns:
            True if successful
        """
        try:
            # Validate
            mo_id = validate_int(mo_id, min_value=1, field_name="mo_id")
            component_id = validate_int(
                component_id, min_value=1, field_name="component_id"
            )
            
            serial_number = (serial_number or "").strip()
            if not serial_number:
                raise ComponentError("Serial number required")
            
            quantity_used = float(quantity_used)
            if quantity_used <= 0:
                raise ComponentError(f"Quantity must be > 0: {quantity_used}")
            
            # Find lot
            lots = self.client.search_read(
                'stock.lot',
                [
                    ('name', '=', serial_number),
                    ('product_id', '=', component_id),
                ],
                ['id'],
                limit=1,
            )
            
            if not lots:
                raise ComponentError(
                    f"Serial {serial_number} not found for component {component_id}"
                )
            
            lot_id = lots[0]['id']
            
            # Check field exists
            field_name = TRACEABILITY_FIELDS['used_in_mo']
            if not self._validate_field_exists('stock.lot', field_name):
                logger.warning(
                    f"Field {field_name} not found on stock.lot, "
                    f"cannot track component usage"
                )
                return False
            
            # Track usage (store MO reference)
            # Note: Better approach would be Many2many relation
            self.client.write(
                'stock.lot',
                [lot_id],
                {field_name: f"{field_name}:{mo_id}"},
            )
            
            self.stats['components_tracked'] += 1
            
            logger.info(
                f"Tracked component usage: serial={serial_number}, "
                f"mo={mo_id}, qty={quantity_used}"
            )
            
            self._audit_log({
                'operation': 'component_tracked',
                'lot_id': lot_id,
                'mo_id': mo_id,
                'component_id': component_id,
                'quantity': quantity_used,
                'bom_line_id': bom_line_id,
            })
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to track component: {e}")
            self.stats['errors'] += 1
            return False
    
    # ═══════════════════════════════════════════════════════════════════════════
    # TRACEABILITY CHAIN RETRIEVAL
    # ═══════════════════════════════════════════════════════════════════════════
    
    def get_traceability_chain(
        self,
        product_id: int,
    ) -> TraceabilityChain:
        """
        Get complete traceability chain for product.
        
        Args:
            product_id: Product ID
        
        Returns:
            TraceabilityChain record
        """
        try:
            product_id = validate_int(
                product_id, min_value=1, field_name="product_id"
            )
        except ValidationError as e:
            raise TraceabilityError(f"Invalid product ID: {e}") from e
        
        try:
            # Get product info
            products = self.client.search_read(
                'product.product',
                [('id', '=', product_id)],
                ['name'],
                limit=1,
            )
            
            if not products:
                raise TraceabilityError(f"Product not found: {product_id}")
            
            product_name = products[0].get('name', '')
            
            # Create chain
            chain = TraceabilityChain(
                product_id=product_id,
                product_name=product_name,
            )
            
            # Get serial numbers
            lots = self.client.search_read(
                'stock.lot',
                [('product_id', '=', product_id)],
                ['id', 'name', 'create_date'],
                limit=100,
            )
            
            for lot in lots:
                chain.serial_numbers.append(
                    SerialNumber(
                        id=lot['id'],
                        name=lot['name'],
                        product_id=product_id,
                        product_name=product_name,
                        created_at=self._parse_datetime(lot.get('create_date')),
                    )
                )
            
            # Get manufacturing orders
            mos = self.client.search_read(
                'mrp.production',
                [('product_id', '=', product_id)],
                ['id'],
                limit=50,
            )
            
            chain.manufacturing_orders = [m['id'] for m in mos]
            
            # Get deliveries (via pickings)
            for mo in mos:
                mo_data = self.client.search_read(
                    'mrp.production',
                    [('id', '=', mo['id'])],
                    ['name'],
                    limit=1,
                )
                
                if mo_data:
                    mo_name = mo_data[0].get('name', '')
                    pickings = self.client.search_read(
                        'stock.picking',
                        [
                            ('sale_id.origin', 'ilike', mo_name),
                            ('picking_type_code', '=', 'outgoing'),
                        ],
                        ['id'],
                        limit=20,
                    )
                    
                    chain.deliveries.extend([p['id'] for p in pickings])
            
            self.stats['chains_retrieved'] += 1
            
            logger.info(
                f"Retrieved traceability chain for product {product_id}: "
                f"{len(chain.serial_numbers)} serials, "
                f"{len(chain.manufacturing_orders)} MOs, "
                f"{len(chain.deliveries)} deliveries"
            )
            
            return chain
        
        except Exception as e:
            logger.error(f"Failed to get traceability chain: {e}")
            self.stats['errors'] += 1
            raise TraceabilityError(f"Chain retrieval failed: {e}") from e
    
    # ═══════════════════════════════════════════════════════════════════════════
    # EXPORT & REPORTING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def export_chain_to_csv(
        self,
        chain: TraceabilityChain,
        filepath: str,
    ) -> bool:
        """
        Export traceability chain to CSV.
        
        Args:
            chain: TraceabilityChain record
            filepath: Output file path
        
        Returns:
            True if successful
        """
        try:
            import csv
            
            with open(filepath, 'w', newline='') as f:
                writer = csv.writer(f)
                
                # Header
                writer.writerow([
                    'Product ID', 'Product Name',
                    'Serial Number', 'Batch ID',
                    'MO ID', 'Delivery ID'
                ])
                
                # Data
                for serial in chain.serial_numbers:
                    writer.writerow([
                        chain.product_id,
                        chain.product_name,
                        serial.name,
                        serial.batch_id or '',
                        ', '.join(str(m) for m in chain.manufacturing_orders),
                        ', '.join(str(d) for d in chain.deliveries),
                    ])
            
            logger.info(f"Exported traceability chain to {filepath}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to export chain: {e}")
            return False
    
    # ═══════════════════════════════════════════════════════════════════════════
    # AUDIT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _audit_log(self, data: Dict[str, Any]) -> None:
        """Add audit entry."""
        data['timestamp'] = datetime.now().isoformat()
        self.audit_log.append(data)
    
    def _parse_datetime(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse Odoo datetime string."""
        if not date_str:
            return None
        
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except:
            return None
    
    # ═══════════════════════════════════════════════════════════════════════════
    # MAIN
    # ═══════════════════════════════════════════════════════════════════════════
    
    def run(self) -> Dict[str, int]:
        """
        Main entry point.
        
        Returns:
            Statistics dict
        """
        log_header("TRACEABILITY MANAGER")
        
        try:
            log_info("Traceability Manager running")
            
            # Summary
            log_info("Traceability Manager Statistics:")
            for key, value in self.stats.items():
                log_info(f"  {key}: {value}")
            
            return self.stats
        
        except Exception as e:
            log_error(f"Traceability manager failed: {e}", exc_info=True)
            raise


# ═══════════════════════════════════════════════════════════════════════════════
# SETUP FUNCTION FOR RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

def setup_traceability(client: OdooClient) -> Dict[str, int]:
    """
    Initialize and run traceability manager.
    
    Args:
        client: OdooClient
    
    Returns:
        Statistics dict
    """
    manager = TraceabilityManager(client)
    return manager.run()

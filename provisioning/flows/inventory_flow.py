"""
inventory_flow.py - Stock Inventory & Scrap Flow

Handles:
- Inventory adjustments (Inventurverfahren)
- Scrap bookings (Ausschussbuchungen)
- Location management (Schrottlager)
- Error resilience & audit trail

Production-ready with proper error handling, logging, and statistics.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional, Any
from datetime import datetime
from dataclasses import dataclass

from provisioning.client import OdooClient
from provisioning.core.validation import safe_float, FormatValidator, ValidationError
from provisioning.utils import log_header, log_info, log_success, log_warn, log_error, timed_operation


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class InventoryAdjustment:
    """Inventory adjustment operation."""
    product_id: int
    location_id: int
    counted_qty: float
    reason: str = "Inventory Count"
    inventory_id: Optional[int] = None
    status: str = "pending"  # pending, in_progress, completed, failed


@dataclass
class ScrapRecord:
    """Scrap/waste booking."""
    product_id: int
    quantity: float
    scrap_location_id: int
    reason: str = "Defect"
    scrap_id: Optional[int] = None
    status: str = "pending"


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class InventoryError(Exception):
    """Base inventory flow error."""
    pass


class InventoryValidationError(InventoryError):
    """Validation error in inventory flow."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# INVENTORY FLOW
# ═══════════════════════════════════════════════════════════════════════════════

class InventoryFlow:
    """Handle inventory adjustments and scrap bookings."""
    
    def __init__(self, client: OdooClient):
        """Initialize flow."""
        self.client = client
        
        self.stats = {
            'inventories_created': 0,
            'inventories_validated': 0,
            'scrap_created': 0,
            'scrap_validated': 0,
            'errors': 0,
        }
        
        self.audit_log: list = []
        
        logger.info("InventoryFlow initialized")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # LOCATION MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _get_or_create_scrap_location(self) -> int:
        """
        Get or create scrap location.
        
        Returns:
            Location ID
        """
        # Search existing
        locations = self.client.search_read(
            'stock.location',
            [
                ('name', '=', 'Scrap'),
                ('usage', 'in', ['production', 'inventory']),
            ],
            ['id'],
            limit=1,
        )
        
        if locations:
            logger.debug(f"Using existing scrap location {locations[0]['id']}")
            return locations[0]['id']
        
        # Create new
        loc_id = self.client.create(
            'stock.location',
            {
                'name': 'Scrap',
                'usage': 'production',  # Scrap is production waste
                'active': True,
            }
        )
        
        if isinstance(loc_id, (list, tuple)):
            if not loc_id:
                raise InventoryError("Failed to create scrap location")
            loc_id = loc_id[0]
        
        logger.info(f"Created scrap location {loc_id}")
        return int(loc_id)
    
    def _get_default_internal_location(self) -> Optional[int]:
        """Get default internal warehouse location."""
        locations = self.client.search_read(
            'stock.location',
            [('usage', '=', 'internal')],
            ['id'],
            limit=1,
        )
        
        if not locations:
            logger.warning("No internal stock location found")
            return None
        
        return locations[0]['id']
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PRODUCT MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _find_or_validate_product(self, product_name: str) -> int:
        """
        Find product by name.
        
        Args:
            product_name: Product name to search
        
        Returns:
            Product ID
        
        Raises:
            InventoryValidationError: If product not found
        """
        products = self.client.search_read(
            'product.product',
            [('name', '=', product_name)],
            ['id'],
            limit=1,
        )
        
        if not products:
            raise InventoryValidationError(f"Product not found: {product_name}")
        
        return products[0]['id']
    
    # ═══════════════════════════════════════════════════════════════════════════
    # INVENTORY ADJUSTMENTS
    # ═══════════════════════════════════════════════════════════════════════════
    
    def create_inventory_adjustment(
        self,
        product_id: int,
        location_id: int,
        counted_qty: float,
        reason: str = "Inventory Count",
    ) -> InventoryAdjustment:
        """
        Create inventory adjustment record.
        
        Args:
            product_id: Product ID
            location_id: Location ID
            counted_qty: Counted quantity
            reason: Adjustment reason
        
        Returns:
            InventoryAdjustment record
        
        Raises:
            InventoryValidationError: If validation fails
        """
        # Validate
        if counted_qty < 0:
            raise InventoryValidationError(f"Quantity cannot be negative: {counted_qty}")
        
        # Create inventory header
        try:
            inv_id = self.client.create(
                'stock.inventory',
                {
                    'name': f"Inventory {datetime.now().isoformat(timespec='seconds')}",
                    'product_ids': [(6, 0, [product_id])],
                    'location_ids': [(6, 0, [location_id])],
                }
            )
            
            if isinstance(inv_id, (list, tuple)):
                if not inv_id:
                    raise InventoryError("create() returned empty result")
                inv_id = inv_id[0]
            
            inv_id = int(inv_id)
            self.stats['inventories_created'] += 1
            
            logger.info(f"Created inventory adjustment {inv_id}")
            
            return InventoryAdjustment(
                product_id=product_id,
                location_id=location_id,
                counted_qty=counted_qty,
                reason=reason,
                inventory_id=inv_id,
                status='pending',
            )
        
        except Exception as e:
            logger.error(f"Failed to create inventory: {e}")
            self.stats['errors'] += 1
            raise InventoryError(f"Failed to create inventory: {e}") from e
    
    def validate_inventory_adjustment(
        self,
        adjustment: InventoryAdjustment,
    ) -> bool:
        """
        Validate and apply inventory adjustment.
        
        Args:
            adjustment: InventoryAdjustment record
        
        Returns:
            True if successful
        """
        if not adjustment.inventory_id:
            raise InventoryValidationError("Inventory ID missing")
        
        try:
            # Start inventory
            self.client.execute(
                'stock.inventory',
                'action_start',
                [adjustment.inventory_id],
            )
            
            logger.debug(f"Started inventory {adjustment.inventory_id}")
            
            # Update counted quantities
            lines = self.client.search_read(
                'stock.inventory.line',
                [('inventory_id', '=', adjustment.inventory_id)],
                ['id'],
            )
            
            for line in lines:
                self.client.write(
                    'stock.inventory.line',
                    [line['id']],
                    {'product_qty': adjustment.counted_qty},
                )
            
            logger.debug(f"Updated {len(lines)} inventory lines")
            
            # Validate inventory
            self.client.execute(
                'stock.inventory',
                'action_validate',
                [adjustment.inventory_id],
            )
            
            adjustment.status = 'completed'
            self.stats['inventories_validated'] += 1
            
            logger.info(f"Validated inventory {adjustment.inventory_id}")
            
            self._audit_log({
                'operation': 'inventory_adjustment',
                'inventory_id': adjustment.inventory_id,
                'product_id': adjustment.product_id,
                'location_id': adjustment.location_id,
                'quantity': adjustment.counted_qty,
                'reason': adjustment.reason,
                'status': 'completed',
            })
            
            return True
        
        except Exception as e:
            adjustment.status = 'failed'
            self.stats['errors'] += 1
            logger.error(f"Failed to validate inventory: {e}", exc_info=True)
            return False
    
    def run_inventory_adjustment(
        self,
        product_id: int,
        location_id: int,
        counted_qty: float,
        reason: str = "Inventory Count",
    ) -> bool:
        """
        Run complete inventory adjustment (create + validate).
        
        Args:
            product_id: Product ID
            location_id: Location ID
            counted_qty: Counted quantity
            reason: Adjustment reason
        
        Returns:
            True if successful
        """
        try:
            # Create
            adjustment = self.create_inventory_adjustment(
                product_id, location_id, counted_qty, reason
            )
            
            # Validate
            return self.validate_inventory_adjustment(adjustment)
        
        except Exception as e:
            logger.error(f"Inventory adjustment failed: {e}", exc_info=True)
            self.stats['errors'] += 1
            return False
    
    # ═══════════════════════════════════════════════════════════════════════════
    # SCRAP BOOKINGS
    # ═══════════════════════════════════════════════════════════════════════════
    
    def create_scrap_record(
        self,
        product_id: int,
        quantity: float,
        reason: str = "Defect",
    ) -> ScrapRecord:
        """
        Create scrap record.
        
        Args:
            product_id: Product ID
            quantity: Scrap quantity
            reason: Reason for scrap
        
        Returns:
            ScrapRecord
        
        Raises:
            InventoryValidationError: If validation fails
        """
        # Validate
        if quantity <= 0:
            raise InventoryValidationError(f"Scrap quantity must be > 0: {quantity}")
        
        # Get scrap location
        scrap_location_id = self._get_or_create_scrap_location()
        
        # Create scrap
        try:
            scrap_id = self.client.create(
                'stock.scrap',
                {
                    'product_id': product_id,
                    'scrap_qty': quantity,
                    'scrap_location_id': scrap_location_id,
                    'reason': reason,
                }
            )
            
            if isinstance(scrap_id, (list, tuple)):
                if not scrap_id:
                    raise InventoryError("create() returned empty result")
                scrap_id = scrap_id[0]
            
            scrap_id = int(scrap_id)
            self.stats['scrap_created'] += 1
            
            logger.info(f"Created scrap record {scrap_id}")
            
            return ScrapRecord(
                product_id=product_id,
                quantity=quantity,
                scrap_location_id=scrap_location_id,
                reason=reason,
                scrap_id=scrap_id,
                status='pending',
            )
        
        except Exception as e:
            logger.error(f"Failed to create scrap: {e}")
            self.stats['errors'] += 1
            raise InventoryError(f"Failed to create scrap: {e}") from e
    
    def validate_scrap_record(self, scrap: ScrapRecord) -> bool:
        """
        Validate and apply scrap record.
        
        Args:
            scrap: ScrapRecord
        
        Returns:
            True if successful
        """
        if not scrap.scrap_id:
            raise InventoryValidationError("Scrap ID missing")
        
        try:
            self.client.execute(
                'stock.scrap',
                'action_validate',
                [scrap.scrap_id],
            )
            
            scrap.status = 'completed'
            self.stats['scrap_validated'] += 1
            
            logger.info(f"Validated scrap {scrap.scrap_id}")
            
            self._audit_log({
                'operation': 'scrap_booking',
                'scrap_id': scrap.scrap_id,
                'product_id': scrap.product_id,
                'quantity': scrap.quantity,
                'reason': scrap.reason,
                'status': 'completed',
            })
            
            return True
        
        except Exception as e:
            scrap.status = 'failed'
            self.stats['errors'] += 1
            logger.error(f"Failed to validate scrap: {e}", exc_info=True)
            return False
    
    def run_scrap_booking(
        self,
        product_id: int,
        quantity: float,
        reason: str = "Defect",
    ) -> bool:
        """
        Run complete scrap booking (create + validate).
        
        Args:
            product_id: Product ID
            quantity: Scrap quantity
            reason: Reason
        
        Returns:
            True if successful
        """
        try:
            # Create
            scrap = self.create_scrap_record(product_id, quantity, reason)
            
            # Validate
            return self.validate_scrap_record(scrap)
        
        except Exception as e:
            logger.error(f"Scrap booking failed: {e}", exc_info=True)
            self.stats['errors'] += 1
            return False
    
    # ═══════════════════════════════════════════════════════════════════════════
    # AUDIT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _audit_log(self, data: Dict[str, Any]) -> None:
        """Add audit entry."""
        data['timestamp'] = datetime.now().isoformat()
        self.audit_log.append(data)
    
    # ═══════════════════════════════════════════════════════════════════════════
    # MAIN
    # ═══════════════════════════════════════════════════════════════════════════
    
    def run(self) -> Dict[str, int]:
        """
        Main entry point.
        
        Returns:
            Statistics dict
        """
        log_header("INVENTORY FLOW")
        
        try:
            # Demo inventory adjustment
            location_id = self._get_default_internal_location()
            if location_id:
                try:
                    product_id = self._find_or_validate_product("Akku")
                    success = self.run_inventory_adjustment(
                        product_id=product_id,
                        location_id=location_id,
                        counted_qty=10.0,
                        reason="Stock Count",
                    )
                    
                    if success:
                        log_success("Inventory adjustment completed")
                    else:
                        log_warn("Inventory adjustment failed")
                
                except InventoryValidationError as e:
                    log_warn(f"Inventory skipped: {e}")
            
            # Summary
            log_info("Inventory Flow Statistics:")
            for key, value in self.stats.items():
                log_info(f"  {key}: {value}")
            
            return self.stats
        
        except Exception as e:
            log_error(f"Inventory flow failed: {e}", exc_info=True)
            raise


# ═══════════════════════════════════════════════════════════════════════════════
# SETUP FUNCTION FOR RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

def setup_inventory_flows(client: OdooClient) -> Dict[str, int]:
    """
    Initialize and run inventory flows.
    
    Args:
        client: OdooClient
    
    Returns:
        Statistics dict
    """
    flow = InventoryFlow(client)
    return flow.run()

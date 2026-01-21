"""
manufacturing_flow.py - Manufacturing Order Flow

Handles:
- MO creation from sales orders (Make-To-Order)
- Material reservation and picking
- Production start/finish with quality control
- Work order management
- Proper error handling and statistics

Production-ready with validation, error handling, and audit trail.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime

from client import OdooClient
from validation import safe_float, validate_int, ValidationError
from utils import (
    log_header, log_info, log_success, log_warn, log_error,
    timed_operation,
)


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class SalesOrderLine:
    """Sales order line."""
    id: int
    order_id: int
    product_id: int
    product_name: str
    quantity: float
    uom_id: int


@dataclass
class ManufacturingOrder:
    """Manufacturing order."""
    id: int
    product_id: int
    quantity: float
    bom_id: Optional[int] = None
    state: str = "draft"  # draft, confirmed, in_progress, done, cancel
    qty_produced: float = 0.0
    
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class ManufacturingError(Exception):
    """Base manufacturing error."""
    pass


class MOValidationError(ManufacturingError):
    """Manufacturing order validation error."""
    pass


class BOMError(ManufacturingError):
    """BOM-related error."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# MANUFACTURING FLOW
# ═══════════════════════════════════════════════════════════════════════════════

class ManufacturingFlow:
    """Manage manufacturing orders and production processes."""
    
    def __init__(self, client: OdooClient):
        """Initialize manufacturing flow."""
        self.client = client
        
        self.stats = {
            'so_lines_processed': 0,
            'mos_created': 0,
            'mos_confirmed': 0,
            'mos_started': 0,
            'mos_finished': 0,
            'materials_reserved': 0,
            'errors': 0,
        }
        
        self.audit_log: List[Dict[str, Any]] = []
        
        logger.info("ManufacturingFlow initialized")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # SALES ORDER PROCESSING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _validate_sales_orders(self, order_ids: List[int]) -> None:
        """
        Validate sales orders.
        
        Args:
            order_ids: List of order IDs
        
        Raises:
            MOValidationError: If validation fails
        """
        if not order_ids:
            raise MOValidationError("No order IDs provided")
        
        for order_id in order_ids:
            if not isinstance(order_id, int) or order_id <= 0:
                raise MOValidationError(f"Invalid order ID: {order_id}")
        
        logger.debug(f"Validated {len(order_ids)} order IDs")
    
    def _read_so_lines(self, order_id: int) -> List[SalesOrderLine]:
        """
        Read sales order lines.
        
        Args:
            order_id: Sales order ID
        
        Returns:
            List of SalesOrderLine records
        """
        lines_data = self.client.search_read(
            'sale.order.line',
            [('order_id', '=', order_id)],
            [
                'id',
                'product_id',
                'product_uom_qty',
                'product_uom',
            ],
            limit=100,
        )
        
        if not lines_data:
            logger.warning(f"No lines found for sales order {order_id}")
            return []
        
        lines = []
        for line_data in lines_data:
            try:
                product_id = line_data.get('product_id')
                if isinstance(product_id, (list, tuple)):
                    product_id = product_id[0]
                
                qty = safe_float(
                    line_data.get('product_uom_qty', 1.0),
                    default=1.0,
                    allow_negative=False,
                    field_name=f"line_{line_data['id']}_qty",
                )
                
                lines.append(SalesOrderLine(
                    id=line_data['id'],
                    order_id=order_id,
                    product_id=int(product_id),
                    product_name=line_data.get('product_name', ''),
                    quantity=qty,
                    uom_id=line_data.get('product_uom', [0])[0] if isinstance(
                        line_data.get('product_uom'), (list, tuple)
                    ) else line_data.get('product_uom', 0),
                ))
            
            except Exception as e:
                logger.error(f"Failed to parse SO line {line_data['id']}: {e}")
                self.stats['errors'] += 1
        
        return lines
    
    # ═══════════════════════════════════════════════════════════════════════════
    # BOM MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _find_bom_for_product(self, product_id: int) -> Optional[Dict[str, Any]]:
        """
        Find applicable BOM for product.
        
        Args:
            product_id: Product ID
        
        Returns:
            BOM record or None
        """
        boms = self.client.search_read(
            'mrp.bom',
            [('product_id', '=', product_id)],
            ['id', 'name'],
            limit=1,
        )
        
        if boms:
            return boms[0]
        
        logger.warning(f"No BOM found for product {product_id}")
        return None
    
    def _validate_product_for_manufacturing(self, product_id: int) -> bool:
        """
        Validate product can be manufactured.
        
        Args:
            product_id: Product ID
        
        Returns:
            True if valid
        
        Raises:
            MOValidationError: If invalid
        """
        try:
            products = self.client.search_read(
                'product.product',
                [('id', '=', product_id)],
                ['type', 'name'],
                limit=1,
            )
            
            if not products:
                raise MOValidationError(f"Product not found: {product_id}")
            
            product = products[0]
            
            # Check product type
            if product.get('type') not in ['product', 'consu']:
                raise MOValidationError(
                    f"Product {product['name']} is not manufactureable "
                    f"(type={product['type']})"
                )
            
            # Check BOM exists
            bom = self._find_bom_for_product(product_id)
            if not bom:
                logger.warning(
                    f"No BOM for product {product['name']}, "
                    f"MO creation may fail"
                )
            
            return True
        
        except Exception as e:
            raise MOValidationError(f"Product validation failed: {e}") from e
    
    # ═══════════════════════════════════════════════════════════════════════════
    # MO CREATION
    # ═══════════════════════════════════════════════════════════════════════════
    
    def create_mo_from_so_line(
        self,
        so_line: SalesOrderLine,
    ) -> Optional[ManufacturingOrder]:
        """
        Create manufacturing order from sales order line.
        
        Args:
            so_line: SalesOrderLine record
        
        Returns:
            ManufacturingOrder or None if failed
        """
        try:
            # Validate product
            self._validate_product_for_manufacturing(so_line.product_id)
            
            # Find BOM
            bom = self._find_bom_for_product(so_line.product_id)
            
            # Create MO
            mo_vals = {
                'product_id': so_line.product_id,
                'product_qty': so_line.quantity,
                'origin': f'SO-{so_line.order_id}',
                'state': 'draft',
            }
            
            if bom:
                mo_vals['bom_id'] = bom['id']
            
            mo_id = self.client.create('mrp.production', mo_vals)
            
            if isinstance(mo_id, (list, tuple)):
                if not mo_id:
                    raise ManufacturingError("create() returned empty result")
                mo_id = mo_id[0]
            
            mo_id = int(mo_id)
            self.stats['mos_created'] += 1
            
            logger.info(
                f"Created MO {mo_id} from SO line {so_line.id} "
                f"(product={so_line.product_id}, qty={so_line.quantity})"
            )
            
            self._audit_log({
                'operation': 'mo_created',
                'mo_id': mo_id,
                'so_line_id': so_line.id,
                'product_id': so_line.product_id,
                'quantity': so_line.quantity,
            })
            
            return ManufacturingOrder(
                id=mo_id,
                product_id=so_line.product_id,
                quantity=so_line.quantity,
                bom_id=bom['id'] if bom else None,
                created_at=datetime.now(),
            )
        
        except Exception as e:
            logger.error(
                f"Failed to create MO from SO line {so_line.id}: {e}",
                exc_info=True,
            )
            self.stats['errors'] += 1
            return None
    
    def create_mos_from_sales_orders(
        self,
        order_ids: List[int],
    ) -> List[ManufacturingOrder]:
        """
        Create MOs from sales orders.
        
        Args:
            order_ids: List of sales order IDs
        
        Returns:
            List of created ManufacturingOrder records
        """
        logger.info(f"Creating MOs from {len(order_ids)} sales orders")
        
        try:
            self._validate_sales_orders(order_ids)
        except MOValidationError as e:
            logger.error(f"Sales order validation failed: {e}")
            self.stats['errors'] += 1
            return []
        
        mos = []
        
        for order_id in order_ids:
            try:
                so_lines = self._read_so_lines(order_id)
                self.stats['so_lines_processed'] += len(so_lines)
                
                for so_line in so_lines:
                    mo = self.create_mo_from_so_line(so_line)
                    if mo:
                        mos.append(mo)
            
            except Exception as e:
                logger.error(f"Failed to process sales order {order_id}: {e}")
                self.stats['errors'] += 1
        
        logger.info(f"Created {len(mos)} MOs from sales orders")
        
        return mos
    
    # ═══════════════════════════════════════════════════════════════════════════
    # MO LIFECYCLE
    # ═══════════════════════════════════════════════════════════════════════════
    
    def confirm_mo(self, mo: ManufacturingOrder) -> bool:
        """
        Confirm manufacturing order.
        
        Args:
            mo: ManufacturingOrder
        
        Returns:
            True if successful
        """
        try:
            self.client.execute(
                'mrp.production',
                'action_confirm',
                [mo.id],
            )
            
            mo.state = 'confirmed'
            self.stats['mos_confirmed'] += 1
            
            logger.info(f"Confirmed MO {mo.id}")
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to confirm MO {mo.id}: {e}")
            self.stats['errors'] += 1
            return False
    
    def start_mo(self, mo: ManufacturingOrder) -> bool:
        """
        Start manufacturing order (assign materials, create work orders).
        
        Args:
            mo: ManufacturingOrder
        
        Returns:
            True if successful
        """
        try:
            # Confirm if not already
            if mo.state == 'draft':
                if not self.confirm_mo(mo):
                    return False
            
            # Assign materials
            self.client.execute(
                'mrp.production',
                'action_assign',
                [mo.id],
            )
            
            mo.state = 'in_progress'
            mo.started_at = datetime.now()
            self.stats['mos_started'] += 1
            
            logger.info(f"Started MO {mo.id}")
            
            self._audit_log({
                'operation': 'mo_started',
                'mo_id': mo.id,
                'quantity': mo.quantity,
            })
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to start MO {mo.id}: {e}", exc_info=True)
            self.stats['errors'] += 1
            return False
    
    def finish_mo(
        self,
        mo: ManufacturingOrder,
        qty_produced: Optional[float] = None,
        quality_passed: bool = True,
    ) -> bool:
        """
        Finish manufacturing order.
        
        Args:
            mo: ManufacturingOrder
            qty_produced: Actual produced quantity (if different from planned)
            quality_passed: Whether quality check passed
        
        Returns:
            True if successful
        """
        try:
            # Validate quantity
            if qty_produced is not None:
                qty_produced = safe_float(
                    qty_produced,
                    default=mo.quantity,
                    allow_negative=False,
                )
                
                if qty_produced <= 0:
                    raise MOValidationError("qty_produced must be > 0")
            else:
                qty_produced = mo.quantity
            
            # Check quality if required
            if not quality_passed:
                logger.warning(f"MO {mo.id} quality check failed, not finishing")
                self.stats['errors'] += 1
                return False
            
            # Mark done (Odoo 14+)
            try:
                self.client.execute(
                    'mrp.production',
                    'button_mark_done',
                    [mo.id],
                )
            except Exception:
                # Fallback for older versions
                logger.debug("button_mark_done not available, trying action_finish")
                self.client.execute(
                    'mrp.production',
                    'action_finish',
                    [mo.id],
                )
            
            mo.state = 'done'
            mo.qty_produced = qty_produced
            mo.finished_at = datetime.now()
            self.stats['mos_finished'] += 1
            
            logger.info(
                f"Finished MO {mo.id} "
                f"(planned={mo.quantity}, produced={qty_produced})"
            )
            
            self._audit_log({
                'operation': 'mo_finished',
                'mo_id': mo.id,
                'quantity_planned': mo.quantity,
                'quantity_produced': qty_produced,
                'quality_passed': quality_passed,
            })
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to finish MO {mo.id}: {e}", exc_info=True)
            self.stats['errors'] += 1
            return False
    
    # ═══════════════════════════════════════════════════════════════════════════
    # DEMO
    # ═══════════════════════════════════════════════════════════════════════════
    
    def run_demo_mo_chain(
        self,
        order_ids: List[int],
    ) -> List[ManufacturingOrder]:
        """
        Run complete MO chain: create → confirm → start → finish.
        
        Args:
            order_ids: Sales order IDs
        
        Returns:
            List of completed MOs
        """
        logger.info(
            f"Running demo MO chain for {len(order_ids)} sales orders"
        )
        
        # Create MOs
        mos = self.create_mos_from_sales_orders(order_ids)
        
        if not mos:
            logger.warning("No MOs created, skipping demo chain")
            return []
        
        completed_mos = []
        
        for mo in mos:
            # Confirm
            if not self.confirm_mo(mo):
                continue
            
            # Start
            if not self.start_mo(mo):
                continue
            
            # Finish
            if self.finish_mo(mo, quality_passed=True):
                completed_mos.append(mo)
        
        logger.info(
            f"Completed demo MO chain: "
            f"{len(completed_mos)}/{len(mos)} MOs finished"
        )
        
        return completed_mos
    
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
        log_header("MANUFACTURING FLOW")
        
        try:
            # Run demo with sample data
            # In real scenario, would call with actual SO IDs
            demo_order_ids = []  # TODO: Get from Odoo or config
            
            if demo_order_ids:
                mos = self.run_demo_mo_chain(demo_order_ids)
                if mos:
                    log_success(f"Completed {len(mos)} MOs")
            else:
                log_warn("No demo order IDs provided, initializing only")
            
            # Summary
            log_info("Manufacturing Flow Statistics:")
            for key, value in self.stats.items():
                log_info(f"  {key}: {value}")
            
            return self.stats
        
        except Exception as e:
            log_error(f"Manufacturing flow failed: {e}", exc_info=True)
            raise


# ═══════════════════════════════════════════════════════════════════════════════
# SETUP FUNCTION FOR RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

def setup_mrp_flows(client: OdooClient) -> Dict[str, int]:
    """
    Initialize and run manufacturing flows.
    
    Args:
        client: OdooClient
    
    Returns:
        Statistics dict
    """
    flow = ManufacturingFlow(client)
    return flow.run()

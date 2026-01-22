"""
purchase_flow.py - Purchase Order Flow

Handles:
- RFQ (Request for Quote) creation
- Supplier management and validation
- Purchase order confirmation
- Goods receipt and picking validation
- Supplier info management (pricing, lead times)
- Proper error handling and statistics

Production-ready with validation, error handling, and audit trail.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime

from provisioning.client import OdooClient
from provisioning.core.validation import safe_float, FormatValidator, ValidationError
from provisioning.utils import log_header, log_info, log_success, log_warn, log_error, timed_operation


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Supplier:
    """Supplier/Vendor record."""
    id: int
    name: str
    code: str
    email: Optional[str] = None
    supplier_rank: int = 1


@dataclass
class SupplierInfo:
    """Supplier-specific pricing and terms."""
    id: int
    product_id: int
    supplier_id: int
    price: float
    min_qty: float = 1.0
    lead_days: int = 7


@dataclass
class PurchaseOrderLine:
    """Purchase order line."""
    id: int
    order_id: int
    product_id: int
    product_name: str
    quantity: float
    price_unit: float
    subtotal: float


@dataclass
class PurchaseOrder:
    """Purchase order."""
    id: int
    name: str
    supplier_id: int
    supplier_name: str
    state: str = "draft"  # draft, sent, confirmed, done, cancel
    lines: List[PurchaseOrderLine] = None
    
    created_at: Optional[datetime] = None
    confirmed_at: Optional[datetime] = None
    received_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.lines is None:
            self.lines = []
    
    @property
    def total_amount(self) -> float:
        """Total PO amount."""
        return sum(line.subtotal for line in self.lines)


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class PurchaseError(Exception):
    """Base purchase error."""
    pass


class SupplierError(PurchaseError):
    """Supplier validation error."""
    pass


class ProductError(PurchaseError):
    """Product validation error."""
    pass


class POValidationError(PurchaseError):
    """Purchase order validation error."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# PURCHASE FLOW
# ═══════════════════════════════════════════════════════════════════════════════

class PurchaseFlow:
    """Manage purchase orders and supplier interactions."""
    
    def __init__(self, client: OdooClient):
        """Initialize purchase flow."""
        self.client = client
        
        self.stats = {
            'suppliers_found': 0,
            'products_found': 0,
            'pos_created': 0,
            'po_lines_created': 0,
            'pos_confirmed': 0,
            'pickings_processed': 0,
            'goods_received': 0,
            'errors': 0,
        }
        
        self.audit_log: List[Dict[str, Any]] = []
        
        logger.info("PurchaseFlow initialized")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # SUPPLIER MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def find_supplier(self, name: str) -> Optional[Supplier]:
        """
        Find supplier by name.
        
        Args:
            name: Supplier name
        
        Returns:
            Supplier record or None
        
        Raises:
            SupplierError: If validation fails
        """
        if not name or not isinstance(name, str):
            raise SupplierError(f"Invalid supplier name: {name}")
        
        try:
            suppliers = self.client.search_read(
                'res.partner',
                [
                    ('name', '=', name),
                    ('supplier_rank', '>', 0),
                    ('is_company', '=', True),
                ],
                ['id', 'name', 'code', 'email', 'supplier_rank'],
                limit=1,
            )
            
            if not suppliers:
                return None
            
            supplier_data = suppliers[0]
            
            self.stats['suppliers_found'] += 1
            
            return Supplier(
                id=supplier_data['id'],
                name=supplier_data.get('name', ''),
                code=supplier_data.get('code', ''),
                email=supplier_data.get('email'),
                supplier_rank=supplier_data.get('supplier_rank', 1),
            )
        
        except Exception as e:
            logger.error(f"Failed to find supplier {name}: {e}")
            raise SupplierError(f"Supplier lookup failed: {e}") from e
    
    def get_supplier_info(
        self,
        supplier_id: int,
        product_id: int,
    ) -> Optional[SupplierInfo]:
        """
        Get supplier-specific pricing and terms.
        
        Args:
            supplier_id: Supplier ID
            product_id: Product ID
        
        Returns:
            SupplierInfo record or None
        """
        try:
            info = self.client.search_read(
                'purchase.supplier.info',
                [
                    ('partner_id', '=', supplier_id),
                    ('product_id', '=', product_id),
                ],
                ['id', 'price', 'min_qty', 'delay'],
                limit=1,
            )
            
            if not info:
                return None
            
            info_data = info[0]
            
            return SupplierInfo(
                id=info_data['id'],
                product_id=product_id,
                supplier_id=supplier_id,
                price=safe_float(info_data.get('price', 0.0)),
                min_qty=safe_float(info_data.get('min_qty', 1.0)),
                lead_days=int(info_data.get('delay', 7)),
            )
        
        except Exception as e:
            logger.warning(f"Failed to get supplier info: {e}")
            return None
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PRODUCT MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def find_product(self, name: str) -> Optional[int]:
        """
        Find product by name.
        
        Args:
            name: Product name
        
        Returns:
            Product ID or None
        
        Raises:
            ProductError: If validation fails
        """
        if not name or not isinstance(name, str):
            raise ProductError(f"Invalid product name: {name}")
        
        try:
            products = self.client.search_read(
                'product.product',
                [('name', '=', name)],
                ['id'],
                limit=1,
            )
            
            if not products:
                return None
            
            self.stats['products_found'] += 1
            
            return products[0]['id']
        
        except Exception as e:
            logger.error(f"Failed to find product {name}: {e}")
            raise ProductError(f"Product lookup failed: {e}") from e
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PO CREATION
    # ═══════════════════════════════════════════════════════════════════════════
    
    def validate_po_line_params(
        self,
        quantity: float,
        price_unit: Optional[float] = None,
    ) -> tuple[float, float]:
        """
        Validate and normalize PO line parameters.
        
        Args:
            quantity: Order quantity
            price_unit: Price per unit (optional)
        
        Returns:
            Tuple of (quantity, price_unit)
        
        Raises:
            POValidationError: If validation fails
        """
        if quantity <= 0:
            raise POValidationError(f"Quantity must be > 0, got {quantity}")
        
        if price_unit is not None:
            if price_unit < 0:
                raise POValidationError(f"Price cannot be negative: {price_unit}")
        else:
            price_unit = 0.0
        
        return quantity, price_unit
    
    def create_rfq(
        self,
        supplier_name: str,
        product_name: str,
        quantity: float = 1.0,
        price_unit: Optional[float] = None,
    ) -> int:
        """
        Create Request for Quote (purchase order in draft state).
        
        Args:
            supplier_name: Supplier name
            product_name: Product name
            quantity: Order quantity
            price_unit: Price per unit (optional, uses supplier info if not provided)
        
        Returns:
            PO ID
        
        Raises:
            SupplierError: If supplier not found
            ProductError: If product not found
            POValidationError: If validation fails
            PurchaseError: If creation fails
        """
        logger.info(
            f"Creating RFQ for supplier '{supplier_name}' "
            f"and product '{product_name}' (qty={quantity})"
        )
        
        try:
            # Find supplier
            supplier = self.find_supplier(supplier_name)
            if not supplier:
                raise SupplierError(f"Supplier not found: {supplier_name}")
            
            # Find product
            product_id = self.find_product(product_name)
            if not product_id:
                raise ProductError(f"Product not found: {product_name}")
            
            # Validate parameters
            quantity, price_unit = self.validate_po_line_params(quantity, price_unit)
            
            # Get supplier info if price not provided
            if price_unit == 0.0:
                supplier_info = self.get_supplier_info(supplier.id, product_id)
                if supplier_info:
                    price_unit = supplier_info.price
                    logger.debug(
                        f"Using supplier price: {price_unit} from supplier info"
                    )
            
            # Create PO header
            po_vals = {
                'partner_id': supplier.id,
                'state': 'draft',
            }
            
            po_id = self.client.create('purchase.order', po_vals)
            
            if isinstance(po_id, (list, tuple)):
                if not po_id:
                    raise PurchaseError("create() returned empty result")
                po_id = po_id[0]
            
            po_id = int(po_id)
            self.stats['pos_created'] += 1
            
            logger.debug(f"Created PO {po_id}")
            
            # Create PO line
            try:
                line_vals = {
                    'order_id': po_id,
                    'product_id': product_id,
                    'product_qty': quantity,
                    'price_unit': price_unit,
                }
                
                line_id = self.client.create('purchase.order.line', line_vals)
                
                if isinstance(line_id, (list, tuple)):
                    if not line_id:
                        raise PurchaseError("Line creation returned empty result")
                    line_id = line_id[0]
                
                self.stats['po_lines_created'] += 1
                
                logger.info(f"Created RFQ {po_id} with {quantity} units")
            
            except Exception as e:
                logger.error(f"Failed to create PO line: {e}")
                # Try to delete PO on line creation failure
                try:
                    self.client.execute('purchase.order', 'action_cancel', [po_id])
                except:
                    pass
                raise PurchaseError(f"PO line creation failed: {e}") from e
            
            # Audit
            self._audit_log({
                'operation': 'rfq_created',
                'po_id': po_id,
                'supplier_id': supplier.id,
                'supplier_name': supplier.name,
                'product_id': product_id,
                'product_name': product_name,
                'quantity': quantity,
                'price_unit': price_unit,
            })
            
            return po_id
        
        except Exception as e:
            logger.error(f"RFQ creation failed: {e}", exc_info=True)
            self.stats['errors'] += 1
            raise
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PO CONFIRMATION
    # ═══════════════════════════════════════════════════════════════════════════
    
    def confirm_po(self, po_id: int) -> bool:
        """
        Confirm purchase order (RFQ → Purchase Order).
        
        Args:
            po_id: PO ID
        
        Returns:
            True if successful
        """
        try:
            self.client.execute(
                'purchase.order',
                'button_confirm',
                [po_id],
            )
            
            self.stats['pos_confirmed'] += 1
            
            logger.info(f"Confirmed PO {po_id}")
            
            self._audit_log({
                'operation': 'po_confirmed',
                'po_id': po_id,
            })
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to confirm PO {po_id}: {e}")
            self.stats['errors'] += 1
            return False
    
    # ═══════════════════════════════════════════════════════════════════════════
    # GOODS RECEIPT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def receive_goods(self, po_id: int) -> bool:
        """
        Receive goods for PO (validate incoming picking).
        
        Args:
            po_id: PO ID
        
        Returns:
            True if successful
        """
        logger.info(f"Processing goods receipt for PO {po_id}")
        
        try:
            # Get PO details
            po_data = self.client.search_read(
                'purchase.order',
                [('id', '=', po_id)],
                ['name', 'state'],
                limit=1,
            )
            
            if not po_data:
                raise PurchaseError(f"PO not found: {po_id}")
            
            po_name = po_data[0]['name']
            po_state = po_data[0]['state']
            
            # Find incoming pickings
            pickings = self.client.search_read(
                'stock.picking',
                [
                    ('purchase_id', '=', po_id),
                    ('picking_type_code', '=', 'incoming'),
                    ('state', 'not in', ['done', 'cancel']),
                ],
                ['id', 'name', 'state'],
                limit=10,
            )
            
            if not pickings:
                logger.warning(f"No incoming pickings found for PO {po_id}")
                return False
            
            received_pickings = 0
            
            for picking in pickings:
                picking_id = picking['id']
                picking_name = picking['name']
                
                try:
                    # Validate picking
                    self.client.execute(
                        'stock.picking',
                        'button_validate',
                        [picking_id],
                    )
                    
                    received_pickings += 1
                    self.stats['pickings_processed'] += 1
                    
                    logger.debug(f"Validated picking {picking_name}")
                
                except Exception as e:
                    logger.warning(
                        f"Failed to validate picking {picking_name}: {e}"
                    )
            
            if received_pickings > 0:
                self.stats['goods_received'] += 1
                logger.info(
                    f"Processed goods receipt for PO {po_id} "
                    f"({received_pickings} pickings)"
                )
                
                self._audit_log({
                    'operation': 'goods_received',
                    'po_id': po_id,
                    'pickings_count': received_pickings,
                })
                
                return True
            
            return False
        
        except Exception as e:
            logger.error(f"Goods receipt failed for PO {po_id}: {e}")
            self.stats['errors'] += 1
            return False
    
    # ═══════════════════════════════════════════════════════════════════════════
    # DEMO
    # ═══════════════════════════════════════════════════════════════════════════
    
    def run_demo_po_chain(
        self,
        scenarios: Optional[List[tuple[str, str, float]]] = None,
    ) -> List[int]:
        """
        Run complete PO chain: RFQ → Confirm → Goods Receipt.
        
        Args:
            scenarios: List of (supplier_name, product_name, quantity) tuples
        
        Returns:
            List of PO IDs
        """
        logger.info("Running demo PO chain")
        
        if not scenarios:
            scenarios = [
                ("Amazon EU", "Akku", 10.0),  # Adjust to actual suppliers
                ("Local Supplier", "Haube", 5.0),
            ]
        
        po_ids = []
        
        for supplier_name, product_name, quantity in scenarios:
            try:
                # Create RFQ
                po_id = self.create_rfq(supplier_name, product_name, quantity)
                
                # Confirm
                if not self.confirm_po(po_id):
                    log_warn(f"Failed to confirm PO {po_id}")
                    continue
                
                # Receive goods
                if not self.receive_goods(po_id):
                    log_warn(f"Failed to receive goods for PO {po_id}")
                    continue
                
                po_ids.append(po_id)
                
            except Exception as e:
                logger.warning(f"Demo scenario failed: {e}")
                self.stats['errors'] += 1
        
        return po_ids
    
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
        log_header("PURCHASE FLOW")
        
        try:
            po_ids = self.run_demo_po_chain()
            
            if po_ids:
                log_success(f"Completed {len(po_ids)} PO chains")
            else:
                log_warn("No demo POs completed (check suppliers/products)")
            
            # Summary
            log_info("Purchase Flow Statistics:")
            for key, value in self.stats.items():
                log_info(f"  {key}: {value}")
            
            return self.stats
        
        except Exception as e:
            log_error(f"Purchase flow failed: {e}", exc_info=True)
            raise


# ═══════════════════════════════════════════════════════════════════════════════
# SETUP FUNCTION FOR RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

def setup_purchase_flows(client: OdooClient) -> Dict[str, int]:
    """
    Initialize and run purchase flows.
    
    Args:
        client: OdooClient
    
    Returns:
        Statistics dict
    """
    flow = PurchaseFlow(client)
    return flow.run()

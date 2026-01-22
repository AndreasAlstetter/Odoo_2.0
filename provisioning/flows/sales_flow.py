"""
sales_flow.py - Sales Order Flow

Handles:
- Customer/supplier quotations
- Order creation and confirmation
- Sales scenarios and demos
- Order validation and status tracking
- Proper error handling and statistics

Production-ready with validation, error handling, and audit trail.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Any, Tuple
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
class Customer:
    """Customer record."""
    id: int
    name: str
    email: Optional[str] = None
    customer_rank: int = 1


@dataclass
class SalesOrderLine:
    """Sales order line."""
    id: int
    product_id: int
    product_name: str
    quantity: float
    price_unit: float
    discount: float = 0.0
    
    @property
    def subtotal(self) -> float:
        """Subtotal before discount."""
        return self.quantity * self.price_unit
    
    @property
    def total_discounted(self) -> float:
        """Total after discount."""
        return self.subtotal * (1 - self.discount / 100)


@dataclass
class SalesOrder:
    """Sales order."""
    id: int
    name: str
    customer_id: int
    customer_name: str
    state: str = "draft"  # draft, sent, confirmed, done, cancel
    lines: List[SalesOrderLine] = None
    
    created_at: Optional[datetime] = None
    confirmed_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.lines is None:
            self.lines = []
    
    @property
    def total_amount(self) -> float:
        """Total order amount."""
        return sum(line.total_discounted for line in self.lines)


@dataclass
class SalesScenario:
    """Sales scenario definition."""
    name: str
    customer_name: str
    lines: List[Tuple[str, float, float]]  # (product_name, quantity, discount)


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class SalesError(Exception):
    """Base sales error."""
    pass


class CustomerError(SalesError):
    """Customer validation error."""
    pass


class ProductError(SalesError):
    """Product validation error."""
    pass


class QuotationError(SalesError):
    """Quotation/order error."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# SALES FLOW
# ═══════════════════════════════════════════════════════════════════════════════

class SalesFlow:
    """Manage sales quotations and orders."""
    
    def __init__(self, client: OdooClient):
        """Initialize sales flow."""
        self.client = client
        
        self.stats = {
            'customers_found': 0,
            'products_found': 0,
            'quotations_created': 0,
            'orders_confirmed': 0,
            'scenarios_completed': 0,
            'errors': 0,
        }
        
        self.audit_log: List[Dict[str, Any]] = []
        
        logger.info("SalesFlow initialized")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # CUSTOMER MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def find_customer(self, name: str) -> Optional[Customer]:
        """
        Find customer by name.
        
        Args:
            name: Customer name
        
        Returns:
            Customer record or None
        
        Raises:
            CustomerError: If validation fails
        """
        if not name or not isinstance(name, str):
            raise CustomerError(f"Invalid customer name: {name}")
        
        try:
            customers = self.client.search_read(
                'res.partner',
                [
                    ('name', '=', name),
                    ('customer_rank', '>', 0),
                ],
                ['id', 'name', 'email', 'customer_rank'],
                limit=1,
            )
            
            if not customers:
                return None
            
            customer_data = customers[0]
            
            self.stats['customers_found'] += 1
            
            return Customer(
                id=customer_data['id'],
                name=customer_data.get('name', ''),
                email=customer_data.get('email'),
                customer_rank=customer_data.get('customer_rank', 1),
            )
        
        except Exception as e:
            logger.error(f"Failed to find customer {name}: {e}")
            raise CustomerError(f"Customer lookup failed: {e}") from e
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PRODUCT MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def find_product(self, name: str) -> Optional[Tuple[int, float]]:
        """
        Find product by name.
        
        Args:
            name: Product name
        
        Returns:
            Tuple of (product_id, list_price) or None
        
        Raises:
            ProductError: If validation fails
        """
        if not name or not isinstance(name, str):
            raise ProductError(f"Invalid product name: {name}")
        
        try:
            products = self.client.search_read(
                'product.product',
                [('name', '=', name)],
                ['id', 'list_price'],
                limit=1,
            )
            
            if not products:
                return None
            
            product_data = products[0]
            
            self.stats['products_found'] += 1
            
            price = safe_float(
                product_data.get('list_price', 0.0),
                default=0.0,
                allow_negative=False,
            )
            
            return (product_data['id'], price)
        
        except Exception as e:
            logger.error(f"Failed to find product {name}: {e}")
            raise ProductError(f"Product lookup failed: {e}") from e
    
    # ═══════════════════════════════════════════════════════════════════════════
    # QUOTATION VALIDATION
    # ═══════════════════════════════════════════════════════════════════════════
    
    def validate_quotation_params(
        self,
        quantity: float,
        discount: float = 0.0,
    ) -> Tuple[float, float]:
        """
        Validate and normalize quotation parameters.
        
        Args:
            quantity: Order quantity
            discount: Discount percentage
        
        Returns:
            Tuple of (quantity, discount)
        
        Raises:
            QuotationError: If validation fails
        """
        if quantity <= 0:
            raise QuotationError(f"Quantity must be > 0, got {quantity}")
        
        if discount < 0 or discount > 100:
            raise QuotationError(f"Discount must be 0-100%, got {discount}")
        
        return quantity, discount
    
    # ═══════════════════════════════════════════════════════════════════════════
    # QUOTATION CREATION
    # ═══════════════════════════════════════════════════════════════════════════
    
    def create_quotation(
        self,
        customer_name: str,
        lines: List[Tuple[str, float, float]],  # (product_name, qty, discount)
    ) -> int:
        """
        Create sales quotation (draft order).
        
        Args:
            customer_name: Customer name
            lines: List of (product_name, quantity, discount) tuples
        
        Returns:
            Quotation ID
        
        Raises:
            CustomerError: If customer not found
            ProductError: If product not found
            QuotationError: If validation fails
        """
        logger.info(f"Creating quotation for customer '{customer_name}'")
        
        try:
            # Find customer
            customer = self.find_customer(customer_name)
            if not customer:
                raise CustomerError(f"Customer not found: {customer_name}")
            
            # Build order lines
            order_lines = []
            
            for product_name, quantity, discount in lines:
                try:
                    # Find product
                    product_info = self.find_product(product_name)
                    if not product_info:
                        raise ProductError(f"Product not found: {product_name}")
                    
                    product_id, price = product_info
                    
                    # Validate
                    quantity, discount = self.validate_quotation_params(
                        quantity, discount
                    )
                    
                    # Add line
                    order_lines.append((
                        0, 0, {
                            'product_id': product_id,
                            'product_uom_qty': quantity,
                            'price_unit': price,
                            'discount': discount,
                        }
                    ))
                
                except (ProductError, QuotationError) as e:
                    logger.error(f"Failed to add line {product_name}: {e}")
                    raise
            
            if not order_lines:
                raise QuotationError("No valid order lines")
            
            # Create quotation
            quotation_vals = {
                'partner_id': customer.id,
                'state': 'draft',
                'order_line': order_lines,
            }
            
            quotation_id = self.client.create('sale.order', quotation_vals)
            
            if isinstance(quotation_id, (list, tuple)):
                if not quotation_id:
                    raise QuotationError("create() returned empty result")
                quotation_id = quotation_id[0]
            
            quotation_id = int(quotation_id)
            self.stats['quotations_created'] += 1
            
            logger.info(
                f"Created quotation {quotation_id} for {customer.name} "
                f"({len(order_lines)} lines)"
            )
            
            # Audit
            self._audit_log({
                'operation': 'quotation_created',
                'quotation_id': quotation_id,
                'customer_id': customer.id,
                'customer_name': customer.name,
                'lines_count': len(order_lines),
            })
            
            return quotation_id
        
        except Exception as e:
            logger.error(f"Quotation creation failed: {e}", exc_info=True)
            self.stats['errors'] += 1
            raise
    
    # ═══════════════════════════════════════════════════════════════════════════
    # ORDER CONFIRMATION
    # ═══════════════════════════════════════════════════════════════════════════
    
    def confirm_quotation(self, quotation_id: int) -> bool:
        """
        Confirm quotation (convert to order).
        
        Args:
            quotation_id: Quotation ID
        
        Returns:
            True if successful
        """
        try:
            self.client.execute(
                'sale.order',
                'action_confirm',
                [quotation_id],
            )
            
            self.stats['orders_confirmed'] += 1
            
            logger.info(f"Confirmed quotation {quotation_id}")
            
            # Audit
            self._audit_log({
                'operation': 'order_confirmed',
                'order_id': quotation_id,
            })
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to confirm quotation {quotation_id}: {e}")
            self.stats['errors'] += 1
            return False
    
    # ═══════════════════════════════════════════════════════════════════════════
    # SCENARIOS
    # ═══════════════════════════════════════════════════════════════════════════
    
    def run_scenario(self, scenario: SalesScenario) -> Optional[int]:
        """
        Run a sales scenario.
        
        Args:
            scenario: SalesScenario definition
        
        Returns:
            Order ID or None if failed
        """
        logger.info(f"Running scenario: {scenario.name}")
        
        try:
            # Create quotation
            quotation_id = self.create_quotation(
                scenario.customer_name,
                scenario.lines,
            )
            
            # Confirm
            if not self.confirm_quotation(quotation_id):
                logger.warning(f"Failed to confirm quotation {quotation_id}")
                return None
            
            self.stats['scenarios_completed'] += 1
            
            log_success(f"Scenario '{scenario.name}' completed (Order {quotation_id})")
            
            return quotation_id
        
        except Exception as e:
            logger.error(f"Scenario '{scenario.name}' failed: {e}")
            self.stats['errors'] += 1
            return None
    
    # ═══════════════════════════════════════════════════════════════════════════
    # DEMO
    # ═══════════════════════════════════════════════════════════════════════════
    
    def run_demo_scenarios(
        self,
        scenarios: Optional[List[SalesScenario]] = None,
    ) -> List[int]:
        """
        Run demo sales scenarios.
        
        Args:
            scenarios: List of SalesScenario definitions
        
        Returns:
            List of order IDs
        """
        logger.info("Running demo sales scenarios")
        
        if not scenarios:
            # Default scenarios
            scenarios = [
                SalesScenario(
                    name="Standard Order",
                    customer_name="Demo Kunde GmbH",
                    lines=[("EVO2 Spartan Drohne", 1.0, 0.0)],
                ),
                SalesScenario(
                    name="Order with Discount",
                    customer_name="NextLap AG",
                    lines=[("EVO2 Lightweight Drohne", 2.0, 10.0)],
                ),
                SalesScenario(
                    name="Bulk Order",
                    customer_name="Demo Kunde GmbH",
                    lines=[("EVO2 Balance Drohne", 5.0, 0.0)],
                ),
                SalesScenario(
                    name="Multi-Product Order",
                    customer_name="NextLap AG",
                    lines=[
                        ("EVO2 Spartan Drohne", 2.0, 5.0),
                        ("EVO2 Lightweight Drohne", 3.0, 5.0),
                    ],
                ),
            ]
        
        order_ids = []
        
        for scenario in scenarios:
            order_id = self.run_scenario(scenario)
            if order_id:
                order_ids.append(order_id)
        
        logger.info(f"Completed {len(order_ids)}/{len(scenarios)} scenarios")
        
        return order_ids
    
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
        log_header("SALES FLOW")
        
        try:
            order_ids = self.run_demo_scenarios()
            
            if order_ids:
                log_success(f"Completed {len(order_ids)} sales orders")
            else:
                log_warn("No sales orders completed (check customers/products)")
            
            # Summary
            log_info("Sales Flow Statistics:")
            for key, value in self.stats.items():
                log_info(f"  {key}: {value}")
            
            return self.stats
        
        except Exception as e:
            log_error(f"Sales flow failed: {e}", exc_info=True)
            raise


# ═══════════════════════════════════════════════════════════════════════════════
# SETUP FUNCTION FOR RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

def setup_sales_flows(client: OdooClient) -> Dict[str, int]:
    """
    Initialize and run sales flows.
    
    Args:
        client: OdooClient
    
    Returns:
        Statistics dict
    """
    flow = SalesFlow(client)
    return flow.run()

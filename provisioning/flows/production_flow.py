"""
production_flow.py - Production Flow Simulation

Simulates:
- Manufacturing order (MO) creation and progression
- Routing operations with time tracking
- Work center operations
- Quality control events
- UMH event generation (MES integration)
- Event export to JSON

Production-ready with proper error handling, statistics, and audit trail.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional, List, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

from provisioning.client import OdooClient
from provisioning.core.validation import safe_float, FormatValidator, ValidationError
from provisioning.utils import log_header, log_info, log_success, log_warn, log_error, timed_operation
from provisioning.config import (
    VARIANT_NAMES,
    UMH_EVENTS_PRODUCTION_FILE,
    QUALITY_CHECK_OPERATION_SEQ,
)

from production_routing import get_routing, VariantName, RoutingOperation
from provisioning.integration.umh_events import UMHEventManager, EventType
from integration.umh_client_sim import UMHClientSimulator


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class OperationExecution:
    """Represents execution of a single routing operation."""
    operation_id: int
    sequence: int
    name: str
    workcenter_code: str
    setup_time_min: float
    run_time_min: float
    quantity: float
    
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: str = "pending"  # pending, in_progress, completed, failed
    
    @property
    def total_time(self) -> float:
        """Total time for this operation."""
        return self.setup_time_min + (self.run_time_min * self.quantity)
    
    @property
    def duration_minutes(self) -> Optional[float]:
        """Actual duration if completed."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds() / 60
        return None


@dataclass
class ManufacturingOrder:
    """Manufacturing order for a variant."""
    variant: str
    quantity: float
    mo_id: Optional[int] = None
    product_id: Optional[int] = None
    
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: str = "pending"  # pending, in_progress, completed, failed
    
    operations: List[OperationExecution] = None
    
    def __post_init__(self):
        if self.operations is None:
            self.operations = []
    
    @property
    def total_duration_minutes(self) -> Optional[float]:
        """Total duration if completed."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds() / 60
        return None
    
    @property
    def operation_time_minutes(self) -> float:
        """Sum of all operation times."""
        return sum(op.total_time for op in self.operations)


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class ProductionError(Exception):
    """Base production flow error."""
    pass


class RoutingError(ProductionError):
    """Routing-related error."""
    pass


class WorkcenterError(ProductionError):
    """Workcenter validation error."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# PRODUCTION FLOW
# ═══════════════════════════════════════════════════════════════════════════════

class ProductionFlow:
    """Simulate manufacturing orders and production processes."""
    
    def __init__(self, client: OdooClient):
        """Initialize production flow."""
        self.client = client
        
        self.umh_manager = UMHEventManager()
        self.umh_client = UMHClientSimulator(
            output_file=UMH_EVENTS_PRODUCTION_FILE
        )
        
        self.stats = {
            'mo_created': 0,
            'operations_executed': 0,
            'quality_checks': 0,
            'mo_completed': 0,
            'events_generated': 0,
            'events_exported': 0,
            'errors': 0,
        }
        
        self.audit_log: List[Dict[str, Any]] = []
        
        logger.info("ProductionFlow initialized")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # ODOO MO MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _get_or_create_mo(
        self,
        variant: str,
        quantity: float,
    ) -> int:
        """
        Get or create manufacturing order in Odoo.
        
        Args:
            variant: Variant name (spartan, lightweight, balance)
            quantity: Order quantity
        
        Returns:
            MO ID
        
        Raises:
            ProductionError: If creation fails
        """
        try:
            # Search for existing template
            templates = self.client.search_read(
                'mrp.production',
                [('name', '=', f'MO-{variant}-{datetime.now().date()}')],
                ['id'],
                limit=1,
            )
            
            if templates:
                return templates[0]['id']
            
            # Create new MO
            mo_id = self.client.create(
                'mrp.production',
                {
                    'name': f'MO-{variant}-{datetime.now().isoformat()}',
                    'product_qty': quantity,
                    'state': 'draft',
                }
            )
            
            if isinstance(mo_id, (list, tuple)):
                if not mo_id:
                    raise ProductionError("create() returned empty result")
                mo_id = mo_id[0]
            
            mo_id = int(mo_id)
            self.stats['mo_created'] += 1
            
            logger.info(f"Created MO {mo_id} for variant {variant}")
            
            return mo_id
        
        except Exception as e:
            logger.error(f"Failed to create MO: {e}")
            self.stats['errors'] += 1
            raise ProductionError(f"Failed to create MO: {e}") from e
    
    # ═══════════════════════════════════════════════════════════════════════════
    # ROUTING & OPERATION VALIDATION
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _validate_routing(self, variant: str, ops: List[RoutingOperation]) -> None:
        """
        Validate routing operations.
        
        Args:
            variant: Variant name
            ops: Operations list
        
        Raises:
            RoutingError: If validation fails
        """
        if not ops:
            raise RoutingError(f"No operations found for variant {variant}")
        
        for op in ops:
            if not op.setup_time_min or not op.run_time_min:
                raise RoutingError(
                    f"Operation {op.seq} missing timing: "
                    f"setup={op.setup_time_min}, run={op.run_time_min}"
                )
            
            if op.setup_time_min < 0 or op.run_time_min < 0:
                raise RoutingError(
                    f"Operation {op.seq} has negative timing"
                )
            
            if not op.workcenter_code:
                raise RoutingError(
                    f"Operation {op.seq} missing workcenter_code"
                )
        
        logger.debug(f"Validated {len(ops)} operations for {variant}")
    
    def _validate_workcenter(self, workcenter_code: str) -> int:
        """
        Validate workcenter exists in Odoo.
        
        Args:
            workcenter_code: Workcenter code
        
        Returns:
            Workcenter ID
        
        Raises:
            WorkcenterError: If not found
        """
        workcenters = self.client.search_read(
            'mrp.workcenter',
            [('code', '=', workcenter_code)],
            ['id'],
            limit=1,
        )
        
        if not workcenters:
            raise WorkcenterError(f"Workcenter not found: {workcenter_code}")
        
        return workcenters[0]['id']
    
    # ═══════════════════════════════════════════════════════════════════════════
    # OPERATION EXECUTION
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _execute_operation(
        self,
        op: RoutingOperation,
        quantity: float,
        current_time: datetime,
    ) -> OperationExecution:
        """
        Execute a routing operation (simulation).
        
        Args:
            op: Routing operation
            quantity: Order quantity
            current_time: Current simulation time
        
        Returns:
            OperationExecution record
        """
        try:
            # Validate workcenter
            self._validate_workcenter(op.workcenter_code)
            
            # Create execution record
            exec_record = OperationExecution(
                operation_id=op.id,
                sequence=op.seq,
                name=op.name,
                workcenter_code=op.workcenter_code,
                setup_time_min=op.setup_time_min,
                run_time_min=op.run_time_min,
                quantity=quantity,
                start_time=current_time,
                status='in_progress',
            )
            
            # Calculate end time
            duration = timedelta(minutes=exec_record.total_time)
            exec_record.end_time = current_time + duration
            exec_record.status = 'completed'
            
            self.stats['operations_executed'] += 1
            
            logger.debug(
                f"Executed operation {op.seq} ({op.name}) "
                f"on {op.workcenter_code}: "
                f"{exec_record.total_time:.1f} min"
            )
            
            return exec_record
        
        except Exception as e:
            logger.error(f"Failed to execute operation {op.seq}: {e}")
            self.stats['errors'] += 1
            raise ProductionError(f"Operation execution failed: {e}") from e
    
    # ═══════════════════════════════════════════════════════════════════════════
    # EVENT GENERATION
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _generate_events_for_mo(
        self,
        mo: ManufacturingOrder,
    ) -> None:
        """
        Generate UMH events for manufacturing order.
        
        Args:
            mo: ManufacturingOrder record
        """
        mo_id = mo.mo_id or -1
        
        # MO Started
        mo_start = self.umh_manager.create_mo_event(
            mo_id=mo_id,
            event_type=EventType.MO_STARTED,
            timestamp=mo.start_time,
        )
        self.umh_manager.queue_event(mo_start)
        self.stats['events_generated'] += 1
        
        # Operation events
        for exec_op in mo.operations:
            op_event = self.umh_manager.create_operation_event(
                mo_id=mo_id,
                operation_seq=exec_op.sequence,
                operation_name=exec_op.name,
                workcenter=exec_op.workcenter_code,
                duration_minutes=exec_op.total_time,
                timestamp=exec_op.start_time,
            )
            self.umh_manager.queue_event(op_event)
            self.stats['events_generated'] += 1
            
            # Quality check event if applicable
            if exec_op.sequence == QUALITY_CHECK_OPERATION_SEQ:
                quality_event = self.umh_manager.create_quality_event(
                    product_id=mo.product_id or -1,
                    stage=exec_op.name,
                    result='pass',  # TODO: Make configurable
                    details=(
                        f"Variant={mo.variant}, "
                        f"Workcenter={exec_op.workcenter_code}, "
                        f"Seq={exec_op.sequence}"
                    ),
                    timestamp=exec_op.end_time,
                )
                self.umh_manager.queue_event(quality_event)
                self.stats['quality_checks'] += 1
                self.stats['events_generated'] += 1
        
        # MO Completed
        mo_done = self.umh_manager.create_mo_event(
            mo_id=mo_id,
            event_type=EventType.MO_COMPLETED,
            timestamp=mo.end_time,
        )
        self.umh_manager.queue_event(mo_done)
        self.stats['events_generated'] += 1
        
        logger.info(f"Generated {mo.stats['events_generated']} events for MO {mo_id}")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # EVENT EXPORT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _export_events(self) -> bool:
        """
        Export pending events to file.
        
        Returns:
            True if successful
        """
        try:
            events_dicts = [
                e.to_dict() for e in self.umh_manager.get_pending_events()
            ]
            
            if not events_dicts:
                logger.warning("No events to export")
                return True
            
            self.umh_client.send_events_batch(events_dicts)
            
            if self.umh_client.export_to_file():
                self.stats['events_exported'] += len(events_dicts)
                logger.info(
                    f"Exported {len(events_dicts)} events to "
                    f"{UMH_EVENTS_PRODUCTION_FILE}"
                )
                return True
            else:
                logger.error("export_to_file() failed")
                self.stats['errors'] += 1
                return False
        
        except Exception as e:
            logger.error(f"Failed to export events: {e}", exc_info=True)
            self.stats['errors'] += 1
            return False
    
    # ═══════════════════════════════════════════════════════════════════════════
    # MAIN
    # ═══════════════════════════════════════════════════════════════════════════
    
    def run_production_for_variant(
        self,
        variant: str,
        quantity: float = 1.0,
    ) -> bool:
        """
        Simulate production run for a variant.
        
        Args:
            variant: Variant name
            quantity: Order quantity
        
        Returns:
            True if successful
        """
        logger.info(
            f"Starting production simulation for variant '{variant}' "
            f"(quantity {quantity})"
        )
        
        try:
            # Validate variant
            if variant not in VARIANT_NAMES:
                raise ValueError(f"Invalid variant: {variant}")
            
            # Get routing
            routing_ops = get_routing(variant)
            self._validate_routing(variant, routing_ops)
            
            # Create MO
            mo_id = self._get_or_create_mo(variant, quantity)
            
            # Create MO record
            mo = ManufacturingOrder(
                variant=variant,
                quantity=quantity,
                mo_id=mo_id,
                start_time=datetime.now(),
                status='in_progress',
            )
            
            # Execute operations
            current_time = mo.start_time
            for routing_op in routing_ops:
                try:
                    exec_op = self._execute_operation(
                        routing_op, quantity, current_time
                    )
                    mo.operations.append(exec_op)
                    current_time = exec_op.end_time
                
                except ProductionError as e:
                    logger.warning(f"Operation failed, continuing: {e}")
                    self.stats['errors'] += 1
            
            # Mark MO completed
            mo.end_time = current_time
            mo.status = 'completed'
            self.stats['mo_completed'] += 1
            
            # Generate events
            self._generate_events_for_mo(mo)
            
            # Export events
            success = self._export_events()
            self.umh_manager.clear_events()
            
            # Summary
            log_success(
                f"Variant '{variant}': "
                f"Operations={len(mo.operations)}, "
                f"Total time={mo.operation_time_minutes:.1f} min, "
                f"Events={len(self.umh_manager.get_pending_events())}"
            )
            
            # Audit
            self._audit_log({
                'operation': 'production_variant',
                'variant': variant,
                'mo_id': mo_id,
                'quantity': quantity,
                'operations_count': len(mo.operations),
                'total_time_minutes': mo.operation_time_minutes,
                'status': 'completed' if success else 'partial',
            })
            
            return success
        
        except Exception as e:
            logger.error(
                f"Production failed for variant {variant}: {e}",
                exc_info=True,
            )
            self.stats['errors'] += 1
            return False
    
    def run_all_variants(
        self,
        quantity: float = 1.0,
    ) -> Dict[str, int]:
        """
        Run production for all variants.
        
        Args:
            quantity: Order quantity per variant
        
        Returns:
            Statistics dict
        """
        log_header("PRODUCTION FLOW - ALL VARIANTS")
        
        try:
            for variant in VARIANT_NAMES:
                success = self.run_production_for_variant(variant, quantity)
                if not success:
                    log_warn(f"Production failed for {variant}")
            
            # Summary
            log_info("Production Flow Statistics:")
            for key, value in self.stats.items():
                log_info(f"  {key}: {value}")
            
            return self.stats
        
        except Exception as e:
            log_error(f"Production flow failed: {e}", exc_info=True)
            raise
    
    # ═══════════════════════════════════════════════════════════════════════════
    # AUDIT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _audit_log(self, data: Dict[str, Any]) -> None:
        """Add audit entry."""
        data['timestamp'] = datetime.now().isoformat()
        self.audit_log.append(data)
        logger.debug(f"Audit: {data}")


# ═══════════════════════════════════════════════════════════════════════════════
# SETUP FUNCTION FOR RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

def setup_production_flows(client: OdooClient) -> Dict[str, int]:
    """
    Initialize and run production flows.
    
    Args:
        client: OdooClient
    
    Returns:
        Statistics dict
    """
    flow = ProductionFlow(client)
    return flow.run_all_variants(quantity=1.0)

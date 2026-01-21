"""
production_routing.py - Manufacturing Routing Definitions

Defines:
- Manufacturing operations (Arbeitsplan) per variant
- Setup & run times, workcenter assignments
- Support for variant-specific time overrides
- Loading from Odoo mrp.routing or static definitions
- Validation and caching

Used by:
- production_flow.py (simulation)
- umh_masterdata.py (export)
- Demos and documentation
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Literal
from datetime import datetime


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# TYPES & CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

VariantName = Literal["spartan", "balance", "lightweight"]

# Valid workcenter codes
VALID_WORKCENTERS = {
    "WC-3D": "3D Printer",
    "WC-LC": "Laser Cutter",
    "WC-NACH": "Post-Processing",
    "WC-WTB": "WT Assembly",
    "WC-LOET": "Soldering",
    "WC-MONT1": "Electronic Assembly",
    "WC-MONT2": "Mechanical Assembly",
    "WC-FLASH": "Controller Programming",
    "WC-QM-END": "Final QC",
}

VARIANT_NAMES = ["spartan", "lightweight", "balance"]


# ═══════════════════════════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class OperationDef:
    """Manufacturing operation definition."""
    
    seq: int
    name: str
    workcenter_code: str
    setup_time_min: float
    run_time_min: float
    qty_per_cycle: float = 1.0
    
    # Optional Odoo IDs
    operation_id: Optional[int] = None
    workcenter_id: Optional[int] = None
    
    def __post_init__(self):
        """Validate operation definition."""
        if self.seq <= 0:
            raise ValueError(f"seq must be > 0, got {self.seq}")
        
        if self.setup_time_min < 0:
            raise ValueError(f"setup_time_min cannot be negative: {self.setup_time_min}")
        
        if self.run_time_min < 0:
            raise ValueError(f"run_time_min cannot be negative: {self.run_time_min}")
        
        if self.qty_per_cycle <= 0:
            raise ValueError(f"qty_per_cycle must be > 0, got {self.qty_per_cycle}")
        
        if self.workcenter_code not in VALID_WORKCENTERS:
            raise ValueError(
                f"Invalid workcenter_code: {self.workcenter_code}. "
                f"Valid: {list(VALID_WORKCENTERS.keys())}"
            )
    
    @property
    def total_time_single_unit(self) -> float:
        """Total time for one unit (setup + run_time)."""
        return self.setup_time_min + self.run_time_min
    
    @property
    def total_time_cycle(self) -> float:
        """Total time for one cycle (setup + run_time * qty_per_cycle)."""
        return self.setup_time_min + (self.run_time_min * self.qty_per_cycle)
    
    def get_time_for_quantity(self, quantity: float) -> float:
        """
        Calculate time for specific quantity.
        
        Args:
            quantity: Order quantity
        
        Returns:
            Total time in minutes (setup + run_time * quantity)
        """
        cycles = quantity / self.qty_per_cycle
        # Setup only once per operation
        return self.setup_time_min + (self.run_time_min * quantity)


# ═══════════════════════════════════════════════════════════════════════════════
# BASE ROUTING (DRY - avoid duplication)
# ═══════════════════════════════════════════════════════════════════════════════

_BASE_ROUTING = [
    OperationDef(10, "3D-Druck Füße", "WC-3D", 10, 60, 4),
    OperationDef(20, "3D-Druck Haube", "WC-3D", 10, 240, 1),
    OperationDef(30, "Laserschneiden Grundplatte", "WC-LC", 10, 10, 1),
    OperationDef(40, "Nacharbeit Grundplatte", "WC-NACH", 5, 15, 1),
    OperationDef(50, "Nacharbeit Füße", "WC-NACH", 5, 10, 4),
    OperationDef(60, "Nacharbeit Haube", "WC-NACH", 5, 10, 1),
    OperationDef(70, "WT bestücken", "WC-WTB", 5, 8, 1),
    OperationDef(80, "Löten Elektronik", "WC-LOET", 5, 15, 1),
    OperationDef(90, "Montage Elektronik", "WC-MONT1", 5, 10, 1),
    OperationDef(100, "Flashen Flugcontroller", "WC-FLASH", 5, 8, 1),
    OperationDef(110, "Montage Gehäuse & Rotoren", "WC-MONT2", 5, 12, 1),
    OperationDef(120, "End-Qualitätskontrolle", "WC-QM-END", 2, 5, 1),
]


# ═══════════════════════════════════════════════════════════════════════════════
# VARIANT-SPECIFIC TIME OVERRIDES (Optional)
# ═══════════════════════════════════════════════════════════════════════════════

_VARIANT_OVERRIDES: Dict[VariantName, Dict[int, Dict[str, float]]] = {
    "lightweight": {
        # Lightweight prints faster (less material)
        10: {"run_time_min": 45},  # 3D-Druck Füße: 60 → 45 min
        20: {"run_time_min": 180},  # 3D-Druck Haube: 240 → 180 min
        # All other operations use base times
    },
    "balance": {
        # Balance has middle-of-road times (same as base)
    },
    "spartan": {
        # Spartan uses base times
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTING CACHE
# ═══════════════════════════════════════════════════════════════════════════════

_ROUTING_CACHE: Dict[VariantName, List[OperationDef]] = {}


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTING FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def _build_routing_for_variant(variant: VariantName) -> List[OperationDef]:
    """
    Build routing for variant by applying variant-specific overrides.
    
    Args:
        variant: Variant name
    
    Returns:
        List of OperationDef sorted by sequence
    
    Raises:
        ValueError: If variant is invalid
    """
    if variant not in VARIANT_NAMES:
        raise ValueError(
            f"Invalid variant: {variant}. Valid: {VARIANT_NAMES}"
        )
    
    # Start with base routing
    routing = []
    for base_op in _BASE_ROUTING:
        # Create copy
        op_dict = {
            'seq': base_op.seq,
            'name': base_op.name,
            'workcenter_code': base_op.workcenter_code,
            'setup_time_min': base_op.setup_time_min,
            'run_time_min': base_op.run_time_min,
            'qty_per_cycle': base_op.qty_per_cycle,
        }
        
        # Apply variant-specific overrides
        if variant in _VARIANT_OVERRIDES:
            overrides = _VARIANT_OVERRIDES[variant]
            if base_op.seq in overrides:
                op_dict.update(overrides[base_op.seq])
        
        routing.append(OperationDef(**op_dict))
    
    # Sort by sequence
    routing.sort(key=lambda op: op.seq)
    
    logger.debug(f"Built routing for variant {variant}: {len(routing)} operations")
    
    return routing


def get_routing(
    variant: VariantName,
    use_cache: bool = True,
) -> List[OperationDef]:
    """
    Get routing operations for variant.
    
    Args:
        variant: Variant name (spartan, lightweight, balance)
        use_cache: Use cached routing if available
    
    Returns:
        List of OperationDef sorted by sequence
    
    Raises:
        ValueError: If variant invalid
    """
    # Check cache
    if use_cache and variant in _ROUTING_CACHE:
        logger.debug(f"Using cached routing for {variant}")
        return _ROUTING_CACHE[variant]
    
    # Build routing
    routing = _build_routing_for_variant(variant)
    
    # Cache
    _ROUTING_CACHE[variant] = routing
    
    return routing


def get_all_routings() -> Dict[VariantName, List[OperationDef]]:
    """
    Get all variant routings.
    
    Returns:
        Dict mapping variant names to operation lists
    """
    return {
        variant: get_routing(variant)
        for variant in VARIANT_NAMES
    }


def validate_routing(variant: VariantName) -> bool:
    """
    Validate routing for variant.
    
    Args:
        variant: Variant name
    
    Returns:
        True if valid
    
    Raises:
        ValueError: If routing invalid
    """
    try:
        routing = get_routing(variant, use_cache=False)
        
        # Check sequences are unique and in order
        seqs = [op.seq for op in routing]
        if len(seqs) != len(set(seqs)):
            raise ValueError(f"Duplicate sequences in routing: {seqs}")
        
        if seqs != sorted(seqs):
            raise ValueError(f"Sequences not in order: {seqs}")
        
        # Check all operations are valid
        for op in routing:
            if op.setup_time_min < 0 or op.run_time_min < 0:
                raise ValueError(
                    f"Operation {op.seq} has negative times: "
                    f"setup={op.setup_time_min}, run={op.run_time_min}"
                )
        
        logger.info(f"Validated routing for {variant}: {len(routing)} operations")
        
        return True
    
    except Exception as e:
        logger.error(f"Routing validation failed for {variant}: {e}")
        raise


def validate_all_routings() -> bool:
    """Validate all variant routings."""
    for variant in VARIANT_NAMES:
        validate_routing(variant)
    
    logger.info(f"Validated all {len(VARIANT_NAMES)} variant routings")
    return True


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTING STATISTICS
# ═══════════════════════════════════════════════════════════════════════════════

def get_routing_stats(
    variant: VariantName,
    quantity: float = 1.0,
) -> Dict[str, float]:
    """
    Get routing statistics for variant.
    
    Args:
        variant: Variant name
        quantity: Order quantity
    
    Returns:
        Dict with statistics (total_time, setup_time, run_time, operation_count)
    """
    routing = get_routing(variant)
    
    total_setup = sum(op.setup_time_min for op in routing)
    total_run = sum(op.get_time_for_quantity(quantity) - op.setup_time_min 
                    for op in routing)
    
    return {
        'variant': variant,
        'quantity': quantity,
        'operation_count': len(routing),
        'setup_time_min': total_setup,
        'run_time_min': total_run,
        'total_time_min': total_setup + total_run,
    }


def get_all_routing_stats(quantity: float = 1.0) -> Dict[VariantName, Dict]:
    """Get routing statistics for all variants."""
    return {
        variant: get_routing_stats(variant, quantity)
        for variant in VARIANT_NAMES
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ODOO INTEGRATION (Optional)
# ═══════════════════════════════════════════════════════════════════════════════

def load_routings_from_odoo(client) -> Dict[VariantName, List[OperationDef]]:
    """
    Load routing definitions from Odoo mrp.routing.
    
    Args:
        client: OdooClient
    
    Returns:
        Dict mapping variants to operation lists
    
    Note:
        Currently not fully integrated. Would require:
        - mrp.routing model in Odoo
        - Variant-to-routing mapping
        - Workcenter-to-code mapping
    """
    # TODO: Implement full Odoo integration
    logger.warning("load_routings_from_odoo() not yet implemented")
    
    # For now, use static definitions
    return get_all_routings()


def sync_routings_to_odoo(client) -> bool:
    """
    Sync routing definitions to Odoo.
    
    Args:
        client: OdooClient
    
    Returns:
        True if successful
    
    Note:
        Would create/update mrp.routing records in Odoo
    """
    # TODO: Implement sync logic
    logger.warning("sync_routings_to_odoo() not yet implemented")
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# INITIALIZATION
# ═══════════════════════════════════════════════════════════════════════════════

# Validate on import
try:
    validate_all_routings()
    logger.info("Production routing definitions validated on import")
except Exception as e:
    logger.error(f"Failed to validate routing definitions: {e}")
    raise

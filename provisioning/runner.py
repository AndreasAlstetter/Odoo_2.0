
"""
runner.py - Orchestrierung für Odoo ERP Provisioning

Lädt alle Stammdaten, Fertigungs-Routing, Qualitätsprotokolle, etc.
Optimiert für >500 Drohnen/Tag.

Usage:
    python -m provisioning.runner
    python -m provisioning.runner --kpi-only
    python -m provisioning.runner --full

FIXED VERSION: All imports use absolute 'provisioning.' prefix + run() compatibility + DataPaths.DATA_DIR fix
"""

import os
import sys
import logging
import argparse
from typing import Optional, Dict, List, Tuple
from datetime import datetime
from dataclasses import dataclass, field


# ✅ FIXED: Use absolute imports with 'provisioning.' prefix
from provisioning.config import (
    OdooRPCConfig,
    get_odoo_config,
    DataPaths,
    LoggingConfig,
)

from provisioning.client import (
    OdooClient,
    OdooClientError,
    AuthenticationError,
)

from provisioning.utils import (
    log_header,
    log_info,
    log_success,
    log_warn,
    log_error,
    set_progress_hook,
)

# Import loaders
from provisioning.loaders.products_loader import ProductsLoader
from provisioning.loaders.suppliers_loader import SuppliersLoader
from provisioning.loaders.supplierinfo_loader import SupplierInfoLoader
from provisioning.loaders.bom_loader import BomLoader
from provisioning.loaders.routing_loader import RoutingLoader
from provisioning.loaders.quality_loader import QualityLoader
from provisioning.loaders.manufacturing_config_loader import ManufacturingConfigLoader
from provisioning.loaders.mailserver_loader import MailServerLoader
from provisioning.loaders.stock_structure_loader import StockStructureLoader

# Import flows
from provisioning.flows.kpi_extractor import KPIExtractor


# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

logger = logging.getLogger(__name__)


def _setup_logging(level: str = LoggingConfig.LEVEL) -> None:
    """Configure logging for runner."""
    formatter = logging.Formatter(
        LoggingConfig.FORMAT,
        datefmt=LoggingConfig.DATE_FORMAT
    )

    # Console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(console_handler)

    # File logging
    if LoggingConfig.AUDIT_ENABLED:
        os.makedirs(LoggingConfig.LOG_DIR, exist_ok=True)
        file_handler = logging.FileHandler(LoggingConfig.LOG_FILE)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)


# ═══════════════════════════════════════════════════════════════════════════════
# DEFAULT DATA PATH (FIX FÜR DataPaths.DATA_DIR)
# ═══════════════════════════════════════════════════════════════════════════════

def _get_default_data_dir() -> str:
    """Get default data directory (relative to project root)."""
    # Assuming: odoo-provisioning/provisioning/runner.py
    # Project root is: odoo-provisioning/
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    default_path = os.path.join(project_root, 'data')
    return os.path.abspath(default_path)


# ═══════════════════════════════════════════════════════════════════════════════
# PROVISIONING STEP DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class ProvisioningStep:
    """Definition einer Provisioning-Phase."""

    name: str
    description: str
    loader_class: type
    loader_kwargs: Dict = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)
    weight: int = 100  # Progress weight (relativ)


# Schritt-Definitionen (mit korrektem Weight)
PROVISIONING_STEPS: List[ProvisioningStep] = [
    ProvisioningStep(
        name='products',
        description='📦 Produkte aus Lagerdaten laden',
        loader_class=ProductsLoader,
        weight=100,  # ~20% bei 500 Unit Total
    ),
    ProvisioningStep(
        name='suppliers',
        description='👥 Lieferanten aus CSV laden',
        loader_class=SuppliersLoader,
        depends_on=['products'],
        weight=80,
    ),
    ProvisioningStep(
        name='supplierinfo',
        description='🔗 Supplierinfos laden',
        loader_class=SupplierInfoLoader,
        depends_on=['products', 'suppliers'],
        weight=80,
    ),
    ProvisioningStep(
        name='mailserver',
        description='📧 Mail-Server-Konfigurationen laden',
        loader_class=MailServerLoader,
        weight=40,
    ),
    ProvisioningStep(
        name='stock_structure',
        description='🏭 Lagerorte, Routen & Kanban laden',
        loader_class=StockStructureLoader,
        depends_on=['products'],
        weight=120,
    ),
    ProvisioningStep(
        name='bom',
        description='🔩 BoMs aus CSV laden',
        loader_class=BomLoader,
        depends_on=['products'],
        weight=120,
        loader_kwargs={'filename': 'bom.csv'},
    ),
    ProvisioningStep(
        name='routing',
        description='🔄 Routing-Operationen laden',
        loader_class=RoutingLoader,
        depends_on=['products', 'bom'],
        weight=100,
    ),
    ProvisioningStep(
        name='manufacturing_config',
        description='⚙️ Manufacturing Sequences (MO-Reference Fix)',
        loader_class=ManufacturingConfigLoader,
        depends_on=['routing'],
        weight=100,
    ),
    ProvisioningStep(
        name='quality',
        description='✅ Quality Points laden',
        loader_class=QualityLoader,
        depends_on=['routing', 'products'],
        weight=100,
    ),
]

# Calculate total weight
TOTAL_WEIGHT = sum(step.weight for step in PROVISIONING_STEPS)


# ═══════════════════════════════════════════════════════════════════════════════
# CLIENT INITIALIZATION
# ═══════════════════════════════════════════════════════════════════════════════

def _build_client_from_env() -> OdooClient:
    """
    Initialize Odoo RPC Client mit Fehlerbehandlung.

    Raises:
        OdooClientError: Wenn Connection/Auth fehlschlägt
    """
    try:
        log_info("[CLIENT] Loading configuration from .env...")
        config = get_odoo_config()

        log_info(
            f"[CLIENT] Connecting to Odoo at {config.url} "
            f"(DB: {config.db}, timeout: {config.timeout}s)"
        )

        client = OdooClient(config)

        # Test authentication
        uid = client.uid
        log_success(f"[CLIENT] Successfully authenticated as UID {uid}")

        return client

    except AuthenticationError as e:
        log_error(f"[CLIENT] Authentication failed: {e}")
        raise
    except OdooClientError as e:
        log_error(f"[CLIENT] Connection failed: {e}")
        raise
    except Exception as e:
        log_error(f"[CLIENT] Unexpected error: {e}")
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# DEPENDENCY RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════════

def _validate_dependencies(steps: List[ProvisioningStep]) -> None:
    """
    Validiere dass alle Dependencies existieren.

    Raises:
        ValueError: Wenn Dependency nicht gefunden oder zirkulär
    """
    step_names = {step.name for step in steps}

    for step in steps:
        for dep in step.depends_on:
            if dep not in step_names:
                raise ValueError(
                    f"Step '{step.name}' depends on '{dep}' which doesn't exist"
                )

    # Simple cycle detection (DFS)
    visited = set()
    rec_stack = set()

    def _has_cycle(name: str) -> bool:
        visited.add(name)
        rec_stack.add(name)

        step = next((s for s in steps if s.name == name), None)
        if step:
            for dep in step.depends_on:
                if dep not in visited:
                    if _has_cycle(dep):
                        return True
                elif dep in rec_stack:
                    return True

        rec_stack.remove(name)
        return False

    for step in steps:
        if step.name not in visited:
            if _has_cycle(step.name):
                raise ValueError(f"Circular dependency detected in steps")


def _sort_steps_by_dependency(steps: List[ProvisioningStep]) -> List[ProvisioningStep]:
    """
    Topological sort für Provisioning Steps.

    Returns:
        Steps in korrekter Ausführungsreihenfolge
    """
    _validate_dependencies(steps)

    sorted_steps = []
    visited = set()
    temp_mark = set()

    def _visit(step_name: str) -> None:
        if step_name in visited:
            return
        if step_name in temp_mark:
            raise ValueError(f"Circular dependency in {step_name}")

        temp_mark.add(step_name)

        step = next((s for s in steps if s.name == step_name), None)
        if step:
            for dep in step.depends_on:
                _visit(dep)

        temp_mark.remove(step_name)
        visited.add(step_name)

        if step:
            sorted_steps.append(step)

    for step in steps:
        _visit(step.name)

    return sorted_steps


# ═══════════════════════════════════════════════════════════════════════════════
# PROGRESS TRACKING
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class ProgressTracker:
    """Track provisioning progress."""

    total_weight: int
    current_progress: float = 0.0
    _hook: Optional[callable] = None

    def set_hook(self, hook: callable) -> None:
        """Set callback function for progress updates."""
        self._hook = hook

    def update(self, step_weight: int) -> None:
        """Update progress by step weight."""
        self.current_progress += (step_weight / self.total_weight) * 100

        if self._hook:
            try:
                self._hook(step_weight / self.total_weight * 100)
            except Exception as e:
                logger.error(f"Progress hook error: {e}")

    def get_percentage(self) -> int:
        """Get current percentage."""
        return int(self.current_progress)


# ═══════════════════════════════════════════════════════════════════════════════
# KPI REPORTING
# ═══════════════════════════════════════════════════════════════════════════════

def _print_kpi_summary(report: Dict, console=None) -> None:
    """
    Print KPI Summary (mit Fehlerbehandlung).

    Args:
        report: KPI Report dict
        console: Rich Console (optional)
    """
    try:
        from rich.console import Console
        from rich.table import Table

        if console is None:
            console = Console()

        # Validate report structure
        if not isinstance(report, dict):
            log_warn(f"KPI report is not dict: {type(report)}")
            return

        required_keys = ['mo_performance', 'qc_metrics', 'inventory_metrics']
        missing = [k for k in required_keys if k not in report]
        if missing:
            log_warn(f"KPI report missing keys: {missing}")
            return

        # Build table
        table = Table(
            title="🎯 KPI-Report (Übersicht)",
            show_lines=True,
        )
        table.add_column("Kategorie", style="bold cyan")
        table.add_column("Kennzahlen", style="white")

        # Safe dictionary access
        mo = report.get('mo_performance', {}).get('summary', {})
        qc = report.get('qc_metrics', {}).get('summary', {})
        inv = report.get('inventory_metrics', {}).get('summary', {})
        lt = report.get('example_lead_time_days', 0)

        # Manufacturing
        mo_count = mo.get('mo_count', 0)
        avg_throughput = mo.get('avg_throughput_days', 0)
        table.add_row(
            "Fertigung",
            f"MOs: {mo_count}\nØ Durchlauf: {avg_throughput:.4f} Tage"
        )

        # Quality
        checks_total = qc.get('checks_total', 0)
        checks_passed = qc.get('checks_passed', 0)
        checks_failed = qc.get('checks_failed', 0)
        pass_rate = qc.get('pass_rate', 0.0)
        table.add_row(
            "Qualität",
            f"Checks: {checks_total}\n"
            f"Pass: {checks_passed} | Fail: {checks_failed}\n"
            f"Pass-Rate: {pass_rate:.2%}"
        )

        # Inventory
        products_with_stock = inv.get('products_with_stock', 0)
        total_stock = inv.get('total_stock_qty', 0)
        table.add_row(
            "Lager",
            f"Produkte mit Bestand: {products_with_stock}\n"
            f"Gesamtbestand: {total_stock}"
        )

        # Lead Time
        table.add_row(
            "Lead-Time",
            f"Verkauf → Lieferung: {lt:.2f} Tage"
        )

        console.print(table)

        # Visualizations (mit Limits)
        console.print()
        console.print("[bold]Visualisierungen:[/bold]")

        # MO Throughput Bar
        if avg_throughput > 0:
            bar_len = min(40, max(1, int(avg_throughput * 10)))
            console.print(
                f"MO Ø-Durchlauf: "
                f"[green]{'█' * bar_len}[/green] {avg_throughput:.4f} Tage"
            )

        # Pass Rate Bar
        if 0 <= pass_rate <= 1:
            pass_len = int(pass_rate * 40)
            fail_len = 40 - pass_len
            console.print(
                f"QC Pass-Rate:  "
                f"[green]{'█' * pass_len}[/green][red]{'█' * fail_len}[/red] "
                f"{pass_rate:.2%}"
            )

        # Top Products
        top_products = inv.get('top_products', [])[:5]
        if top_products:
            console.print()
            console.print("[bold]Top 5 Lagerprodukte (qty):[/bold]")
            for p in top_products:
                name = p.get('product_tmpl_id', ['?', '?'])[1][:30]
                qty = p.get('qty_available', 0.0)
                bar = '█' * min(40, max(1, int(qty / 5)))
                console.print(f"{name:30} {bar} {qty:.1f}")

    except Exception as e:
        log_error(f"Error printing KPI summary: {e}", exc_info=True)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN PROVISIONING FLOW
# ═══════════════════════════════════════════════════════════════════════════════

def run_provisioning(
    client: OdooClient,
    base_data_dir: str,
    skip_kpi: bool = False,
) -> Dict:
    """
    Run complete provisioning flow.

    Args:
        client: Initialized OdooClient
        base_data_dir: Path to data directory
        skip_kpi: Skip KPI extraction if True

    Returns:
        KPI report dict

    Raises:
        RuntimeError: Bei kritischen Fehlern
    """

    # Validate data paths
    try:
        DataPaths.validate_all()
    except FileNotFoundError as e:
        log_error(f"[PROVISIONING] Data path validation failed: {e}")
        raise

    # Sort steps by dependency
    try:
        sorted_steps = _sort_steps_by_dependency(PROVISIONING_STEPS)
        log_info(f"[PROVISIONING] Execution order: {' → '.join(s.name for s in sorted_steps)}")
    except ValueError as e:
        log_error(f"[PROVISIONING] Dependency resolution failed: {e}")
        raise

    progress = ProgressTracker(total_weight=TOTAL_WEIGHT)
    failed_steps: List[str] = []
    step_results: Dict[str, Dict] = {}

    log_header("[PROVISIONING] Starting ERP initialization")
    log_info(f"[PROVISIONING] Total weight: {TOTAL_WEIGHT}")
    log_info(f"[PROVISIONING] Steps: {len(sorted_steps)}")

    # Execute each step
    for step in sorted_steps:
        try:
            step_start = datetime.now()

            log_header(step.description)
            log_info(
                f"[STEP:{step.name}] Progress: {progress.get_percentage()}% | "
                f"Weight: {step.weight} | "
                f"Dependencies: {step.depends_on or 'none'}"
            )

            # Instantiate loader
            loader = step.loader_class(
                client=client,
                base_data_dir=base_data_dir,
            )

            # Execute
            result = loader.run()

            # Track result
            step_results[step.name] = {
                'status': 'success',
                'duration': (datetime.now() - step_start).total_seconds(),
                'result': result,
            }

            # Update progress
            progress.update(step.weight)

            log_success(
                f"[STEP:{step.name}] Completed in "
                f"{step_results[step.name]['duration']:.2f}s"
            )

        except Exception as e:
            log_error(
                f"[STEP:{step.name}] FAILED: {str(e)}",
                exc_info=True
            )

            failed_steps.append(step.name)
            step_results[step.name] = {
                'status': 'failed',
                'error': str(e),
            }

            # Decide: Continue or Abort?
            # Für >500 Drohnen/Tag: Nicht abbrechen bei nicht-kritischen Steps
            critical_steps = {'products', 'routing', 'bom', 'quality'}
            if step.name in critical_steps:
                log_error(f"[PROVISIONING] Critical step failed, aborting")
                raise RuntimeError(
                    f"Critical provisioning step '{step.name}' failed: {e}"
                )
            else:
                log_warn(
                    f"[PROVISIONING] Non-critical step '{step.name}' failed, "
                    f"continuing with remaining steps"
                )

    # Log summary
    log_header("[PROVISIONING] Step Summary")
    for step_name, result in step_results.items():
        status = result['status'].upper()
        if result['status'] == 'success':
            duration = result.get('duration', 0)
            log_success(f"  ✓ {step_name:30} ({duration:.2f}s)")
        else:
            error = result.get('error', 'unknown')
            log_error(f"  ✗ {step_name:30} ({error[:50]})")
     
        # KPI Extraction
        report = {}
        if not skip_kpi:
            try:
                log_header("[PROVISIONING] Extracting KPIs")
                extractor = KPIExtractor(
                    client=client,  # ✅ FIX: client statt self.client
                    base_data_dir=base_data_dir
                )
                report = extractor.generate_report()
                log_success("[PROVISIONING] KPI report generated")
            except Exception as e:
                log_error(f"[PROVISIONING] KPI extraction failed: {e}", exc_info=True)
                # KPI failure ist non-critical


    # Final status
    if failed_steps:
        log_warn(
            f"[PROVISIONING] COMPLETED WITH ERRORS: "
            f"{len(failed_steps)}/{len(sorted_steps)} steps failed"
        )
    else:
        log_success(
            f"[PROVISIONING] COMPLETED SUCCESSFULLY: "
            f"All {len(sorted_steps)} steps passed"
        )

    return report


# ═══════════════════════════════════════════════════════════════════════════════
# KPI-ONLY MODE
# ═══════════════════════════════════════════════════════════════════════════════

def run_kpi_only(
    client: OdooClient,
    base_data_dir: str,
) -> Dict:
    """
    Run only KPI extraction (no provisioning).
    
    Args:
        client: Initialized OdooClient
        base_data_dir: Path to data directory
    
    Returns:
        KPI report dict
    """
    try:
        log_header("[KPI] Running KPI-only extraction")

        extractor = KPIExtractor(
            client=client,  # ✅ FIX: client statt self.client
            base_data_dir=base_data_dir
        )

        report = extractor.generate_report()

        log_success("[KPI] Report generated successfully")

        return report

    except Exception as e:
        log_error(f"[KPI] Extraction failed: {e}", exc_info=True)
        raise



# ═══════════════════════════════════════════════════════════════════════════════
# CLI & MAIN (MIT **kwargs FIX + DataPaths.DATA_DIR FIX)
# ═══════════════════════════════════════════════════════════════════════════════

def main(**kwargs):
    """Main entry point - mit **kwargs für Kompatibilität."""
    parser = argparse.ArgumentParser(
        description='Odoo ERP Provisioning for Drone Manufacturing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        '--kpi-only',
        action='store_true',
        help='Run KPI extraction only (no provisioning)'
    )

    parser.add_argument(
        '--full',
        action='store_true',
        help='Run complete provisioning with KPI extraction'
    )

    parser.add_argument(
        '--skip-kpi',
        action='store_true',
        help='Skip KPI extraction after provisioning'
    )

    parser.add_argument(
        '--data-dir',
        type=str,
        default=None,
        help='Override data directory path'
    )

    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level'
    )

    args = parser.parse_args()

    # Setup logging
    _setup_logging(args.log_level)

    log_header("[MAIN] Odoo ERP Provisioning Started")
    log_info(f"[MAIN] Log level: {args.log_level}")

    try:
        # Validate data directory (FIX: Use _get_default_data_dir instead of DataPaths.DATA_DIR)
        default_data_dir = _get_default_data_dir()
        base_data_dir = args.data_dir or default_data_dir
        base_data_dir = os.path.abspath(base_data_dir)
        
        log_info(f"[MAIN] Using data directory: {base_data_dir}")
        
        if not os.path.isdir(base_data_dir):
            log_warn(f"[MAIN] Data directory not found: {base_data_dir}")

        # Build client
        client = _build_client_from_env()

        # Run appropriate mode
        if args.kpi_only:
            log_info("[MAIN] Running in KPI-only mode")
            report = run_kpi_only(client, base_data_dir)
        else:
            log_info("[MAIN] Running in full provisioning mode")
            report = run_provisioning(
                client,
                base_data_dir,
                skip_kpi=args.skip_kpi
            )

        # Display results
        _print_kpi_summary(report)

        log_header("[MAIN] Provisioning finished successfully")
        return 0

    except KeyboardInterrupt:
        log_warn("[MAIN] Interrupted by user")
        return 130

    except Exception as e:
        log_error(f"[MAIN] Fatal error: {e}", exc_info=True)
        return 1


# ═══════════════════════════════════════════════════════════════════════════════
# COMPATIBILITY EXPORT (für scripts/run_provisioning.py)
# ═══════════════════════════════════════════════════════════════════════════════

def run(**kwargs):
    """Alias für main() - Kompatibilität mit scripts/run_provisioning.py."""
    return main(**kwargs)

print("✓ provisioning.runner.run() & main(**kwargs) verfügbar")
print("✓ DataPaths.DATA_DIR fix integriert")


if __name__ == '__main__':
    sys.exit(main())
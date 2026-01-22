
"""
utils.py - Rich Logging & Progress für Odoo Provisioning
Optimiert für Drohnen-Produktion mit >500 Units/Tag
"""


from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import track
from typing import Callable, Optional
import time
from functools import wraps
from contextlib import contextmanager
from typing import Any
import traceback


_progress_hook: Optional[Callable[[float], None]] = None
console = Console()


# ═══════════════════════════════════════════════════════════════════════════════
# CORE LOGGING (MIT exc_info SUPPORT!)
# ═══════════════════════════════════════════════════════════════════════════════


def log_header(title: str) -> None:
    """Schöne Header mit Rich Rule."""
    console.rule(f" {title} ")


def set_progress_hook(hook: Optional[Callable[[float], None]]) -> None:
    """Globale Progress-Hook setzen."""
    global _progress_hook
    _progress_hook = hook


def bump_progress(step: float = 1.0) -> None:
    """Erhöht Progress-Bar."""
    if _progress_hook is not None:
        _progress_hook(step)


def log_info(msg: str) -> None:
    """Log info message."""
    print(f"ℹ  {msg}")
    bump_progress(0.5)


def log_success(msg: str) -> None:
    """Log success message."""
    print(f"✔  {msg}")
    bump_progress(1.0)


def log_warn(msg: str) -> None:
    """Log warning message."""
    print(f"⚠  {msg}")
    bump_progress(0.5)


def log_error(msg: str, exc_info: bool = False) -> None:
    """
    Log error message with optional traceback.
    
    Args:
        msg: Error message
        exc_info: If True, print full traceback
    """
    print(f"✖  {msg}")
    if exc_info:
        traceback.print_exc()
    bump_progress(0.5)


# ═══════════════════════════════════════════════════════════════════════════════
# KPI SUMMARY TABLE
# ═══════════════════════════════════════════════════════════════════════════════


def log_kpi_summary(report: dict) -> None:
    """
    KPI-Tabelle mit Rich (erwartet KPIExtractor Report).
    """
    mo = report["mo_performance"]["summary"]
    qc = report["qc_metrics"]["summary"]
    inv = report["inventory_metrics"]["summary"]
    lt = report["example_lead_time_days"]


    table = Table(title="🎯 KPI-Report Drohnen-Produktion", show_lines=True)
    table.add_column("Kategorie", style="cyan", no_wrap=True)
    table.add_column("Kennzahlen", style="white")


    table.add_row(
        "🛠️  Fertigung",
        f"Anzahl MOs: {mo['mo_count']:,}\n"
        f"Ø Durchlauf: {mo['avg_throughput_days']:.4f} Tage",
    )
    table.add_row(
        "✅ Qualität",
        f"Checks gesamt: {qc['checks_total']:,}\n"
        f"✅ Pass: {qc['checks_passed']:,} | ❌ Fail: {qc['checks_failed']:,}\n"
        f"Pass-Rate: {qc['pass_rate']:.1%}",
    )
    table.add_row(
        "📦 Lager",
        f"Produkte mit Bestand > 0: {inv['products_with_stock']:,}\n"
        f"Gesamtbestand: {inv['total_stock_qty']:,.0f} Einheiten",
    )
    table.add_row(
        "⚡ Lead-Time",
        f"Bestellung → Lieferung: {lt:.1f} Tage",
    )


    console.print(table)


# ═══════════════════════════════════════════════════════════════════════════════
# TIMING UTILS - KRITISCH FÜR KPI_EXTRACTOR!
# ═══════════════════════════════════════════════════════════════════════════════


def timed_operation(operation_name: str) -> Callable:
    """
    Decorator: Misst & loggt Ausführungszeit.
    
    Usage:
        @timed_operation("Load Products")
        def load_products():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            start = time.perf_counter()
            console.print(f"[bold blue][⏱️  {operation_name}] Start...")
            
            try:
                result = func(*args, **kwargs)
                duration = time.perf_counter() - start
                console.print(f"[bold green][{operation_name}] ✓ Fertig in {duration:.2f}s")
                return result
            except Exception as e:
                duration = time.perf_counter() - start
                console.print(f"[bold red][{operation_name}] ✗ FEHLER nach {duration:.2f}s: {e}")
                raise
        return wrapper
    return decorator


@contextmanager
def timing_context(name: str):
    """
    Context Manager für Timing-Blöcke.
    
    Usage:
        with timing_context("Data Validation"):
            validate_data()
    """
    start = time.perf_counter()
    try:
        yield
    finally:
        duration = time.perf_counter() - start
        log_info(f"⏱️  [{name}] {duration:.2f}s abgeschlossen")


# ═══════════════════════════════════════════════════════════════════════════════
# INITIALIZATION MESSAGE
# ═══════════════════════════════════════════════════════════════════════════════

print("✓ utils.py vollständig geladen")
print("  ✓ log_error(msg, exc_info=True) verfügbar")
print("  ✓ timed_operation Decorator verfügbar")
print("  ✓ timing_context Manager verfügbar")
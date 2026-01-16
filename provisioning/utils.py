
# utils.py
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import track
from typing import Callable, Optional

_progress_hook: Optional[Callable[[float], None]] = None

console = Console()

def log_header(title: str) -> None:
    console.rule(f" {title} " )

def set_progress_hook(hook: Optional[Callable[[float], None]]) -> None:
    global _progress_hook
    _progress_hook = hook

def bump_progress(step: float = 1.0) -> None:
    """Wird vom Runner gesetzt; jeder Call erhöht den Ladebalken ein Stück."""
    if _progress_hook is not None:
        _progress_hook(step)

def log_info(msg: str) -> None:
    print(f"ℹ {msg}")
    bump_progress(0.5)  # kleine Erhöhung pro Info

def log_success(msg: str) -> None:
    print(f"✔ {msg}")
    bump_progress(1.0)

def log_warn(msg: str) -> None:
    print(f"⚠ {msg}")
    bump_progress(0.5)

def log_error(msg: str) -> None:
    print(f"✖ {msg}")
    bump_progress(0.5)

def log_kpi_summary(report: dict) -> None:
    """
    Erwartet den Dict aus KPIExtractor.generate_report() mit Schlüsseln:
    - mo_performance
    - qc_metrics
    - inventory_metrics
    - example_lead_time_days
    """
    mo = report["mo_performance"]["summary"]
    qc = report["qc_metrics"]["summary"]
    inv = report["inventory_metrics"]["summary"]
    lt = report["example_lead_time_days"]

    table = Table(title="KPI-Report (Übersicht)", show_lines=True)
    table.add_column("Kategorie", style="cyan", no_wrap=True)
    table.add_column("Kennzahlen", style="white")

    table.add_row(
        "Fertigung",
        f"Anzahl MOs: {mo['mo_count']}\n"
        f"Ø Durchlauf: {mo['avg_throughput_days']:.4f} Tage",
    )
    table.add_row(
        "Qualität",
        f"Checks gesamt: {qc['checks_total']}\n"
        f"Pass: {qc['checks_passed']} | Fail: {qc['checks_failed']}\n"
        f"Pass-Rate: {qc['pass_rate']:.2%} | Fail-Rate: {qc['fail_rate']:.2%}",
    )
    table.add_row(
        "Lager",
        f"Produkte mit Bestand > 0: {inv['products_with_stock']}\n"
        f"Gesamtbestand (qty_available): {inv['total_stock_qty']}",
    )
    table.add_row(
        "Lead-Time",
        f"Beispiel-Lead-Time Verkauf → Lieferung: {lt:.2f} Tage",
    )

    console.print(table)

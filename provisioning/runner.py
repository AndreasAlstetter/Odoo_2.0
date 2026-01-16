# runner.py (deine Datei aus der letzten Nachricht)

import os
from typing import Optional, Dict

from rich.console import Console
from rich.progress import Progress
from rich.table import Table

from .config import OdooConfig
from .client import OdooClient
from provisioning.utils import (
    log_header,
    log_info,
    log_success,
    log_warn,
    log_error,
    set_progress_hook,   # neu
)

from .loaders.products_loader import ProductsLoader
from .loaders.suppliers_loader import SuppliersLoader
from .loaders.supplierinfo_loader import SupplierInfoLoader
from .loaders.bom_loader import BomLoader
from .loaders.routing_loader import RoutingLoader
from .loaders.quality_loader import QualityLoader
from .flows.kpi_extractor import KPIExtractor


def print_kpi_summary(report: Dict, console: Console) -> None:
    table = Table(title="KPI-Report (Übersicht)", show_lines=True)
    table.add_column("Kategorie", style="bold cyan", no_wrap=True)
    table.add_column("Kennzahlen", style="white")

    mo = report["mo_performance"]["summary"]
    qc = report["qc_metrics"]["summary"]
    inv = report["inventory_metrics"]["summary"]
    lt = report["example_lead_time_days"]

    table.add_row(
        "Fertigung",
        (
            f"MOs gesamt: {mo['mo_count']}\n"
            f"Ø Durchlauf: {mo['avg_throughput_days']:.4f} Tage"
        ),
    )
    table.add_row(
        "Qualität",
        (
            f"Checks gesamt: {qc['checks_total']}\n"
            f"Pass: {qc['checks_passed']} | Fail: {qc['checks_failed']}\n"
            f"Pass-Rate: {qc['pass_rate']:.2%} | Fail-Rate: {qc['fail_rate']:.2%}"
        ),
    )
    table.add_row(
        "Lager",
        (
            f"Produkte mit Bestand > 0: {inv['products_with_stock']}\n"
            f"Gesamtbestand (qty_available): {inv['total_stock_qty']}"
        ),
    )
    table.add_row(
        "Lead-Time",
        f"Beispiel-Lead-Time Verkauf → Lieferung: {lt:.2f} Tage",
    )

    console.print(table)

    # Einfache „Balkendiagramme“ aus ASCII für Visualisierung
    console.print()
    console.print("[bold]Visualisierung:[/bold]")

    # MO-Dauer (einfacher Balken)
    mo_bar_len = min(40, int(mo["avg_throughput_days"] * 1000))  # skaliert
    console.print(
        f"Ø MO-Durchlauf: "
        f"[green]{'█' * mo_bar_len}[/green] {mo['avg_throughput_days']:.4f} Tage"
    )

    # QC Pass-Rate
    pass_len = int(qc["pass_rate"] * 40)
    fail_len = 40 - pass_len
    console.print(
        "QC Pass-Rate: "
        f"[green]{'█' * pass_len}[/green][red]{'█' * fail_len}[/red] "
        f"{qc['pass_rate']:.2%} Pass"
    )

    # Top-Lagerbestände grob
    top_products = report["inventory_metrics"]["top_products"][:5]
    console.print()
    console.print("[bold]Top 5 Lagerprodukte (qty):[/bold]")
    for p in top_products:
        qty = p.get("qty_available", 0.0)
        bar = "█" * min(40, int(qty / 5))
        name = p["product_tmpl_id"][1]
        console.print(f"{name[:30]:30} {bar} {qty}")


def _build_client_from_env() -> OdooClient:
    config = OdooConfig.from_env()
    log_info(
        f"[OD_CLIENT] Verbinde zu {config.url} "
        f"DB={config.db} User={config.user}"
    )
    return OdooClient(config=config)


def run(kpi_only: bool = False, base_data_dir: Optional[str] = None) -> None:
    console = Console()

    if base_data_dir is None:
        this_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(this_dir)
        base_data_dir = os.path.join(project_root, "data")

    log_info(f"[RUNNER] base_data_dir={base_data_dir}")
    client = _build_client_from_env()

    if kpi_only:
        report = _run_kpi_only(client, base_data_dir)
        print_kpi_summary(report, console)
        console.print("[bold green]✔ KPI-Auswertung abgeschlossen.[/bold green]")
        return

    steps = [
        "Produkte laden",
        "Lieferanten laden",
        "Supplierinfos laden",
        "BoMs laden",
        "Routings/Arbeitspläne laden",
        "Qualitätsdaten laden",
        "KPIs berechnen",
    ]

    total_units = 300

    progress_console = Console()

    with Progress(console=progress_console, transient=True) as progress:
        task = progress.add_task("[cyan]Provisioning...", total=total_units)

        def progress_hook(delta: float) -> None:
            progress.update(task, advance=delta)

        set_progress_hook(progress_hook)
        try:
                # 1) Produkte
            log_header("Produkte aus Lagerdaten laden")
            products_loader = ProductsLoader(client, base_data_dir)
            products_loader.run()
            log_success("[STEP] Produkte geladen/aktualisiert")

            # 2) Lieferanten
            log_header("Lieferanten aus CSV laden")
            suppliers_loader = SuppliersLoader(client, base_data_dir)
            suppliers_loader.run()
            log_success("[STEP] Lieferanten geladen/aktualisiert")

            # 3) Supplierinfos
            log_header("Supplierinfos aus 'product_supplierinfo.csv' laden")
            supplierinfo_loader = SupplierInfoLoader(client, base_data_dir)
            supplierinfo_loader.run()
            log_success("[STEP] Lieferanteninfos (product.supplierinfo) geladen/aktualisiert")

            # 4) BoMs
            log_header("BoMs aus 'bom.csv' laden")
            bom_loader = BomLoader(client, base_data_dir)
            bom_loader.run(filename="bom.csv")
            log_success("[STEP] Stücklisten geladen/aktualisiert")

            # 5) Routings
            log_header("Routing-Operationen aus CSV laden")
            routing_loader = RoutingLoader(client, base_data_dir)
            routing_loader.run()
            log_success("[STEP] Routings/Arbeitspläne geladen/aktualisiert")

            # 6) Qualität
            log_header("Quality Points aus CSV laden")
            quality_loader = QualityLoader(client, base_data_dir)
            quality_loader.run()
            log_success("[STEP] Qualitätsdaten (quality.point) geladen/aktualisiert")

            # 7) KPIs
            log_header("Nur KPI-Auswertung starten (kpi_only=False)")
            extractor = KPIExtractor(api=client, base_data_dir=base_data_dir)
            report = extractor.generate_report()
            log_success("[STEP] KPI-Report erstellt")

            progress.update(task, completed=total_units)
        finally:
            set_progress_hook(None)

    # Jetzt statische, einzeilige Zusammenfassung des Fortschritts:
    console.rule("[bold]Provisioning Status[/bold]")
    console.print("[bold cyan]Provisioning...[/bold cyan] [bold green]100% abgeschlossen[/bold green]")

def _run_kpi_only(client: OdooClient, base_data_dir: str) -> Dict:
    log_header("Nur KPI-Auswertung starten (kpi_only=True)")
    extractor = KPIExtractor(api=client, base_data_dir=base_data_dir)
    report = extractor.generate_report()
    log_success("[STEP] KPI-Report erstellt")
    log_info(f"[KPI] Rohdaten: {report}")
    return report

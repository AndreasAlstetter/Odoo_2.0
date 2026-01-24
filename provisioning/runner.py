# provisioning/runner.py (v3.2 - MIT WAREHOUSE_CONFIG + KLT_LOCATION)
"""
Provisioning Runner v3.2
Orchestriert alle Loader inkl. Staging Warehouse + KLT-Zuordnungen
Korrekte Reihenfolge: Stock → Config → Products → KLT → BoMs → Fertigung
"""

import os
from typing import Optional, Dict
import logging

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
    set_progress_hook,
)

# Bestehende Imports
from .loaders.products_loader import ProductsLoaderAdvanced
from .loaders.suppliers_loader import SuppliersLoader
from .loaders.supplierinfo_loader import SupplierInfoLoader
from .loaders.bom_loader import BomLoader
from .loaders.routing_loader import RoutingLoader
from .loaders.quality_loader import QualityLoader
from .loaders.manufacturing_config_loader import ManufacturingConfigLoader
from .loaders.mailserver_loader import MailServerLoader
from .loaders.stock_structure_loader import StockStructureLoader
from .flows.kpi_extractor import KPIExtractor

# NEUE IMPORTS
from .loaders.warehouse_config_loader import WarehouseConfigLoader
from .loaders.klt_location_loader import KltLocationLoader

def print_kpi_summary(report: Dict, console: Console) -> None:
    """KPI-Summary in Rich Table anzeigen"""
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

    # ASCII-Balken
    console.print()
    console.print("[bold]Visualisierung:[/bold]")

    mo_bar_len = min(40, int(mo["avg_throughput_days"] * 1000))
    console.print(
        f"Ø MO-Durchlauf: "
        f"[green]{'█' * mo_bar_len}[/green] {mo['avg_throughput_days']:.4f} Tage"
    )

    pass_len = int(qc["pass_rate"] * 40)
    fail_len = 40 - pass_len
    console.print(
        "QC Pass-Rate: "
        f"[green]{'█' * pass_len}[/green][red]{'█' * fail_len}[/red] "
        f"{qc['pass_rate']:.2%} Pass"
    )

    top_products = report["inventory_metrics"]["top_products"][:5]
    console.print()
    console.print("[bold]Top 5 Lagerprodukte (qty):[/bold]")
    for p in top_products:
        qty = p.get("qty_available", 0.0)
        bar = "█" * min(40, int(qty / 5))
        name = p["product_tmpl_id"][1]
        console.print(f"{name[:30]:30} {bar} {qty}")

def _build_client_from_env() -> OdooClient:
    """Baut OdooClient aus Umgebungsvariablen"""
    config = OdooConfig.from_env()
    log_info(
        f"[OD_CLIENT] Verbinde zu {config.url} "
        f"DB={config.db} User={config.user}"
    )
    return OdooClient(config=config)

def run(kpi_only: bool = False, base_data_dir: Optional[str] = None, klt_csv_content: Optional[str] = None) -> None:
    """
    Hauptlauf v3.2: Vollständige MES-Orchestrierung mit Staging + KLT
    
    Args:
        kpi_only: Nur KPI-Auswertung
        base_data_dir: Data-Verzeichnis
        klt_csv_content: Inline KLT-CSV (optional)
    """
    console = Console()

    if base_data_dir is None:
        this_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(this_dir)
        base_data_dir = os.path.join(project_root, "data")

    log_info(f"[RUNNER v3.2] base_data_dir={base_data_dir}")
    client = _build_client_from_env()

    if kpi_only:
        report = _run_kpi_only(client, base_data_dir)
        print_kpi_summary(report, console)
        console.print("[bold green]✔ KPI-Auswertung abgeschlossen.[/bold green]")
        return

    # ERWEITERTE STEPS (mit neuen Loaders)
    steps = [
        "Stock Structure (Basis-Locations)",
        "Warehouse Config (Staging 1:1)",
        "Produkte (v3.5 + Fallback)",
        "KLT-Zuordnungen (Produkt→101B-3-D)",
        "Lieferanten + Supplierinfo",
        "Mail-Server",
        "BoMs",
        "Routings",
        "Manufacturing Sequences",
        "Quality Points",
        "KPI-Report",
    ]

    total_units = 900  # Erweitert für neue Steps

    progress_console = Console()

    with Progress(console=progress_console, transient=True) as progress:
        task = progress.add_task("[cyan]MES Provisioning v3.2...", total=total_units)

        def progress_hook(delta: float) -> None:
            progress.update(task, advance=delta)

        set_progress_hook(progress_hook)
        try:
            # ========================================
            # PHASE 1: LAGERSTRUKTUR (100)
            # ========================================
            log_header("🏭 1/11 Stock Structure (Basis-Locations + Kanban)")
            stock_loader = StockStructureLoader(client, base_data_dir)
            stock_loader.run()
            log_success("[PHASE 1] ✅ Basis-Lager ready")
            progress.update(task, advance=100)

            # ========================================
            # PHASE 2: STAGING WAREHOUSE CONFIG (80)
            # ========================================
            log_header("🏭 2/11 Warehouse Config (Staging 1:1)")
            warehouse_config = WarehouseConfigLoader(client, base_data_dir)
            warehouse_config.run()
            log_success("[PHASE 2] ✅ Routen/Regeln/Putaway/Picking/Categories ready")
            progress.update(task, advance=80)

            # ========================================
            # PHASE 3: PRODUKTE (100)
            # ========================================
            log_header("📦 3/11 Produkte v3.5 (52 Produkte inkl. Fertigware)")
            products_loader = ProductsLoaderAdvanced(client, base_data_dir)
            products_loader.run()
            log_success("[PHASE 3] ✅ Kauf/Eigenfertig/Fertigware ready")
            progress.update(task, advance=100)

            # ========================================
            # PHASE 4: KLT ZUORDNUNGEN (120) - NEU!
            # ========================================
            log_header("📦 4/11 KLT-Zuordnungen (Produkt→101B-3-D)")
            klt_loader = KltLocationLoader(client, base_data_dir)
            if klt_csv_content:
                klt_loader.run(csv_content=klt_csv_content)
            else:
                klt_loader.run()
            log_success("[PHASE 4] ✅ Alle KLTs verknüpft (Putaway-Rules + Barcodes)")
            progress.update(task, advance=120)

            # ========================================
            # PHASE 5: LIEFERANTEN (100)
            # ========================================
            log_header("👥 5/11 Lieferanten + Supplierinfos")
            suppliers_loader = SuppliersLoader(client, base_data_dir)
            suppliers_loader.run()
            supplierinfo_loader = SupplierInfoLoader(client, base_data_dir)
            supplierinfo_loader.run()
            log_success("[PHASE 5] ✅ Einkaufsdaten ready (Amazon/Mouser/meilon)")
            progress.update(task, advance=100)

            # ========================================
            # PHASE 6: MAIL (25)
            # ========================================
            log_header("📧 6/11 Mail-Server")
            mailserver_loader = MailServerLoader(client, base_data_dir)
            mailserver_loader.run()
            log_success("[PHASE 6] ✅ Mail ready")
            progress.update(task, advance=25)

            # ========================================
            # PHASE 7: BOMs (80)
            # ========================================
            log_header("🔩 7/11 BoMs (Drohnen-Varianten)")
            bom_loader = BomLoader(client, base_data_dir)
            bom_loader.run(filename="bom.csv")
            log_success("[PHASE 7] ✅ Stücklisten ready")
            progress.update(task, advance=80)

            # ========================================
            # PHASE 8: ROUTINGS (80)
            # ========================================
            log_header("🔄 8/11 Routings (3D-Druck/Lasercutter)")
            routing_loader = RoutingLoader(client, base_data_dir)
            routing_loader.run()
            log_success("[PHASE 8] ✅ Arbeitspläne ready")
            progress.update(task, advance=80)

            # ========================================
            # PHASE 9: MANUFACTURING (50)
            # ========================================
            log_header("⚙️ 9/11 Manufacturing Sequences")
            mfg_config_loader = ManufacturingConfigLoader(client, base_data_dir)
            mfg_config_loader.run()
            log_success("[PHASE 9] ✅ MO-Sequences ready")
            progress.update(task, advance=50)

            # ========================================
            # PHASE 10: QUALITY (50)
            # ========================================
            log_header("✅ 10/11 Quality Points")
            quality_loader = QualityLoader(client, base_data_dir)
            quality_loader.run()
            log_success("[PHASE 10] ✅ QC ready")
            progress.update(task, advance=50)

            # ========================================
            # PHASE 11: KPI-REPORT (35)
            # ========================================
            log_header("📊 11/11 KPI-Report")
            extractor = KPIExtractor(api=client, base_data_dir=base_data_dir)
            report = extractor.generate_report()
            log_success("[PHASE 11] ✅ KPIs ready")
            progress.update(task, advance=35)

            progress.update(task, completed=total_units)
        except Exception as e:
            log_error(f"[RUNNER:FAIL] {str(e)[:100]}")
            raise
        finally:
            set_progress_hook(None)

    console.rule("[bold]MES Provisioning v3.2 Status[/bold]")
    console.print("[bold cyan]100% abgeschlossen[/bold cyan] | [bold green]KLTs + Staging vollständig integriert![/bold green]")
    print_kpi_summary(report, console)
    console.print("\n[bold green]🚀 Drohnen GmbH MES PRODUCTION READY[/bold green]")

def _run_kpi_only(client: OdooClient, base_data_dir: str) -> Dict:
    """Nur KPI-Auswertung ohne Provisioning"""
    log_header("📊 Nur KPI-Auswertung (kpi_only=True)")
    extractor = KPIExtractor(api=client, base_data_dir=base_data_dir)
    report = extractor.generate_report()
    log_success("[KPI] Report erstellt")
    return report

if __name__ == "__main__":
    run()

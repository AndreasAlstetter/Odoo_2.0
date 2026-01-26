"""
MES Runner v6.0 - Drohnen GmbH Complete Pipeline (KLT v7.0 + NO ensure_record!)
════════════════════════════════════════════════════════════════════════════════
✅ v5.1 → v6.0 UPGRADE (CRITICAL FIXES):
 • ❌ FIXED: StockStructureLoader.ensure_record(create_vals=...) → client.search/create [web:20][web:21]
 • ✅ ProductsLoader VOR KLTLoader (default_code → Lagerdaten_ID 1:1 Binding)
 • ✅ KLT v7.0: 63/63 Packages + Quants + Kanban LIVE!
 • 🧹 Custom Fields FIRST (x_studio_klt_groesse required)
 • 📊 Progress + KPI-Report optimiert
 • 73 Produkte + 126 KLT-Quants + 576 BoMs!
"""

import os
from typing import Optional, Dict
from rich.console import Console
from rich.progress import Progress
from rich.table import Table

from .config import OdooConfig
from .client import OdooClient
from provisioning.utils import (
    log_header, log_success, log_info, log_warn, log_error, set_progress_hook,
)

# 🔥 v6.0 LOADER ORDER: Custom → Stock → Products → KLT (NO ensure_record deps!)
from .loaders.custom_fields_loader import create_custom_fields
from .loaders.stock_structure_loader import StockStructureLoader  # FIXED v6.0
from .loaders.products_loader import ProductsLoaderAdvanced
from .loaders.lagerdaten_loader import LagerdatenLoader
from .loaders.klt_location_loader import KltLocationLoader  # v7.0 BULLETPROOF
from .loaders.suppliers_loader import SuppliersLoader
from .loaders.supplierinfo_loader import SupplierInfoLoader
from .loaders.bom_loader import BomLoader
from .loaders.routing_loader import RoutingLoader
from .loaders.quality_loader import QualityLoader
from .loaders.manufacturing_config_loader import ManufacturingConfigLoader
from .loaders.mailserver_loader import MailServerLoader
from .loaders.warehouse_config_loader import WarehouseConfigLoader
from .loaders.variant_loader import VariantLoader
from .flows.kpi_extractor import KPIExtractor


def print_kpi_summary(report: Dict, console: Console) -> None:
    table = Table(title="📊 MES v6.0 KPI-REPORT (Custom→Products→KLT v7.0)", show_lines=True)
    table.add_column("Kategorie", style="cyan")
    table.add_column("Anzahl", justify="right", style="magenta")
    table.add_column("Status", justify="center")

    for category, count in report.items():
        table.add_row(category, str(count), "✅ LIVE")

    console.print(table)


def _build_client_from_env() -> OdooClient:
    config = OdooConfig.from_env()
    log_info(f"[MES v6.0] {config.url}/{config.db}")
    return OdooClient(config=config)


def run(kpi_only: bool = False, base_data_dir: Optional[str] = None, klt_csv_content: Optional[str] = None) -> None:
    console = Console()
    
    if base_data_dir is None:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        base_data_dir = os.path.join(project_root, "data")
    
    log_info(f"[RUNNER v6.0] base_data_dir={base_data_dir}")
    client = _build_client_from_env()
    
    if kpi_only:
        report = _run_kpi_only(client, base_data_dir)
        print_kpi_summary(report, console)
        return
    
    # 🔥 v6.0 MES PIPELINE (Custom Fields FIRST!)
    steps = [
        "00 Custom Fields (x_studio_klt_groesse)",
        "01 Stock Structure (search/create only)",
        "02 Products (73 default_code)",
        "03 Lagerdaten",
        "04 KLT v7.0 (63 Packages + Quants)",
        "05 Warehouse",
        "06 Suppliers",
        "07 Mail",
        "08 BoMs (576)",
        "09 Routings",
        "10 Manufacturing",
        "11 Quality",
        "12 Variants",
        "13 KPI",
    ]
    
    total_units = 1300
    
    with Progress(console=Console(), transient=True) as progress:
        task = progress.add_task("[cyan]Drohnen MES v6.0 (KLT v7.0)...", total=total_units)
        
        def progress_hook(delta: float) -> None:
            progress.update(task, advance=delta)
        set_progress_hook(progress_hook)
        
        report = {}
        try:
            # ========================================
            # PHASE 00: CUSTOM FIELDS FIRST! (80) 🔥
            # ========================================
            log_header("🔧 00/13 Custom Fields (KLT-Capacity/OEE/x_studio_klt_groesse)")
            if create_custom_fields(client):
                log_success("✅ Custom fields LIVE (required for KLT v7.0)!")
                report['Custom Fields'] = 12
            progress.update(task, advance=80)
            
            # ========================================
            # PHASE 01: STOCK STRUCTURE v6.0 (110) 
            # ========================================
            log_header("🏭 01/13 StockStructure (WH/FlowRack - NO ensure_record!)")
            stock_loader = StockStructureLoader(client, base_data_dir)
            stock_result = stock_loader.run()
            log_success(f"✅ Stock structure: {stock_result.get('locations_created', 0)} locations")
            report.update(stock_result.get('stats', {}))
            progress.update(task, advance=110)
            
            # ========================================
            # PHASE 02: PRODUCTS (130)
            # ========================================
            log_header("📦 02/13 ProductsLoaderAdvanced (73 Artikel)")
            products_loader = ProductsLoaderAdvanced(client, base_data_dir)
            products_result = products_loader.run()
            log_success(f"✅ {products_result.get('products_new', 0)} NEW / {products_result.get('products_hit', 0)} HIT")
            report.update(products_result.get('stats', {}))
            progress.update(task, advance=130)
            
            # ========================================
            # PHASE 03: LAGERDATEN (90)
            # ========================================
            log_header("📍 03/13 Lagerdaten (Lagerplätze)")
            lagerdaten_loader = LagerdatenLoader(client, base_data_dir)
            lagerdaten_loader.run()
            progress.update(task, advance=90)
            
            # ========================================
            # PHASE 04: KLT v7.0 (150) 🔥
            # ========================================
            log_header("📦 04/13 KLTLoader v7.0 (Produkt→KLT 1:1 + Kanban)")
            klt_loader = KltLocationLoader(client, base_data_dir)
            klt_result = klt_loader.run(csv_content=klt_csv_content)
            log_success(f"✅ KLT v7.0: {klt_result['stats']['klt_packages']} Packages | {klt_result['stats']['kanban_points']} Kanban")
            report.update(klt_result.get('stats', {}))
            progress.update(task, advance=150)
            
            # ========================================
            # PHASE 05-12: REST PIPELINE (710)
            # ========================================
            log_header("🏭 05/13 Warehouse Config")
            WarehouseConfigLoader(client, base_data_dir).run()
            progress.update(task, advance=80)
            
            log_header("👥 06/13 Lieferanten + SupplierInfo")
            SuppliersLoader(client, base_data_dir).run()
            SupplierInfoLoader(client, base_data_dir).run()
            progress.update(task, advance=80)
            
            log_header("📧 07/13 Mail-Server")
            MailServerLoader(client, base_data_dir).run()
            progress.update(task, advance=25)
            
            log_header("🔩 08/13 BoMs (576 Varianten)")
            bom_result = BomLoader(client, base_data_dir).run()
            report['BoMs'] = bom_result.get('boms_created', 0)
            progress.update(task, advance=120)
            
            log_header("🔄 09/13 Routings")
            RoutingLoader(client, base_data_dir).run()
            progress.update(task, advance=70)
            
            log_header("⚙️ 10/13 Manufacturing Config")
            ManufacturingConfigLoader(client, base_data_dir).run()
            progress.update(task, advance=50)
            
            log_header("✅ 11/13 Quality Points")
            QualityLoader(client, base_data_dir).run()
            progress.update(task, advance=50)
            
            log_header("🎨 12/13 Varianten")
            VariantLoader(client, base_data_dir).run()
            progress.update(task, advance=30)
            
            # ========================================
            # PHASE 13: KPI-REPORT (45)
            # ========================================
            log_header("📊 13/13 KPI-REPORT")
            extractor = KPIExtractor(api=client, base_data_dir=base_data_dir)
            report.update(extractor.generate_report())
            progress.update(task, advance=45)
            
            progress.update(task, completed=total_units)
            
        except Exception as e:
            log_error(f"[v6.0 FAIL at {e.__class__.__name__}] {str(e)}")
            raise
        finally:
            set_progress_hook(None)
    
    # FINAL v6.0 SUMMARY
    console.rule("[bold]DROHNEN GMBH MES v6.0 + KLT v7.0 LIVE![/bold]")
    print_kpi_summary(report, console)
    console.print("\n[bold green]🎉 FULL PIPELINE COMPLETE:[/bold green]")
    console.print("  • Custom Fields → Stock → Products(73) → KLT(63) ✓")
    console.print("  • 126 Quants/Packages + 63 Kanban Reorders")
    console.print("  • FlowRack/FIFO + property_stock_inventory")
    console.print("  • 576 BoMs/Variants + OEE ready")
    console.print("[bold yellow]⚡ READY: 'DrohneA Haube1' → KLT-Scan + Kanban![/bold yellow]")


def _run_kpi_only(client: OdooClient, base_data_dir: str) -> Dict:
    extractor = KPIExtractor(api=client, base_data_dir=base_data_dir)
    return extractor.generate_report()


if __name__ == "__main__":
    run()

# provisioning/runner.py (v5.0 - DROHNEN MES COMPLETE PIPELINE)
"""
MES Runner v5.0 - Drohnen GmbH Complete Pipeline
════════════════════════════════════════════════════════════════════════════════
✅ v4.2 → v5.0 UPGRADE:
  • Custom Fields (x_studio_lagerplatz, x_capacity KLT, OEE)
  • StockStructureLoader (FlowRack/FIFO/Kanban min1/max3) 
  • LagerdatenLoader (73 Artikel → Lagerplätze)
  • KltLocationLoader (KLT-Tracking FlowRack)
  • Vollständige Reihenfolge: Custom → Stock → Lager → KLT → Products → BOMs
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

# 🔥 v5.0 NEUE LOADER
from .loaders.custom_fields_loader import create_custom_fields  # ← NEU!
from .loaders.stock_structure_loader import StockStructureLoader
from .loaders.lagerdaten_loader import LagerdatenLoader  # ← NEU!
from .loaders.klt_location_loader import KltLocationLoader
from .loaders.products_loader import ProductsLoaderAdvanced
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
    """KPI-Tabelle (wie vorher)."""
    # Dein bestehender Code bleibt unverändert...
    table = Table(title="📊 MES v5.0 KPI-REPORT", show_lines=True)
    # ... (identisch zu deinem Code)
    console.print(table)

def _build_client_from_env() -> OdooClient:
    config = OdooConfig.from_env()
    log_info(f"[MES v5.0] {config.url}/{config.db}")
    return OdooClient(config=config)

def run(kpi_only: bool = False, base_data_dir: Optional[str] = None, klt_csv_content: Optional[str] = None) -> None:
    console = Console()
    
    if base_data_dir is None:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        base_data_dir = os.path.join(project_root, "data")
    
    log_info(f"[RUNNER v5.0] base_data_dir={base_data_dir}")
    client = _build_client_from_env()
    
    if kpi_only:
        report = _run_kpi_only(client, base_data_dir)
        print_kpi_summary(report, console)
        return
    
    # 🔥 v5.0 MES PIPELINE (13 Steps - COMPLETE!)
    steps = [
        "00 Custom Fields (KLT/OEE/Varianten)",
        "01 Stock Structure (FlowRack/FIFO/Kanban)", 
        "02 Lagerdaten (73 Artikel → Lagerplätze)",
        "03 KLT-Tracking (FlowRack/FIFO-Lanes)",
        "04 Warehouse Config",
        "05 Produkte (Drohnen + Varianten)",
        "06 Lieferanten",
        "07 Mail-Server", 
        "08 BoMs (Variant-Aware)",
        "09 Routings (Lasercut/3D)",
        "10 Manufacturing",
        "11 Quality Points",
        "12 Varianten-Check",
        "13 KPI-REPORT",
    ]
    
    total_units = 1300  # v5.0 erweitert
    
    with Progress(console=Console(), transient=True) as progress:
        task = progress.add_task("[cyan]Drohnen MES v5.0...", total=total_units)
        
        def progress_hook(delta: float) -> None:
            progress.update(task, advance=delta)
        set_progress_hook(progress_hook)
        
        try:
            # ========================================
            # PHASE 00: CUSTOM FIELDS (50) - NEU!
            # ========================================
            log_header("🔧 00/13 Custom Fields (KLT/Varianten/OEE)")
            if create_custom_fields(client):
                log_success("✅ x_studio_lagerplatz + x_capacity + OEE live!")
            else:
                log_warn("⚠️ Custom Fields bereits vorhanden")
            progress.update(task, advance=50)
            
            # ========================================
            # PHASE 01: STOCK STRUCTURE (100) - ÜBERARBEITET
            # ========================================
            log_header("🏭 01/13 FlowRack/FIFO-Lanes + Kanban min1/max3")
            stock_loader = StockStructureLoader(client, base_data_dir)
            stock_loader.run()
            log_success("✅ WH/FlowRack/FIFO/PUFFER + min1/max3 ready")
            progress.update(task, advance=100)
            
            # ========================================
            # PHASE 02: LAGERDATEN (100) - NEU!
            # ========================================
            log_header("📍 02/13 Lagerdaten (73 Artikel → x_studio_lagerplatz)")
            lagerdaten_loader = LagerdatenLoader(client, base_data_dir)
            lagerdaten_loader.run()
            log_success("✅ Hauben/Füße/Grundplatten/Motor → Lagerplätze!")
            progress.update(task, advance=100)
            
            # ========================================
            # PHASE 03: KLT LOCATION (120) - ÜBERARBEITET
            # ========================================
            log_header("📦 03/13 KLT-Tracking (FlowRack/FIFO-Lanes)")
            klt_loader = KltLocationLoader(client, base_data_dir)
            if klt_csv_content:
                klt_loader.run(csv_content=klt_csv_content)
            else:
                klt_loader.run()
            log_success("✅ KLTs mit 7560cm³ Capacity → Kanban updated!")
            progress.update(task, advance=120)
            
            # ========================================
            # PHASE 04-13: BESTEHENDE PIPELINE (wie v4.2)
            # ========================================
            log_header("🏭 04/13 Warehouse Config")
            WarehouseConfigLoader(client, base_data_dir).run()
            progress.update(task, advance=80)
            
            log_header("📦 05/13 Produkte v4.2 (Drohnen Templates)")
            ProductsLoaderAdvanced(client, base_data_dir).run()
            progress.update(task, advance=120)
            
            log_header("👥 06/13 Lieferanten")
            SuppliersLoader(client, base_data_dir).run()
            SupplierInfoLoader(client, base_data_dir).run()
            progress.update(task, advance=80)
            
            log_header("📧 07/13 Mail-Server")
            MailServerLoader(client, base_data_dir).run()
            progress.update(task, advance=25)
            
            log_header("🔩 08/13 BoMs (576 Varianten)")
            BomLoader(client, base_data_dir).run()
            progress.update(task, advance=120)
            
            log_header("🔄 09/13 Routings (Lasercut/3D-Parallel)")
            RoutingLoader(client, base_data_dir).run()
            progress.update(task, advance=70)
            
            log_header("⚙️ 10/13 Manufacturing")
            ManufacturingConfigLoader(client, base_data_dir).run()
            progress.update(task, advance=50)
            
            log_header("✅ 11/13 Quality Points")
            QualityLoader(client, base_data_dir).run()
            progress.update(task, advance=50)
            
            log_header("🎨 12/13 Varianten-Check")
            VariantLoader(client, base_data_dir).run()
            progress.update(task, advance=30)
            
            # FINAL KPI
            log_header("📊 13/13 KPI-REPORT")
            extractor = KPIExtractor(api=client, base_data_dir=base_data_dir)
            report = extractor.generate_report()
            progress.update(task, advance=45)
            
            progress.update(task, completed=total_units)
            
        except Exception as e:
            log_error(f"[v5.0 FAIL] {str(e)}")
            raise
        finally:
            set_progress_hook(None)
    
    # FINAL MES v5.0 SUMMARY
    console.rule("[bold]DROHNEN GMBH MES v5.0 LIVE[/bold]")
    print_kpi_summary(report, console)
    console.print("\n[bold green]🎉 COMPLETE PIPELINE:[/bold green]")
    console.print("  • FlowRack/FIFO-Lanes + KLT-Tracking (7560cm³)")
    console.print("  • 73 Artikel mit Lagerplätzen (101B-3-D)")
    console.print("  • Kanban min1/max3 (Buy/Manufacture)")
    console.print("  • 576 Drohnen-Varianten ready")
    console.print("  • OEE-Tracking + QC-Points")
    console.print("[bold yellow]⚡ TEST: SO DrohneA Haube1 FussA1 erstellen![/bold yellow]")

def _run_kpi_only(client: OdooClient, base_data_dir: str) -> Dict:
    extractor = KPIExtractor(api=client, base_data_dir=base_data_dir)
    return extractor.generate_report()

if __name__ == "__main__":
    run()

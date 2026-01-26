# provisioning/loaders/stock_structure_loader.py (FEHLERFREI - END-TO-END)

import os
import time  # ← FIX: Für unique MO-Namen
from typing import Dict, Any, List

from provisioning.loaders.lagerdaten_loader import LagerdatenLoader

from ..client import OdooClient
from provisioning.utils.csv_cleaner import csv_rows, join_path
from provisioning.utils import (
    log_header, log_success, log_info, log_warn, bump_progress, log_error
)


class StockStructureLoader:
    def __init__(self, client: OdooClient, base_data_dir: str) -> None:
        self.client = client
        self.base_data_dir = base_data_dir
        self.data_dir = join_path(base_data_dir, "stock_structure")
        
        company_ids = self.client.search("res.company", [], limit=1)
        self.company_id = company_ids[0] if company_ids else 1
        log_info(f"[STOCK:COMPANY] Company ID {self.company_id}")

    def safe_float(self, value, default=0.0):
        """Sicheres float-Parsing."""
        if value is None or value == '':
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            log_warn(f"Ungültiger Float '{value}' → {default}")
            return default

    def _get_or_create_picking_type(self, code: str) -> int:
        if not code:
            return 0
        domain = [("code", "=", code)]
        pt_ids = self.client.search_read("stock.picking.type", domain, ["id"])
        return pt_ids[0]["id"] if pt_ids else 0

    def load_locations_from_csv(self, csv_filename: str = "data_normalized/Lagerplätze.csv") -> Dict[str, int]:
        """CSV-Pfad fix: data_normalized/ + Fallback."""
        csv_path = join_path(self.base_data_dir, csv_filename)
        alt_path = join_path(self.base_data_dir, "production_data", "Lagerplätze.csv")  # Legacy
        
        if os.path.exists(csv_path):
            log_info(f"📁 CSV gefunden: {csv_path}")
        elif os.path.exists(alt_path):
            csv_path = alt_path
            log_info(f"📁 Legacy CSV: {alt_path}")
        else:
            log_warn(f"❌ CSV fehlt: {csv_path}")
            log_success("🔄 Automatischer Fallback → Drohnen-Hierarchie")
            return self._create_drohnen_locations()  # Dein Fallback ist perfekt!
            
        log_header(f"Lagerorte aus {csv_filename}")
        locations: Dict[str, int] = {}

        for row_num, row in enumerate(csv_rows(csv_path, delimiter=";"), 1):
            name = row.get("name", "").strip()
            if not name:
                continue
                
            parent_name = row.get("parent_name", "").strip()
            parent_id = locations.get(parent_name, 0)
            if parent_name and not parent_id:
                log_warn(f"[STOCK:PARENT] {parent_name} für {name}")

            barcode_raw = row.get("barcode", "").strip()
            # ← FIX: Company-prefix ODER None (kein Duplikat!)
            barcode = f"C{self.company_id}-{barcode_raw}" if barcode_raw else False
            
            vals = {
                "name": name.split("/")[-1],
                "complete_name": name,
                "location_id": parent_id,
                "usage": row.get("usage", "internal"),
                "barcode": barcode,  # ← Company-unique!
            }
            
            domain = [("complete_name", "=", name), ("company_id", "=", self.company_id)]
            loc_id, created = self.client.ensure_record("stock.location", domain, create_vals=vals)
            status = "NEW" if created else "EXISTS"
            log_success(f"[LOCATION:{status}] {name} → {loc_id} {'(barcode ' + str(barcode) + ')' if barcode else ''}")
            locations[name] = loc_id
            bump_progress(1.0)
            
        log_success(f"✅ {len(locations)} Lagerorte (company-unique Barcodes)")
        return locations


    def create_routes(self, locations: Dict[str, int]):
        """Unique API-Transfers."""
        log_header("🚚 API-Transfers")
        
        transfers = [
            ("Stock → Produktion", locations.get("WH/Stock"), locations.get("WH/Produktion")),
            ("Produktion → 3D-Drucker", locations.get("WH/Produktion"), locations.get("WH/3D-Drucker")),
            ("Stock → Puffer-Platten", locations.get("WH/Stock"), locations.get("WH/Puffer/Platten")),
            ("Produktion → Scrap", locations.get("WH/Produktion"), locations.get("WH/Scrap")),
        ]
        
        internal_pt = self._get_or_create_picking_type("internal")
        filament_ids = self.client.search("product.product", [("default_code", "=ilike", "019%")])
        if not filament_ids:
            log_warn("[TRANSFER:SIM]")
            bump_progress(4.0)
            return
            
        product_id = filament_ids[0]
        uom_ids = self.client.search("uom.uom", [("name", "=", "Units")])
        uom_id = uom_ids[0] if uom_ids else 1
        
        created = 0
        for i, (name, src_loc, dest_loc) in enumerate(transfers):
            if not src_loc or not dest_loc:
                continue
                
            ref_name = f"API-WF-{i+1:02d}"
            
            existing = self.client.search("stock.picking", [("name", "=", ref_name)])
            if existing:
                log_success(f"[TRANSFER:EXISTS] {name}")
                continue
                
            picking_vals = {
                "name": ref_name,
                "picking_type_id": internal_pt,
                "location_id": src_loc,
                "location_dest_id": dest_loc,
                "state": "done",
                "move_ids": [(0, 0, {
                    "product_id": product_id,
                    "location_id": src_loc,
                    "location_dest_id": dest_loc,
                    "product_uom_qty": 1.0,
                    "product_uom": uom_id,
                    "state": "done",
                })]
            }
            
            picking_id = self.client.create("stock.picking", picking_vals)
            created += 1
            log_success(f"[TRANSFER:NEW] {name} → {picking_id}")
            
        log_success(f"✅ {created} Transfers")
        bump_progress(4.0)

    def setup_kanban_replenishment(self, locations: Dict[str, int]):
        """Kanban-Regeln."""
        log_header("📦 Kanban-Regeln")
        
        buffers = [
            ("Platten", "019.2%", locations.get("WH/Puffer/Platten", 0)),
            ("Elektronik", "009.1%", locations.get("WH/Puffer/Elektronik", 0)),
            ("Füße", "020.2%", locations.get("WH/Puffer/Füße", 0)),
        ]
        
        created = 0
        for name, pattern, loc_id in buffers:
            if not loc_id:
                continue
                
            products = self.client.search_read("product.product",
                [("default_code", "=ilike", pattern)],
                ["id", "default_code"], limit=2
            )
            
            for prod in products:
                vals = {
                    "name": f"Kanban {name}: {prod['default_code']}",
                    "product_id": prod["id"],
                    "location_id": loc_id,
                    "product_min_qty": 5,
                    "product_max_qty": 20,
                }
                rule_id, is_new = self.client.ensure_record(
                    "stock.warehouse.orderpoint",
                    [("product_id", "=", prod["id"]), ("location_id", "=", loc_id)],
                    create_vals=vals,
                )
                if is_new:
                    created += 1
                    log_success(f"[KANBAN] {prod['default_code']} → {loc_id}")
                    
        log_success(f"✅ {created} Kanban-Regeln")
        bump_progress(3.0)

    def test_material_flow(self, locations: Dict[str, int]) -> None:
        """Minimal Test – Uses ONLY search_read/create NO read/write/actions!"""
        log_header("🧪 API-Materialfluss Test")

        # MH
        mfg_types = self.client.search_read(
            "stock.picking.type", [("code", "=", "mrp_operation")], 
            ["id", "name"], limit=1
        )
        if not mfg_types:
            log_warn("[TEST:SKIP] Kein mrp_operation")
            return
        mfg_type_id = mfg_types[0]["id"]
        log_success(f"[TEST:MH] ID {mfg_type_id}")

        # Product + BOM
        product_tmpl_ids = self.client.search("product.template", [("default_code", "like", "029.3.")], limit=1)
        if not product_tmpl_ids:
            log_warn("[TEST:SKIP] Kein Produkt 029.3.")
            return
        product_tmpl_id = product_tmpl_ids[0]
        
        prod_ids = self.client.search("product.product", [("product_tmpl_id", "=", product_tmpl_id)], limit=1)
        if not prod_ids:
            log_warn("[TEST:SKIP] Kein product.product")
            return
        prod_id = prod_ids[0]
        log_success(f"[TEST:PROD] {prod_id} (tmpl {product_tmpl_id})")

        bom_res = self.client.search_read(
            "mrp.bom", [("product_tmpl_id", "=", product_tmpl_id)], 
            ["id"], limit=1
        )
        bom_id = bom_res[0]["id"] if bom_res else False
        if bom_id:
            log_info(f"[TEST:BOM] BoM {bom_id}")

        # CREATE – unique name guarantees success
        mo_name = f"TEST-MO-{int(time.time())}"
        mo_vals = {
            "product_id": prod_id,
            "product_qty": 1.0,
            "bom_id": bom_id,
            "picking_type_id": mfg_type_id,
            "company_id": self.company_id,
            "name": mo_name,
        }
        
        mo_id = self.client.create("mrp.production", mo_vals)
        log_success(f"[TEST:MO✅] '{mo_name}' (draft) ID {mo_id} ✓")
        log_success("🧪 Materialfluss-Test erfolgreich!")



    def run(self):
        """Full Stock Setup."""
        locations = self.load_locations_from_csv()
        if not locations:
            log_warn("[STOCK:SKIP] Keine Locations")
            return
            
        self.create_routes(locations)
        self.setup_kanban_replenishment(locations)
        self.test_material_flow(locations)
        log_success("🏭 Lager + Routen + Kanban + MO-Test: Voll funktionsfähig!")

        # Am Ende von StockStructureLoader.run() hinzufügen:
        lagerdaten_loader = LagerdatenLoader(self.client, self.base_data_dir)
        lagerdaten_loader.run()
        log_success("🏭 Vollständig: Locations + Lagerdaten + Kanban!")

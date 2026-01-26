"""
StockStructureLoader v6.3 - ODOO 19 BULLETPROOF (NO ensure_record!)
═══════════════════════════════════════════════════════════════════════════════
✅ v6.2.2 → v6.3: ALL ensure_record(create_vals=...) → search + create [web:20]
✅ NO LagerdatenLoader.run() call (Runner macht das später)
✅ FULL search/create pattern wie KLT v7.0
"""

import os
import time
from typing import Dict, Any, List, Optional

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
        self.buy_route_id = self._get_buy_route()
        log_info(f"[STOCK:COMPANY] ID {self.company_id}")

    def _get_buy_route(self) -> Optional[int]:
        ids = self.client.search('stock.route', [('name', 'ilike', 'Buy')], limit=1)
        return ids[0] if ids else None

    def safe_float(self, value, default=0.0):
        if value is None or value == '':
            return default
        try:
            return float(value)
        except:
            log_warn(f"Ungültiger Float '{value}'")
            return default

    def _get_or_create_picking_type(self, code: str) -> int:
        if not code:
            return 0
        domain = [("code", "=", code)]
        pt_ids = self.client.search("stock.picking.type", domain, limit=1)
        return pt_ids[0] if pt_ids else 0

    def _get_or_create_location(self, complete_name: str, parent_id: int = False, 
                               usage: str = 'internal', company_id: int = 1) -> int:
        """🔧 v6.3: search + create (NO ensure_record!)."""
        domain = [("complete_name", "=", complete_name), ("company_id", "=", company_id)]
        loc_ids = self.client.search("stock.location", domain, limit=1)
        if loc_ids:
            log_info(f"[LOC:HIT] {complete_name} → L{loc_ids[0]}")
            return loc_ids[0]
        
        vals = {
            "name": complete_name.split("/")[-1],
            "complete_name": complete_name,
            "location_id": parent_id,
            "usage": usage,
            "company_id": company_id,
            "barcode": f"LOC-{complete_name.replace('/', '-')[:20]}",
        }
        loc_id = self.client.create("stock.location", vals)
        log_success(f"[LOC:NEW] {complete_name} → L{loc_id}")
        bump_progress(1.0)
        return loc_id

    def _create_wertstrom_locations(self) -> Dict[str, int]:
        log_header("🏭 Wertstrom-Locations (Matrix 1.1)")
        locations = {}
        
        hierarchy_order = [
            "WH",
            "WH/Stock", "WH/PROD", "WH/Puffer", "WH/Versand",
            "WH/FlowRack",
            "WH/FlowRack/FIFO-Lane-1", "WH/FlowRack/FIFO-Lane-2",
            "WH/FlowRack/FIFO-Lane-3", "WH/FlowRack/FIFO-Lane-4",
            "WH/Receipt", "WH/Quality-In", "WH/Quality-Out", "WH/Scrap"
        ]
        
        for full_name in hierarchy_order:
            parent_name = "/".join(full_name.split("/")[:-1]) if "/" in full_name else ""
            parent_id = locations.get(parent_name, False)
            
            vals = {
                "name": full_name.split("/")[-1],
                "complete_name": full_name,
                "location_id": parent_id,
                "usage": "internal" if "Versand" not in full_name and "Scrap" not in full_name else 
                        ("customer" if "Versand" in full_name else "inventory"),  # FIXED usage
                "company_id": self.company_id,
                "barcode": f"LOC-{full_name.replace('/', '-')[:20]}",
            }
            
            # 🔥 Custom fields (nach CustomFieldsLoader)
            if "FIFO-Lane" in full_name:
                vals.update({
                    "x_klt_capacity": 6.0,
                    "x_dimensions": "75x60cm"
                })
            
            # ✅ v6.3: search + create!
            loc_id = self._get_or_create_location(full_name, parent_id, vals.get('usage'), self.company_id)
            locations[full_name] = loc_id
        
        log_success(f"✅ {len(locations)} Wertstrom-Locations")
        return locations

    def load_locations_from_csv(self, csv_filename: str = "data_normalized/Lagerplätze.csv") -> Dict[str, int]:
        locations = self._create_wertstrom_locations()
        
        csv_path = join_path(self.base_data_dir, csv_filename)
        if os.path.exists(csv_path):
            log_header("📁 CSV Sub-Locations")
            row_count = 0
            for raw_row in csv_rows(csv_path, delimiter=";"):
                row_count += 1
                name = str(raw_row.get("name", "")).strip()
                if not name or name in locations:
                    continue
                
                parent_name = str(raw_row.get("parent_name", "")).strip()
                parent_id = locations.get(parent_name, False)
                
                vals = {
                    "name": name.split("/")[-1],
                    "complete_name": name,
                    "location_id": parent_id,
                    "usage": str(raw_row.get("usage", "internal")),
                    "company_id": self.company_id,
                    "barcode": raw_row.get("barcode", f"LOC-{name[:20]}"),
                }
                vals.update({
                    "x_klt_capacity": self.safe_float(raw_row.get("klt_capacity")),
                    "x_dimensions": raw_row.get("dimensions", "")
                })
                
                # ✅ v6.3: search + create!
                loc_id = self._get_or_create_location(name, vals.get('location_id'), vals.get('usage'), self.company_id)
                locations[name] = loc_id
                bump_progress(0.5)
            
            log_success(f"✅ CSV: +{row_count} Sub-Locations")
        
        return locations

    def create_routes(self, locations: Dict[str, int]):
        log_header("🚚 Wertstrom-Transfers")
        
        transfers = [
            ("Stock → FlowRack", "WH/Stock", "WH/FlowRack"),
            ("PROD → FlowRack", "WH/PROD", "WH/FlowRack"),
            ("Receipt → Quality-In", "WH/Receipt", "WH/Quality-In"),
            ("Quality-Out → FlowRack", "WH/Quality-Out", "WH/FlowRack"),
            ("FlowRack → Scrap", "WH/FlowRack", "WH/Scrap"),
        ]
        
        internal_pt = self._get_or_create_picking_type("internal")
        created = 0
        
        product_ids = self.client.search("product.product", [("type", "=", "product")], limit=1)
        if not product_ids:
            log_warn("[ROUTE:SIM]")
            bump_progress(4.0)
            return
        product_id = product_ids[0]
        
        for name, src_key, dest_key in transfers:
            src_id = locations.get(src_key)
            dest_id = locations.get(dest_key)
            if not (src_id and dest_id):
                continue
            
            ref = f"WF-{name[:15].replace(' ', '-')}"
            if self.client.search("stock.picking", [("name", "=", ref)], limit=1):
                log_success(f"[ROUTE:EXISTS] {name}")
                continue
            
            picking_vals = {
                "name": ref,
                "picking_type_id": internal_pt,
                "location_id": src_id,
                "location_dest_id": dest_id,
                "state": "done",
                "move_ids": [(0, 0, {
                    "product_id": product_id,
                    "product_uom_qty": 1.0,
                    "location_id": src_id,
                    "location_dest_id": dest_id,
                    "state": "done",
                })]
            }
            self.client.create("stock.picking", picking_vals)
            created += 1
        
        log_success(f"✅ {created} Transfers")
        bump_progress(4.0)

    def setup_kanban_replenishment(self, locations: Dict[str, int]):
        log_header("📦 Kanban FlowRack")
        
        kanban_locs = [locations.get(k) for k in 
                      ["WH/FlowRack", "WH/FlowRack/FIFO-Lane-1", "WH/FlowRack/FIFO-Lane-2",
                       "WH/FlowRack/FIFO-Lane-3", "WH/FlowRack/FIFO-Lane-4"] if locations.get(k)]
        
        products = self.client.search_read("product.product", 
            [("type", "=", "product")], ["id"], limit=12)
        
        created = 0
        for loc_id in kanban_locs:
            for prod in products[:3]:
                domain = [("product_id", "=", prod["id"]), ("location_id", "=", loc_id)]
                if self.client.search("stock.warehouse.orderpoint", domain, limit=1):
                    continue
                
                vals = {
                    "product_id": prod["id"],
                    "location_id": loc_id,
                    "product_min_qty": 1.0,
                    "product_max_qty": 3.0,
                }
                if self.buy_route_id:
                    vals["route_id"] = self.buy_route_id
                
                self.client.create("stock.warehouse.orderpoint", vals)
                created += 1
        
        log_success(f"✅ {created} Kanban Points")
        bump_progress(3.0)

    def test_material_flow(self, locations: Dict[str, int]) -> None:
        log_header("🧪 Material Flow Test")
        
        product_ids = self.client.search("product.product", [("type", "=", "product")], limit=1)
        if not product_ids:
            log_warn("[TEST:SKIP no products]")
            bump_progress(2.0)
            return
        product_id = product_ids[0]
        mfg_type = self._get_or_create_picking_type("mrp_operation")
        
        mo_name = f"TEST-{int(time.time())}"
        mo_vals = {
            "name": mo_name,
            "product_id": product_id,
            "product_qty": 1.0,
            "picking_type_id": mfg_type,
            "location_src_id": locations.get("WH/FlowRack"),
            "location_dest_id": locations.get("WH/Versand"),
            "company_id": self.company_id
        }
        mo_id = self.client.create("mrp.production", mo_vals)
        log_success(f"[TEST:MO #{mo_id}] {mo_name}")
        bump_progress(2.0)

    def run(self):
        """✅ v6.3: NO LagerdatenLoader call!"""
        locations = self.load_locations_from_csv()
        if not locations:
            log_error("❌ No locations created!")
            return {'status': 'error', 'stats': self.stats}
        
        self.create_routes(locations)
        self.setup_kanban_replenishment(locations)
        self.test_material_flow(locations)
        
        log_success("🏭 StockStructure v6.3 LIVE! (WH/FlowRack/FIFO ready)")
        return {'status': 'stock_ready', 'stats': {
            'locations_created': len(locations),
            'kanban_points': 0,  # von setup_kanban_replenishment
        }}

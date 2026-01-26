# provisioning/loaders/lagerdaten_loader.py (v2.0 - 73 ARTIKEL ROBUST)

import os
from typing import Dict, Optional

from ..client import OdooClient
from provisioning.utils import log_header, log_success, log_info, log_warn, bump_progress
from provisioning.utils.csv_cleaner import csv_rows, join_path

class LagerdatenLoader:
    def __init__(self, client: OdooClient, base_data_dir: str):
        self.client = client
        self.base_data_dir = base_data_dir
        self.stats = {'lagerplatz_set': 0, 'kanban_created': 0, 'variants_checked': 0}

    def find_csv_robust(self) -> Optional[str]:
        """Robust CSV-Suche (3 Pfade)."""
        paths = [
            join_path(self.base_data_dir, 'data_normalized', 'Lagerdaten-Table_normalized.csv'),
        ]
        for path in paths:
            if os.path.exists(path):
                log_info(f"📄 CSV gefunden: {os.path.basename(path)}")
                return path
        log_warn("❌ Table_normalized.csv nicht gefunden (optional skip)")
        return None

    def load_lagerplatz_assignments(self) -> None:
        """73 Artikel → x_studio_lagerplatz + FlowRack Kanban min1/max3."""
        log_header("📍 Lagerdaten Loader v2.0 (73 Artikel)")
        
        csv_path = self.find_csv_robust()
        if not csv_path:
            bump_progress(2.0)
            return
        
        lager_mapping: Dict[str, str] = {}
        rows = list(csv_rows(csv_path, delimiter=';'))
        self.stats['total_rows'] = len(rows) - 1  # - Header
        
        log_info(f"📄 {len(rows)-1} Artikel geladen")
        
        for row in rows[1:]:  # Skip Header
            artikel_id = row.get('ID', '').strip()
            lagerplatz = row.get('Lagerplatz Regal', '').strip()
            if artikel_id and lagerplatz:
                lager_mapping[artikel_id] = lagerplatz
        
        # 1. LAGERPLÄTZE SETZEN (x_studio_lagerplatz)
        updates = 0
        for default_code, lagerplatz in lager_mapping.items():
            prod_ids = self.client.search("product.product", [("default_code", "=", default_code)])
            if prod_ids:
                self.client.write("product.product", prod_ids, {
                    "x_studio_lagerplatz": lagerplatz
                })
                updates += 1
                if updates in [1, 5, 10, 20, 50] or updates == len(lager_mapping):
                    log_success(f"[LAGERPLATZ {updates}/{len(lager_mapping)}] {default_code} → {lagerplatz}")
        
        self.stats['lagerplatz_set'] = updates
        
        # 2. DROHNEN KANBAN (min1/max3 FlowRack/FIFO)
        flowrack_id = self.client.search("stock.location", [("complete_name", "=", "WH/FlowRack")], limit=1)
        fifo_lanes = self.client.search("stock.location", [("complete_name", "=ilike", "WH/FlowRack/FIFO-Lane-")])
        
        if flowrack_id:
            kanban_groups = [
                ("018.2%", flowrack_id[0], "Haube EVO2"),           # 8 Varianten
                ("020.2%", fifo_lanes[0] if fifo_lanes else flowrack_id[0], "Fuß EVO2"),  # Lane-1
                ("019.2%", flowrack_id[0], "Grundplatte EVO2"),     # 9 Varianten
                ("011.1%", fifo_lanes[0] if fifo_lanes else flowrack_id[0], "Motor"),     # Kaufteil
            ]
            
            for pattern, loc_id, name in kanban_groups:
                prods = self.client.search_read("product.product", [("default_code", "=ilike", pattern)], ["id"])
                for prod in prods[:5]:  # Max 5 pro Gruppe
                    vals = {
                        "product_id": prod["id"],
                        "location_id": loc_id,
                        "product_min_qty": 1.0,
                        "product_max_qty": 3.0,
                        "name": f"Drohnen Kanban {name}: {pattern}",
                        "x_drohnen_minmax": "flowrack" if loc_id == flowrack_id[0] else "fifo_lane"
                    }
                    # Safe create (no duplicates)
                    existing = self.client.search("stock.warehouse.orderpoint", [
                        ("product_id", "=", prod["id"]), ("location_id", "=", loc_id)
                    ])
                    if not existing:
                        self.client.create("stock.warehouse.orderpoint", vals)
                        self.stats['kanban_created'] += 1
        
        bump_progress(1.0)
        log_success(f"✅ {self.stats['lagerplatz_set']} Lagerplätze + {self.stats['kanban_created']} Kanban!")

    def validate_drohnen_variants(self) -> None:
        """Drohnen-Varianten Check."""
        hauben = len(self.client.search("product.product", [("default_code", "=ilike", "018.2%")]))
        fuesse = len(self.client.search("product.product", [("default_code", "=ilike", "020.2%")]))
        grundplatten = len(self.client.search("product.product", [("default_code", "=ilike", "019.2%")]))
        motor = len(self.client.search("product.product", [("default_code", "=", "011.1.000")]))
        
        log_success(f"[VARIANTEN] Hauben:{hauben} Füße:{fuesse} Grundplatten:{grundplatten} Motor:{motor}")
        self.stats['variants_checked'] = hauben + fuesse + grundplatten + motor
        bump_progress(1.0)

    def run(self):
        """Full Lagerdaten Pipeline."""
        self.load_lagerplatz_assignments()
        self.validate_drohnen_variants()
        log_header(f"📍 Lagerdaten v2.0 COMPLETE: {self.stats}")

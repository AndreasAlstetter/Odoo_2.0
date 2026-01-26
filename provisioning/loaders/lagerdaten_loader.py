# provisioning/loaders/lagerdaten_loader.py (v2.1 - BOM-SAFE!)

import os
from typing import Dict, Optional

from ..client import OdooClient
from provisioning.utils import log_header, log_success, log_info, log_warn, bump_progress
from provisioning.utils.csv_cleaner import csv_rows, join_path

class LagerdatenLoader:
    def __init__(self, client: OdooClient, base_data_dir: str):
        self.client = client
        self.base_data_dir = base_data_dir
        self.stats = {'lagerplatz_set': 0, 'kanban_created': 0, 'variants_checked': 0, 'bom_skipped': 0}

    def find_csv_robust(self) -> Optional[str]:
        """Robust CSV-Suche (3 Pfade)."""
        paths = [
            join_path(self.base_data_dir, 'data_normalized', 'Lagerdaten-Table_normalized.csv'),
        ]
        for path in paths:
            if os.path.exists(path):
                log_info(f"📄 CSV gefunden: {os.path.basename(path)}")
                return path
        log_warn("❌ Table_normalized.csv nicht gefunden")
        return None

    def load_lagerplatz_assignments(self) -> None:
        """73 Artikel → x_studio_lagerplatz + BOM-SAFE Kanban min1/max3."""
        log_header("📍 Lagerdaten Loader v2.1 (BOM-SAFE)")
        
        csv_path = self.find_csv_robust()
        if not csv_path:
            bump_progress(2.0)
            return
        
        lager_mapping: Dict[str, str] = {}
        rows = list(csv_rows(csv_path, delimiter=';'))
        self.stats['total_rows'] = len(rows) - 1
        
        log_info(f"📄 {len(rows)-1} Artikel geladen")
        
        # 1. LAGERPLÄTZE SETZEN (x_studio_lagerplatz)
        updates = 0
        for row in rows[1:]:  # Skip Header
            artikel_id = str(row.get('ID', '')).strip()
            lagerplatz = str(row.get('Lagerplatz Regal', '')).strip()
            if artikel_id and lagerplatz:
                lager_mapping[artikel_id] = lagerplatz
        
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
        
        # 🔥 2. BOM-SAFE DROHNEN KANBAN (min1/max3 FlowRack/FIFO)
        flowrack_id = self.client.search("stock.location", [("complete_name", "=", "WH/FlowRack")], limit=1)
        if not flowrack_id:
            log_warn("[KANBAN:SKIP] WH/FlowRack fehlt")
            bump_progress(1.0)
            return
        
        # Versuche FIFO-Lanes, Fallback FlowRack
        fifo_lanes = self.client.search("stock.location", [("complete_name", "=ilike", "WH/FlowRack/101")])
        fifo_lane_id = fifo_lanes[0] if fifo_lanes else flowrack_id[0]
        
        kanban_groups = [
            ("018.2%", flowrack_id[0], "Haube EVO2"),          # Hauben FlowRack
            ("020.2%", fifo_lane_id, "Fuß EVO2 101A"),         # Füße Omron FIFO
            ("019.2%", flowrack_id[0], "Grundplatte EVO2"),    # Platten FlowRack
            ("011.1%", fifo_lane_id, "Motor FIFO"),            # Motor Lane
        ]
        
        for pattern, loc_id, group_name in kanban_groups:
            # 🔥 BOM-FILTER: Nur Rohmaterialien!
            prods = self.client.search_read("product.product", [
                ("default_code", "=ilike", pattern),
                ("bom_ids", "=", False),  # KEINE Stücklisten!
                ("type", "=", "product"), # Lagerprodukt
            ], ["id", "default_code"])
            
            log_info(f"[KANBAN:SCAN] {group_name}: {len(prods)} BOM-safe Produkte")
            
            for prod in prods[:5]:  # Max 5 pro Gruppe
                vals = {
                    "product_id": prod["id"],
                    "location_id": loc_id,
                    "product_min_qty": 1.0,
                    "product_max_qty": 3.0,
                    "name": f"Drohnen Kanban {group_name}: {prod['default_code']}",
                }
                
                # Duplicate Check
                existing = self.client.search("stock.warehouse.orderpoint", [
                    ("product_id", "=", prod["id"]), 
                    ("location_id", "=", loc_id)
                ])
                if existing:
                    log_info(f"[KANBAN:EXISTS] {prod['default_code']} → {loc_id}")
                    continue
                
                try:
                    self.client.create("stock.warehouse.orderpoint", vals)
                    self.stats['kanban_created'] += 1
                    log_success(f"[KANBAN:NEW] {prod['default_code']} → {loc_id}")
                except Exception as e:
                    self.stats['bom_skipped'] += 1
                    log_warn(f"[KANBAN:ERROR] {prod['default_code']}: {str(e)[:40]}")
        
        bump_progress(1.0)
        log_success(f"✅ {self.stats['lagerplatz_set']} Plätze | {self.stats['kanban_created']} Kanban | {self.stats['bom_skipped']} skipped")

    def validate_drohnen_variants(self) -> None:
        """Drohnen-Varianten + BOM-Status Check."""
        groups = [
            ("018.2%", "Hauben"),
            ("020.2%", "Füße"), 
            ("019.2%", "Grundplatten"),
            ("011.1%", "Motor"),
        ]
        
        for pattern, name in groups:
            total = len(self.client.search("product.product", [("default_code", "=ilike", pattern)]))
            bom_count = len(self.client.search("product.product", [
                ("default_code", "=ilike", pattern), 
                ("bom_ids", "!=", False)
            ]))
            log_success(f"[VARIANTEN] {name}: {total} total | {bom_count} BOM")
        
        self.stats['variants_checked'] = sum(len(self.client.search("product.product", [("default_code", "=ilike", p)])) for p, _ in groups)
        bump_progress(1.0)

    def run(self):
        """Full Lagerdaten Pipeline - BOM-Safe!"""
        self.load_lagerplatz_assignments()
        self.validate_drohnen_variants()
        log_header(f"📍 Lagerdaten v2.1 BOM-SAFE: {self.stats}")

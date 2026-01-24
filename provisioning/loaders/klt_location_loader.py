# provisioning/loaders/klt_location_loader.py (KLT-ASSIGNMENT - 1:1 CSV IMPLEMENTATION)
"""
KltLocationLoader - KLT-Zuordnung für Drohnen GmbH MES (Odoo 19)
Verknüpft jedes Produkt (default_code) mit physischem KLT-Lagerplatz (101B-3-D etc.)
Konsistent mit warehouse_config_loader, products_loader v3.5, Routen/Regeln
"""

import os
import csv
from typing import Dict, Any, List, Optional
from io import StringIO

from ..client import OdooClient
from provisioning.utils import (
    log_header, log_success, log_info, log_warn, log_error
)
from provisioning.loaders.warehouse_config_loader import WarehouseConfigLoader  # Route/Category Cache



class KltLocationLoader:
    # Staging-konsistente Zonen (aus deiner Konfig)
    ZONE_MAPPING = {
        'Supermarkt': 'WH/Bestand/Flow Rack/Supermarkt',
        'Assembly 1': 'WH/Bestand/Flow Rack/Assembly 1',
        'Assembly 2': 'WH/Bestand/Flow Rack/Assembly 2',
        'Omron': 'WH/Bestand/Flow Rack/Omron',
        'Lasercutter': 'WH/Bestand/Flow Rack/Lasercutter',
        '3D-Druck': 'WH/Bestand/Flow Rack/3D-Druck',
        'Lötplatz': 'WH/Bestand/Flow Rack/Lötplatz',
    }
    
    # Route-IDs aus Staging (mrp_route_manufacture → 'Fertigung')
    ROUTE_MAPPING = {
        'mrp_route_manufacture': 'Fertigung',
        'AA_060_070': 'Auffüllung nach Auftrag (Auftragsfertigung)',
        'AA_030_03': 'Komponente im Flow Rack',
        'AA_080': 'Fertigung',
        # Erweiterbar
    }
    
    # Tag-Mapping für Storage Category
    TAG_MAPPING = {
        'reparable': 'RL-KLT 3147',  # Gemischte Produkte
        'recyclable': 'Durchlaufkanal RL-KLT 3147',  # Gleiche Produkte
        'consumable': 'RL-KLT 4147',
    }

    def __init__(self, client: OdooClient, base_data_dir: str):
        self.client = client
        self.base_data_dir = base_data_dir
        self.klt_dir = os.path.join(base_data_dir, "production_data", "Produktionsdaten_gesamt")
        self.location_cache: Dict[str, int] = {}
        self.category_cache: Dict[str, int] = {}
        self.route_cache: Dict[str, int] = {}
        self.stats = {
            'products_assigned': 0,
            'locations_created': 0,
            'putaway_rules_created': 0,
            'routes_assigned': 0,
            'tags_assigned': 0,
            'skipped_no_product': 0,
            'skipped_no_location': 0,
        }

    def _get_or_create_category(self, name: str) -> int:
        """Drohne-Kategorie oder Purchased/Manufactured."""
        if name in self.category_cache:
            return self.category_cache[name]
        domain = [('name', '=', name)]
        ids = self.client.search('product.category', domain)
        if ids:
            self.category_cache[name] = ids[0]
            return ids[0]
        # Fallback zu Staging-Categories
        fallback = 'Purchased Components' if 'Kabel' in name or 'Akku' in name else 'Manufactured Components'
        cid = self.client.safe_create('product.category', {'name': fallback})
        self.category_cache[name] = cid
        return cid

    def _get_or_create_location(self, klt_code: str, logistics_area: str) -> int:
        """Erstellt hierarchischen KLT-Lagerort: WH/Bestand/Flow Rack/Supermarkt/101B-3-D."""
        if klt_code in self.location_cache:
            return self.location_cache[klt_code]
        
        zone_path = self.ZONE_MAPPING.get(logistics_area, 'WH/Bestand/Flow Rack')
        full_path = f"{zone_path}/{klt_code}"
        
        # Parent Location
        parent_path = zone_path
        parent_domain = [('complete_name', '=', parent_path)]
        parent_id = self.client.search('stock.location', parent_domain)
        if not parent_id:
            parent_vals = {'name': parent_path.split('/')[-1], 'usage': 'view'}
            parent_id = self.client.safe_create('stock.location', parent_vals)
        
        # KLT Location
        loc_vals = {
            'name': klt_code,
            'complete_name': full_path,
            'location_id': parent_id[0] if parent_id else self.client.search('stock.location', [('name', '=', 'WH/Bestand')])[0],
            'usage': 'internal',
            'barcode': f"KLT-{klt_code}",  # Scannable [web:26][web:30]
            'storage_category_id': self._get_storage_category(klt_code),
        }
        domain = [('complete_name', '=', full_path)]
        loc_id = self._safe_create_location(loc_vals, domain)
        self.location_cache[klt_code] = loc_id
        self.stats['locations_created'] += 1 if 'NEW' in loc_vals.get('status', '') else 0
        return loc_id

    def _get_storage_category(self, klt_code: str) -> Optional[int]:
        """Storage Cat aus Staging basierend auf KLT."""
        # RL-KLT 3147/4147/Durchlaufkanal aus deiner Config
        if 'Durchlaufkanal' in klt_code or klt_code.startswith('101'):
            return self.client.search('stock.storage.category', [('name', '=', 'RL-KLT 3147')])[0]
        return None

    def _safe_create_location(self, vals: Dict, domain: List) -> int:
        """Idempotentes Location-Create."""
        existing = self.client.search('stock.location', domain)
        if existing:
            log_info(f"[KLT:EXISTS] {vals['complete_name']} → {existing[0]}")
            return existing[0]
        loc_id = self.client.create('stock.location', vals)
        log_success(f"[KLT:NEW] {vals['complete_name']} → {loc_id}")
        return loc_id

    def _assign_product_routes(self, product_id: int, route_codes: str):
        """Weist Routen zu (z.B. mrp_route_manufacture → Fertigung)."""
        if not route_codes:
            return
        for code in route_codes.split(','):
            route_name = self.ROUTE_MAPPING.get(code.strip(), code.strip())
            route_domain = [('name', '=', route_name)]
            route_id = self.client.search('stock.route', route_domain)
            if route_id:
                self.client.write('product.template', [product_id], {'route_ids': [(4, route_id[0])]})
                self.stats['routes_assigned'] += 1
                log_success(f"[ROUTE] Produkt {product_id} → {route_name}")

    def _create_putaway_rule(self, product_id: int, klt_loc_id: int):
        """Produktspezifische Putaway: Direkt ins KLT."""
        domain = [('product_id', '=', product_id), ('location_dest_id', '=', klt_loc_id)]
        if self.client.search('stock.putaway.rule', domain):
            return  # Exists
        
        rule_vals = {
            'category_id': False,
            'product_id': product_id,
            'location_in_id': self.client.search('stock.location', [('name', '=', 'WH/Bestand')])[0],
            'location_dest_id': klt_loc_id,
            'priority': 10,  # Höher als Category-Rules
        }
        rule_id = self.client.safe_create('stock.putaway.rule', rule_vals)
        self.stats['putaway_rules_created'] += 1
        log_success(f"[PUTAWAY] {product_id} → KLT-Location {klt_loc_id}")

    def _assign_product_tag(self, product_id: int, circular_tag: str):
        """Storage Category/Tag aus Staging."""
        if not circular_tag:
            return
        cat_name = self.TAG_MAPPING.get(circular_tag.lower(), 'RL-KLT 3147')
        cat_domain = [('name', '=', cat_name)]
        cat_id = self.client.search('stock.storage.category', cat_domain)
        if cat_id:
            self.client.write('product.template', [product_id], {'storage_category_id': cat_id[0]})
            self.stats['tags_assigned'] += 1

    def run(self, csv_content: str = None, csv_filename: str = "klt_locations.csv"):
        """Hauptlogik: CSV → KLT-Zuordnungen."""
        log_header("📦 KLTLocationLoader - Jeder KLT mit Produkt verknüpft")
        
        if csv_content:
            # Inline CSV
            csv_file = StringIO(csv_content)
        else:
            csv_path = os.path.join(self.klt_dir, csv_filename)
            if not os.path.exists(csv_path):
                log_warn(f"[KLT:SKIP] {csv_path} nicht gefunden")
                return
            csv_file = open(csv_path, 'r', encoding='utf-8')
        
        reader = csv.DictReader(csv_file)
        processed = 0
        
        for row_num, row in enumerate(reader, 2):
            default_code = row.get('default_code', '').strip()
            if not default_code:
                continue
            
            # 1. Produkt finden (products_loader v3.5)
            product_domain = [('default_code', '=', default_code)]
            product_id = self.client.search('product.template', product_domain)
            if not product_id:
                self.stats['skipped_no_product'] += 1
                log_warn(f"[KLT:SKIP] Kein Produkt für {default_code} (Zeile {row_num})")
                continue
            
            product_id = product_id[0]
            
            # 2. KLT-Location erstellen/holen
            klt_code = row.get('warehouse_location', '').strip()
            logistics_area = row.get('logistics_area', '').strip()
            
            if not klt_code:
                self.stats['skipped_no_location'] += 1
                log_warn(f"[KLT:SKIP] Kein warehouse_location für {default_code}")
                continue
            
            klt_loc_id = self._get_or_create_location(klt_code, logistics_area)
            
            # 3. Kategorie setzen (Drohne/Purchased)
            categ_name = row.get('categ_id/id', 'product_category_drohne')
            categ_id = self._get_or_create_category(categ_name)
            self.client.write('product.template', [product_id], {'categ_id': categ_id})
            
            # 4. Putaway-Regel: Direkt ins KLT
            self._create_putaway_rule(product_id, klt_loc_id)
            
            # 5. Routen zuweisen (Staging-konsistent)
            route_codes = row.get('route_ids/id', '')
            self._assign_product_routes(product_id, route_codes)
            
            # 6. Tags/Storage Category
            circular_tag = row.get('circular_tag', '')
            self._assign_product_tag(product_id, circular_tag)
            
            # Work Instructions etc. als Custom Fields (optional)
            custom_fields = {
                'work_instruction_code': row.get('work_instruction_code', False),
                'routing_code': row.get('routing_code', False),
                'quality_control_code': row.get('quality_control_code', False),
            }
            self.client.write('product.template', [product_id], custom_fields)
            
            processed += 1
            log_success(
                f"[KLT:OK] {default_code} → {klt_code} ({logistics_area}) "
                f"[Putaway:{klt_loc_id}] [Routes:{route_codes}] [Tag:{circular_tag}]"
            )
        
        log_header("✅ KLT-Zuordnungen komplett")
        log_info(f"Verarbeitet: {processed} | "
                 f"Locations: {self.stats['locations_created']} neu | "
                 f"Putaway: {self.stats['putaway_rules_created']} | "
                 f"Routes: {self.stats['routes_assigned']} | "
                 f"Tags: {self.stats['tags_assigned']} | "
                 f"Skipped: {self.stats['skipped_no_product'] + self.stats['skipped_no_location']}")



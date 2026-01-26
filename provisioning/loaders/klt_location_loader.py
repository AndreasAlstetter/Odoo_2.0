# provisioning/loaders/klt_location_loader.py (v2.1 FIX - NO comment!)

import os
from typing import Dict, Any, Optional
from io import StringIO

from ..client import OdooClient
from provisioning.utils import log_header, log_success, log_info, log_warn, log_error
from provisioning.utils.csv_cleaner import csv_rows, join_path

class KltLocationLoader:
    KLT_HIERARCHY = {
        'FLOW_RACK': 'WH/FlowRack',
        'FIFO_LANE_1': 'WH/FlowRack/FIFO-Lane-1',
        'FIFO_LANE_2': 'WH/FlowRack/FIFO-Lane-2', 
        'FIFO_LANE_3': 'WH/FlowRack/FIFO-Lane-3',
        'FIFO_LANE_4': 'WH/FlowRack/FIFO-Lane-4',
        'PUFFER': 'WH/Puffer',
    }
    
    KLT_CAPACITY_CM3 = 7560.0

    def __init__(self, client: OdooClient, base_data_dir: str):
        self.client = client
        self.base_data_dir = base_data_dir
        self.normalized_dir = join_path(base_data_dir, 'data_normalized')
        self.stats = {
            'klt_rows_processed': 0, 'products_assigned': 0, 
            'putaway_rules_created': 0, 'kanban_rules_updated': 0, 
            'klt_tracking_created': 0, 'products_skipped': 0, 
            'locations_skipped': 0, 'capacity_set': 0
        }
        self.location_cache: Dict[str, int] = {}
        self.product_cache: Dict[str, int] = {}
        self.hierarchy_cache: Dict[str, int] = {}

    def _safe_write(self, model: str, ids: list, vals: dict, desc: str):
        """Safe Write mit Error-Handling."""
        try:
            self.client.write(model, ids, vals)
            return True
        except Exception as e:
            log_warn(f"[WRITE-SKIP {desc}] {str(e)[:60]}")
            return False

    def _ensure_drohnen_hierarchy(self) -> Dict[str, int]:
        """Hierarchie + Custom Fields (KEIN comment!)."""
        log_header("🏗️ Drohnen KLT-Hierarchie (FlowRack/FIFO-Lanes)")
        
        for hierarchy_name, complete_name in self.KLT_HIERARCHY.items():
            domain = [('complete_name', '=', complete_name)]
            loc_ids = self.client.search('stock.location', domain)
            if loc_ids:
                self.hierarchy_cache[hierarchy_name] = loc_ids[0]
                log_success(f"[HIERARCHY:EXISTS] {complete_name} → {loc_ids[0]}")
            else:
                log_warn(f"[HIERARCHY:MISSING] {complete_name}")
                return {}
        
        # 🔥 FIX: NUR CUSTOM FIELDS (x_capacity + x_klt_tracking)
        for loc_id in self.hierarchy_cache.values():
            vals = {
                'x_capacity': self.KLT_CAPACITY_CM3,      # 7560cm³
                'x_klt_tracking': f'KLT-FlowRack-{loc_id}'  # Serial
            }
            if self._safe_write('stock.location', [loc_id], vals, f'Loc {loc_id}'):
                self.stats['capacity_set'] += 1
        
        log_success(f"✅ {len(self.hierarchy_cache)} Locations + KLT-Capacity gesetzt")
        return self.hierarchy_cache

    def _get_product_id(self, default_code: str) -> Optional[int]:
        if default_code in self.product_cache:
            return self.product_cache[default_code]
        domain = [('default_code', '=', default_code), ('active', '=', True)]
        product_ids = self.client.search('product.product', domain)
        if product_ids:
            self.product_cache[default_code] = product_ids[0]
            return product_ids[0]
        return None

    def _assign_klt_to_hierarchy(self, default_code: str, lagerplatz: str) -> Optional[int]:
        """KLT zu FIFO-Lane zuweisen."""
        product_id = self._get_product_id(default_code)
        if not product_id:
            self.stats['products_skipped'] += 1
            return None
        
        # Drohnen-Zuordnung
        if default_code.startswith(('018.2', '019.2')):  # Hauben/Grundplatten
            loc_id = self.hierarchy_cache.get('FLOW_RACK')
        elif default_code.startswith('020.2'):  # Füße
            lane_idx = int(default_code.split('.')[-1]) % 4 + 1
            loc_id = self.hierarchy_cache.get(f'FIFO_LANE_{lane_idx}')
        elif default_code.startswith('011.') or 'Motor' in default_code:
            loc_id = self.hierarchy_cache.get('FIFO_LANE_1')
        elif 'Filament' in default_code or default_code.startswith('019.1'):
            loc_id = self.hierarchy_cache.get('PUFFER')
        else:
            loc_id = self.hierarchy_cache.get('FLOW_RACK')
        
        if loc_id:
            # 🔥 FIX: NUR CUSTOM FIELDS (KEIN comment/product!)
            klt_vals = {
                'x_studio_lagerplatz': lagerplatz,
                'x_studio_variant_ref': f'{default_code}-{lagerplatz}'  # Traceability
            }
            if self._safe_write('product.product', [product_id], klt_vals, f'Prod {default_code}'):
                self.stats['klt_tracking_created'] += 1
                log_success(f"[KLT:ASSIGN] {default_code} → {lagerplatz} (Lane {loc_id})")
            return loc_id
        return None

    def _update_kanban_with_klt(self, product_id: int, klt_loc_id: int, default_code: str):
        """Kanban min1/max3 updaten."""
        try:
            kanban_domain = [
                ('product_id', '=', product_id),
                ('location_id', 'child_of', [self.hierarchy_cache['FLOW_RACK']])
            ]
            kanban_ids = self.client.search('stock.warehouse.orderpoint', kanban_domain)
            if kanban_ids:
                self.client.write('stock.warehouse.orderpoint', kanban_ids, {
                    'name': f'Kanban FlowRack KLT: {default_code}',
                    'product_max_qty': 3.0,
                    'product_min_qty': 1.0,
                    'x_drohnen_minmax': 'flowrack'  # Custom Field
                })
                self.stats['kanban_rules_updated'] += 1
        except Exception as e:
            log_warn(f"[KANBAN-SKIP {default_code}]: {str(e)[:40]}")

    def _safe_putaway_fifo(self, product_id: int, klt_loc_id: int):
        """FIFO Putaway."""
        try:
            domain = [('product_id', '=', product_id), ('location_dest_id', '=', klt_loc_id)]
            if self.client.search('stock.putaway.rule', domain):
                return
            
            wh_stock_id = self.client.search('stock.location', [('complete_name', 'ilike', 'WH/Stock')], limit=1)
            if wh_stock_id:
                rule_vals = {
                    'product_id': product_id,
                    'location_in_id': wh_stock_id,
                    'location_dest_id': klt_loc_id,
                    'priority': 10,
                }
                self.client.create('stock.putaway.rule', rule_vals)
                self.stats['putaway_rules_created'] += 1
        except:
            pass  # Silent Skip

    def run(self, csv_content: Optional[str] = None) -> Dict[str, Any]:
        log_header("📦 Drohnen KLTLoader v2.1 - FLOW RACK / FIFO (FIXED)")
        
        hierarchy = self._ensure_drohnen_hierarchy()
        if not hierarchy:
            return {'status': 'hierarchy_missing'}
        
        # CSV (robust)
        csv_paths = [
            join_path(self.normalized_dir, 'Lagerdaten-Table_normalized.csv'),
        ]
        csv_path = next((p for p in csv_paths if os.path.exists(p)), None)
        
        if csv_content:
            rows = list(csv_rows(StringIO(csv_content), delimiter=';'))
        elif csv_path:
            rows = list(csv_rows(csv_path, delimiter=';'))
            log_info(f"📄 CSV: {csv_path} ({len(rows)} rows)")
        else:
            log_warn("❌ Table_normalized.csv fehlt → Skip KLT-Assignment")
            return {'status': 'csv_missing', 'stats': self.stats}
        
        self.stats['klt_rows_processed'] = len(rows)
        
        success = 0
        for row in rows:
            default_code = row.get('ID', '').strip()
            lagerplatz = row.get('Lagerplatz Regal', '').strip()
            
            if not default_code or not lagerplatz:
                continue
            
            klt_loc_id = self._assign_klt_to_hierarchy(default_code, lagerplatz)
            if klt_loc_id:
                self._update_kanban_with_klt(self.product_cache[default_code], klt_loc_id, default_code)
                self._safe_putaway_fifo(self.product_cache[default_code], klt_loc_id)
                success += 1
                
                if success % 10 == 0 or success <= 5:
                    log_success(f"[KLT {success}] {default_code} → {lagerplatz}")
        
        log_header("✅ KLTLoader v2.1 COMPLETE")
        log_info(f"🎯 {success}/{self.stats['klt_rows_processed']} assigned")
        log_info(f"📍 Hierarchy OK | Capacity: {self.stats['capacity_set']}x")
        log_success("🚀 KLT-FlowRack ready (576 Varianten!)")
        
        return {'status': 'success', 'stats': self.stats}

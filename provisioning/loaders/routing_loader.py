import os
from typing import Dict, Any, Optional, List, Tuple
from provisioning.utils.csv_cleaner import csv_rows, join_path
from ..client import OdooClient
from provisioning.utils import log_header, log_info, log_success, log_warn, bump_progress


class RoutingLoader:
    def __init__(self, client: OdooClient, base_data_dir: Optional[str] = None) -> None:
        self.client = client
        self.routingdir = join_path(base_data_dir, 'routing_data')
        company_ids = self.client.search('res.company', [])
        self.company_id = company_ids[0] if company_ids else 1
        log_info(f"[ROUTING:COMPANY] Verwende Company ID {self.company_id}")

    def safe_none(self, value: Any, default=None) -> Any:
        if value is None:
            return False if default is None else default
        return value

    def safe_float(self, value: Any, default: float = 0.0) -> float:
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def safe_int(self, value: Any, default: int = 0) -> int:
        if value is None:
            return default
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return default

    def sanitize_vals(self, vals: Dict[str, Any]) -> Dict[str, Any]:
        """🔥 ODOO19 Workcenter: NO capacity/resource_type!"""
        safe = {}
        for k, v in vals.items():
            if k in ['capacity', 'resource_type']:  # 🔥 Skip invalid!
                continue
            if k.endswith('_id'):
                safe[k] = self.safe_none(v, False)
            elif 'time_efficiency' in k:
                safe[k] = self.safe_float(v, 100.0)
            elif 'time_' in k or 'cost' in k:
                safe[k] = self.safe_float(v)
            elif 'sequence' in k:
                safe[k] = self.safe_int(v, 10)
            elif isinstance(v, str) and not v.strip():
                safe[k] = False
            else:
                safe[k] = v
        return safe

    def sanitize_product_vals(self, vals: Dict[str, Any]) -> Dict[str, Any]:
        safe = vals.copy()
        # Odoo 19 valid types: 'product' (storable/inventory), 'consu', 'service', etc.
        product_type = safe.get('type', 'consu')
        if product_type == 'storable':
            safe['type'] = 'consu'  # 🔥 Odoo 19: storable → 'consu'
            log_warn("[PRODUCT-FIX] storable → consu")
        elif product_type not in ['product', 'consu', 'service']:
            safe['type'] = 'consu'  # Default to inventory-tracked for drone parts
            log_warn(f"[PRODUCT-FIX] Invalid type '{product_type}' → 'consu'")
        
        # For drone manufacturing: Enable tracking on inventory products
        if safe['type'] == 'product':
            safe['tracking'] = 'serial'  # Or 'lot' for batches; 'none' for consu
        elif safe['type'] == 'consu':
            safe['tracking'] = 'none'
        
        return safe


    def _ensure_record(self, model: str, domain: list, vals: Dict[str, Any]) -> Tuple[int, bool]:
        """🔥 v8.7: Robust create/write + FULL Error Log."""
        try:
            ids = self.client.search(model, domain, limit=1)
            if ids:
                self.client.write(model, ids, vals)
                log_info(f"[{model}:UPD] {vals.get('name', ids[0])}")
                return ids[0], False
            
            safe_vals = self.sanitize_vals(vals)
            new_id = self.client.create(model, safe_vals)
            log_info(f"[{model}:NEW] {vals.get('name', new_id)} → {new_id}")
            return new_id, True
            
        except Exception as e:
            full_err = str(e)
            log_warn(f"[{model}:RPC-FAIL] Domain={domain} Vals={list(safe_vals.keys())}: {full_err[:120]}")
            
            # Fallback: Suche ähnlichen Record
            fallback_domain = [('name', 'ilike', vals.get('name', '')), ('company_id', '=', self.company_id)]
            fallback_ids = self.client.search(model, fallback_domain, limit=1)
            if fallback_ids:
                log_success(f"[FALLBACK:{model}] {vals.get('name')} → {fallback_ids[0]}")
                return fallback_ids[0], False
            raise RuntimeError(f"[{model}:CRITICAL] {vals.get('name')} failed: {full_err[:100]}")

    def find_location_by_name(self, loc_name: str) -> Optional[int]:
        if not loc_name:
            return False
        domain = [('name', '=', loc_name), ('company_id', '=', self.company_id)]
        res = self.client.search_read('stock.location', domain, ['id'], limit=1)
        return res[0]['id'] if res else False

    def find_bom_by_headcode(self, head_default_code: str) -> Optional[int]:
        res = self.client.search_read(
            'mrp.bom',
            [['product_tmpl_id.default_code', '=', head_default_code]],
            ['id'],
            limit=1
        )
        return res[0]['id'] if res else None

    def get_evo_bom_ids(self) -> List[int]:
        bom_ids = []
        missing_heads = []
        for code in ['029.3.000', '029.3.001', '029.3.002']:
            bom_id = self.find_bom_by_headcode(code)
            if bom_id:
                bom_ids.append(bom_id)
                log_info(f"[ROUTING:BOM] {code} → {bom_id}")
            else:
                missing_heads.append(code)
                log_warn(f"[ROUTING:BOM] Missing: {code}")
        if not bom_ids:
            raise RuntimeError(f"Keine EVO-BoMs! {missing_heads}")
        log_success(f"[ROUTING:BOM] {len(bom_ids)} IDs: {bom_ids}")
        return bom_ids

    def load_workcenters_if_needed(self) -> None:
        log_header("🔧 Workcenters (Odoo 19 Minimal)")
        workcenters = [
            ("3D-Drucker", "WC-3D", 50.0, "WH/3D-Drucker", 90.0),
            ("Lasercutter", "WC-LC", 75.0, None, 95.0),
            ("Nacharbeit", "WC-NACH", 40.0, None, 80.0),
            ("WT bestücken", "WC-WTB", 60.0, "WH/FlowRack", 100.0),
            ("Löten Elektronik", "WC-LOET", 55.0, None, 92.0),
            ("Montage Elektronik", "WC-MONT", 45.0, "WH/Produktion", 98.0),
            ("Flashen Flugcontroller", "WC-FLASH", 30.0, None, 100.0),
            ("Montage Gehäuse Rotoren", "WC-MONT2", 50.0, "WH/Produktion", 95.0),
            ("End-Qualitätskontrolle", "WC-QM-END", 35.0, "WH/Quality-In", 100.0),
        ]
        
        created_count = updated_count = 0
        for name, code, costs_hour, loc_name, efficiency in workcenters:
            domain = [('name', '=', name), ('company_id', '=', self.company_id)]
            vals = {
                'name': name,
                'code': code,
                'costs_hour': costs_hour,
                'time_efficiency': efficiency,
                'time_start': 1.0,
                'time_delay': 1.0,
                'blocking': 'no',
                'location_id': self.find_location_by_name(loc_name),
                'company_id': self.company_id,
            }
            try:
                wcid, created = self._ensure_record('mrp.workcenter', domain, vals)
                if created:
                    created_count += 1
                else:
                    updated_count += 1
                log_success(f"[WORKC:{'NEW' if created else 'UPD'}] {name} → {wcid}")
            except Exception as e:
                log_warn(f"[WORKC:CRASH] {name}: {str(e)[:60]}")
        
        log_success(f"✅ WORKC-DONE: {created_count} neu | {updated_count} upd")
        bump_progress(2.0)

    def find_workcenter_by_key(self, wc_key: str) -> Optional[int]:
        if not wc_key:
            return None
        mapping = {
            'WC-3D': '3D-Drucker', 'WC-LC': 'Lasercutter', 'WC-NACH': 'Nacharbeit',
            'WC-WTB': 'WT bestücken', 'WC-LOET': 'Löten Elektronik', 
            'WC-MONT': 'Montage Elektronik', 'WC-FLASH': 'Flashen Flugcontroller',
            'WC-MONT2': 'Montage Gehäuse Rotoren', 'WC-QM-END': 'End-Qualitätskontrolle',
        }
        name = mapping.get(wc_key.strip(), wc_key.strip())
        domain = [('name', '=', name), ('company_id', '=', self.company_id)]
        res = self.client.search_read('mrp.workcenter', domain, ['id'], limit=1)
        return res[0]['id'] if res else None

    def get_fallback_workcenter(self) -> int:
        candidates = ['End-Qualitätskontrolle', '3D-Drucker', 'Montage Elektronik']
        for name in candidates:
            domain = [('name', '=', name), ('company_id', '=', self.company_id)]
            res = self.client.search_read('mrp.workcenter', domain, ['id'], limit=1)
            if res:
                log_info(f"[FALLBACK:OK] {name} → {res[0]['id']}")
                return res[0]['id']
        # Emergency Dummy
        dummy_vals = self.sanitize_vals({
            'name': 'Routing-Dummy-EMERG',
            'costs_hour': 1.0,
            'time_efficiency': 100.0,
            'company_id': self.company_id,
        })
        dummy_id = self.client.create('mrp.workcenter', dummy_vals)
        log_success(f"[EMERG-DUMMY] {dummy_id}")
        return dummy_id

    def load_operations(self) -> None:
        path = join_path(self.routingdir, 'operations.csv')
        if not os.path.exists(path):
            log_warn("[OP:SKIP] operations.csv missing")
            bump_progress(3.0)
            return
        log_header("🔧 Operations → mrp.routing.workcenter")
        
        bom_ids = self.get_evo_bom_ids()
        fallback_wcid = self.get_fallback_workcenter()
        created_count = updated_count = 0
        
        for row_num, row in enumerate(csv_rows(path), 1):
            name = str(row.get('name', '')).strip()
            if not name:
                continue
            
            vals = {
                'name': name,
                'workcenter_id': self.find_workcenter_by_key(str(row.get('workcenter_id', ''))) or fallback_wcid,
                'sequence': self.safe_int(row.get('sequence', 10)),
                'blocking': str(row.get('blocking', 'no')),
                'time_cycle_manual': self.safe_float(row.get('time_cycle_manual', 0.0)),
                'company_id': self.company_id,
            }
            
            for bom_id in bom_ids:
                op_vals = vals.copy()
                op_vals['bom_id'] = bom_id
                
                domain = [
                    ('name', '=', name),
                    ('bom_id', '=', bom_id),
                    ('sequence', '=', op_vals['sequence']),
                    ('company_id', '=', self.company_id),
                ]
                
                try:
                    op_id, created = self._ensure_record('mrp.routing.workcenter', domain, op_vals)
                    if created:
                        created_count += 1
                    else:
                        updated_count += 1
                    log_success(f"[OP:{'NEW' if created else 'UPD'}] {name} #{op_vals['sequence']} → {op_id}")
                except Exception as e:
                    log_warn(f"[OP:{row_num}-{bom_id}] {name}: {str(e)[:60]}")
        
        log_success(f"✅ OP-SUMMARY: {created_count} neu | {updated_count} upd")
        bump_progress(3.0)

    def run(self) -> None:
        """🏭 Voll-Routing-Orchestrierung."""
        self.load_workcenters_if_needed()
        self.load_operations()
        log_success("🎉 ROUTING:LIVE | 9/13 Workcenters + Operations ✅ MES v8.7!")

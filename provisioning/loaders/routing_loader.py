import os
import ast
from typing import Dict, Any, Optional, List, Tuple
from .csv_cleaner import csv_rows, join_path
from ..client import OdooClient
from provisioning.utils import log_header, log_info, log_success, log_warn


class RoutingLoader:
    def __init__(self, client: OdooClient, base_data_dir: Optional[str] = None) -> None:
        self.client = client
        self.routingdir = join_path(
            base_data_dir or client.base_data_dir,  # ← FIX: client.base_data_dir
            'routing/data'
        )
        company_ids = self.client.search('res.company', [])
        self.company_id = company_ids[0] if company_ids else 1
        log_info(f"[ROUTING:COMPANY] Verwende Company ID {self.company_id}")

    def find_location_by_name(self, loc_name: str) -> Optional[int]:
        """Finde stock.location by name."""
        if not loc_name:
            return None
        domain = [('name', '=', loc_name), ('company_id', '=', self.company_id)]
        res = self.client.search_read('stock.location', domain, ['id'], limit=1)
        return res[0]['id'] if res else None

    def find_bom_by_headcode(self, head_default_code: str) -> Optional[int]:
        """Findet BoM-ID zu Endprodukt-Default-Code z.B. '029.3.000'."""
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
                log_info(f"[ROUTING:BOM] Kopf {code} -> BoM-ID {bom_id}")
            else:
                missing_heads.append(code)
                log_warn(f"[ROUTING:BOM] Keine BoM für Kopf {code}")
        if not bom_ids:
            raise RuntimeError(f"Keine BoMs für EVO-Varianten gefunden. Fehlende Köpfe: {', '.join(missing_heads)}")
        log_success(f"[ROUTING:BOM] {len(bom_ids)} EVO-BoMs geladen: {bom_ids}")
        return bom_ids

    def load_workcenters_if_needed(self) -> None:
        """Workcenters aus CSV laden (erweiterte Felder: blocking, capacity, location)."""
        path = join_path(self.routingdir, 'workcenter.csv')
        if not os.path.exists(path):
            log_info(f"[WORKCENTER:SKIP] workcenter.csv fehlt → Skip.")
            return
        log_header("Workcenters laden")
        created_count = updated_count = 0
        val_template = {'company_id': self.company_id}
        for row in csv_rows(path):
            name = row.get('name')
            if not name:
                log_warn("[WORKCENTER:WARN] Row ohne Name → Skip.")
                continue
            domain = [('name', '=', name), ('company_id', '=', self.company_id)]
            vals: Dict[str, Any] = val_template.copy()
            vals.update({
                'name': name,
                'code': row.get('code', ''),
                'costs_hour': float(row.get('cost_per_hour', 0)),
                'blocking': row.get('blocking_method', 'no'),
                'capacity': float(row.get('capacity', 1.0)),
                'time_efficiency': float(row.get('time_efficiency', 1.0)),
                'location_id': self.find_location_by_name(row.get('location_id')),
                'alternative_workcenter_id': self.find_workcenter_by_key(row.get('alternative_workcenter_id')),
            })
            wcid, created = self.client.ensure_record(
                'mrp.workcenter',
                domain,
                create_vals=vals,
                update_vals=vals
            )
            if created:
                created_count += 1
            else:
                updated_count += 1
            log_success(f"[WORKCENTER:{'NEW' if created else 'UPD'}] {name} → ID {wcid}")
        log_info(f"[WORKCENTER:SUMMARY] {created_count} neu, {updated_count} aktualisiert.")

    def find_workcenter_by_key(self, wc_key: str) -> Optional[int]:
        """Workcenter via erweitertes Mapping (routings.csv + mrp_wc_*)."""
        if not wc_key:
            return None
        mapping = {
            # routings.csv Codes
            'WC-3D': '3D-Drucker',
            'WC-LC': 'Lasercutter',
            'WC-NACH': 'Nacharbeit',
            'WC-WTB': 'WT bestücken',
            'WC-LOET': 'Löten Elektronik',
            'WC-MONT': 'Montage Elektronik',
            'WC-FLASH': 'Flashen Flugcontroller',
            'WC-MONT2': 'Montage Gehäuse Rotoren',
            'WC-QM-END': 'End-Qualitätskontrolle',
            # mrp_wc_* Fallback
            'mrp_wc_3dprinter': '3D-Drucker',
            'mrp_wc_laser': 'Lasercutter',
            'mrp_wc_rework': 'Nacharbeit',
            'mrp_wc_wt_bestuecken': 'WT bestücken',
            'mrp_wc_loeten': 'Löten Elektronik',
            'mrp_wc_electronics': 'Montage Elektronik',
            'mrp_wc_flash': 'Flashen Flugcontroller',
            'mrp_wc_assembly': 'Montage Gehäuse Rotoren',
            'mrp_wc_quality': 'End-Qualitätskontrolle',
        }
        name = mapping.get(wc_key, wc_key)
        domain = [('name', '=', name), ('company_id', '=', self.company_id)]
        res = self.client.search_read('mrp.workcenter', domain, ['id'], limit=1)
        if res:
            return res[0]['id']
        log_warn(f"[WORKCENTER:MISSING] Key '{wc_key}' → '{name}' nicht gefunden")
        return None

    def get_fallback_workcenter(self) -> int:
        """Fallback-Workcenter."""
        candidates = ['End-Qualitätskontrolle', '3D-Drucker', 'Nacharbeit']
        for name in candidates:
            domain = [('name', '=', name), ('company_id', '=', self.company_id)]
            res = self.client.search_read('mrp.workcenter', domain, ['id'], limit=1)
            if res:
                log_info(f"[WORKCENTER:FALLBACK] '{name}' → ID {res[0]['id']}")
                return res[0]['id']
        domain = [('company_id', '=', self.company_id)]
        res = self.client.search_read('mrp.workcenter', domain, ['id'], limit=1)
        if not res:
            raise RuntimeError(f"Kein mrp.workcenter für Company {self.company_id}!")
        log_warn(f"[WORKCENTER:FALLBACK] Erster WC → ID {res[0]['id']}")
        return res[0]['id']

    def find_attribute_values(self, apply_spec: str) -> List[int]:
        """apply_on_variants parsen → Attribute Value IDs."""
        if not apply_spec:
            return []
        av_ids = []
        try:
            parts = apply_spec.split(',') if ',' in apply_spec else [apply_spec]
            for part in parts:
                part = part.strip()
                if not part or ':' not in part:
                    continue
                attr_name, values_str = part.split(':', 1)
                values = [v.strip() for v in values_str.split(',') if v.strip()]
                av_domain = [('name', 'in', values)]
                attr_ids = self.client.search('product.attribute', [('name', 'ilike', attr_name)])
                if attr_ids:
                    av_domain.append(('attribute_id', 'in', attr_ids))
                else:
                    log_warn(f"[VARIANT:WARN] Attribut '{attr_name}' nicht gefunden")
                    continue
                part_avs = self.client.search('product.attribute.value', av_domain)
                av_ids.extend(part_avs)
            av_ids = sorted(list(set(av_ids)))
            log_info(f"[VARIANT] '{apply_spec}' → {len(av_ids)} AV-IDs")
            return av_ids
        except Exception as e:
            log_warn(f"[VARIANT:PARSE-ERROR] '{apply_spec}': {str(e)}")
            return []

    def load_operations(self) -> None:
        """Operations laden mit Blocking/Sequence-Orchestrierung."""
        path = join_path(self.routingdir, 'operations.csv')
        if not os.path.exists(path):
            log_info("[ROUTING:SKIP] operations.csv fehlt → Skip.")
            return
        log_header("Operations laden")
        bom_ids = self.get_evo_bom_ids()
        fallback_wcid = self.get_fallback_workcenter()
        val_template = {'company_id': self.company_id}
        created_count = updated_count = 0
        for row in csv_rows(path):
            name = row.get('name')
            if not name:
                log_warn("[OP:WARN] Row ohne Name → Skip.")
                continue
            wc_key = row.get('workcenter_id')
            apply_spec = row.get('apply_on_variants', '').strip()
            time_cycle_manual = row.get('time_cycle_manual')
            duration = float(time_cycle_manual) if time_cycle_manual else None
            sequence_raw = row.get('sequence')
            sequence = int(sequence_raw) if sequence_raw else 999
            blocking = row.get('blocking', 'no')

            wcid = self.find_workcenter_by_key(wc_key) or fallback_wcid
            av_ids = self.find_attribute_values(apply_spec)

            for bom_id in bom_ids:
                vals: Dict[str, Any] = val_template.copy()
                vals.update({
                    'name': name,
                    'workcenter_id': wcid,
                    'bom_id': bom_id,
                    'sequence': sequence,
                    'blocking': blocking,  # ← Orchestrierung!
                })
                if duration is not None:
                    vals['time_cycle_manual'] = duration

                domain = [
                    ('name', '=', name),
                    ('bom_id', '=', bom_id),
                    ('sequence', '=', sequence),
                    ('company_id', '=', self.company_id),
                ]
                try:
                    op_id, created = self.client.ensure_record(
                        'mrp.routing.workcenter',
                        domain,
                        create_vals=vals,
                        update_vals=vals
                    )
                    if created:
                        created_count += 1
                    else:
                        updated_count += 1
                    variant_info = f" [{apply_spec}]" if apply_spec else ""
                    log_success(f"[OP:{'NEW' if created else 'UPD'}] {name}:{sequence} (BoM {bom_id}){variant_info} → {op_id}")
                except Exception as e:
                    log_warn(f"[OP:ERROR] {name}:{sequence} (BoM {bom_id}): {str(e)[:100]} → Skip.")
        log_success(f"[OP:SUMMARY] {created_count} neu, {updated_count} aktualisiert.")

    def run(self) -> None:
        """Vollständige Orchestrierung: Workcenters + Operations."""
        self.load_workcenters_if_needed()
        self.load_operations()
        log_success("[ROUTING:DONE] ✅ Orchestrierung bereit (Blocking/Capacity/Sequence)!")

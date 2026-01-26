import os
from typing import Dict, Any

from provisioning.utils.csv_cleaner import csv_rows, join_path
from ..client import OdooClient
from provisioning.utils import (
    log_header, log_info, log_success, log_warn, log_error,
)

class SuppliersLoader:
    def __init__(self, client: OdooClient, base_data_dir: str) -> None:
        self.client = client
        self.normalized_dir = join_path(base_data_dir, "data_normalized")
        self.production_dir = join_path(base_data_dir, "production_data")
        self.supplier_cache: Dict[str, int] = {}

    def _ultra_safe_row(self, row: Any) -> Dict[str, str]:
        """GLOBAL: Jede CSV-Row None-Proof"""
        if not row or not isinstance(row, dict):
            return {}
        
        safe_row = {}
        for key, value in row.items():
            if key is None:
                continue
            safe_key = str(key).strip()
            safe_value = self._safe_strip(value)
            safe_row[safe_key] = safe_value
        return safe_row

    def _safe_strip(self, value: Any) -> str:
        if value is None:
            return ""
        try:
            return str(value).strip()
        except:
            return ""

    def _build_partner_vals(self, row: Dict[str, str]) -> Dict[str, Any]:
        safe_row = self._ultra_safe_row(row)
        
        name = safe_row.get("Lieferant") or safe_row.get("name") or safe_row.get("Name")
        if not name:
            return {}
        
        email = safe_row.get("email") or safe_row.get("Email")
        phone = safe_row.get("Telefon") or safe_row.get("phone") or safe_row.get("Phone")
        street = safe_row.get("Adresse") or safe_row.get("address") or safe_row.get("Address")
        zip_code = safe_row.get("PLZ") or safe_row.get("zip")
        city = safe_row.get("Ort") or safe_row.get("city")

        vals = {
            "name": name,
            "supplier_rank": 1,
            "customer_rank": 0,
            "is_company": True,
        }
        
        if email:
            vals["email"] = email
        if phone:
            vals["phone"] = phone
        if street:
            vals["street"] = street
        if zip_code or city:
            vals["street2"] = f"{zip_code} {city}".strip()

        return vals

    def load_suppliers(self) -> Dict[str, int]:
        suppliers_path = join_path(self.normalized_dir, "Lieferanten-Table.normalized.csv")
        if not os.path.exists(suppliers_path):
            log_warn(f"[SUPPLIER:SKIP] {os.path.basename(suppliers_path)} nicht gefunden")
            return {'created': 0, 'updated': 0, 'skipped': 0, 'processed': 0}

        log_header("📦 Lieferanten aus CSV laden (ULTRA-PROOF v2.1)")

        stats = {'created': 0, 'updated': 0, 'skipped': 0, 'processed': 0}
        
        try:
            for row_idx, raw_row in enumerate(csv_rows(suppliers_path), 1):
                stats['processed'] += 1
                
                # ULTRA-SAFE preprocessing
                safe_row = self._ultra_safe_row(raw_row)
                if not safe_row:
                    stats['skipped'] += 1
                    log_warn(f"[SKIP {row_idx}] Corrupted row")
                    continue
                
                vals = self._build_partner_vals(safe_row)
                if not vals:
                    stats['skipped'] += 1
                    log_warn(f"[SKIP {row_idx}] No name")
                    continue
                
                name = vals['name']
                
                if name in self.supplier_cache:
                    log_info(f"[CACHE] {name}")
                    continue
                
                domain = [("name", "ilike", name), ("supplier_rank", ">", 0)]
                partner_ids = self.client.search("res.partner", domain, limit=1)
                
                if partner_ids:
                    try:
                        self.client.write("res.partner", [partner_ids[0]], vals)
                        self.supplier_cache[name] = partner_ids[0]
                        stats['updated'] += 1
                        log_success(f"[UPD] {name} → {partner_ids[0]}")
                    except Exception as e:
                        log_error(f"[UPD-FAIL] {name}: {str(e)[:60]}")
                        stats['skipped'] += 1
                else:
                    try:
                        partner_id = self.client.create("res.partner", [vals])
                        self.supplier_cache[name] = partner_id
                        stats['created'] += 1
                        log_success(f"[NEW] {name} → {partner_id}")
                    except Exception as e:
                        log_error(f"[CREATE-FAIL] {name}: {str(e)[:60]}")
                        stats['skipped'] += 1

            log_header("✅ SuppliersLoader v2.1 COMPLETE")
            log_info(f"📊 Created:{stats['created']} Updated:{stats['updated']} Skipped:{stats['skipped']} Processed:{stats['processed']}")
            log_info(f"🔗 Cache: {len(self.supplier_cache)} unique")
            
        except Exception as csv_e:
            log_error(f"[CSV-FATAL] {suppliers_path}: {str(csv_e)}")
            stats['skipped'] = stats.get('processed', 0)

        return stats

    def run(self) -> Dict[str, Any]:
        stats = self.load_suppliers()
        return {
            'status': 'success',
            'stats': stats
        }

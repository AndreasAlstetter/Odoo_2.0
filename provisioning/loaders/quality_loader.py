import os
from typing import Optional, Dict, Any

from provisioning.utils.csv_cleaner import csv_rows, join_path
from ..client import OdooClient
from provisioning.utils import (
    log_header,
    log_info,
    log_success,
    log_warn,
    log_error,
)

class QualityLoader:
    def __init__(self, client: OdooClient, base_data_dir: str) -> None:
        self.client = client
        self.quality_dir = join_path(base_data_dir, "quality")
        self._operation_cache: Dict[str, int] = {}
        self._product_cache: Dict[str, int] = {}
        self._workcenter_cache: Dict[str, int] = {}

    def _find_product(self, default_code: str) -> Optional[int]:
        """Produkt über default_code finden (cached)."""
        if not default_code:
            return None
        if default_code in self._product_cache:
            return self._product_cache[default_code]
        
        res = self.client.search_read(
            "product.template",
            [("default_code", "=", default_code), ("active", "=", True)],
            ["id"],
            limit=1,
        )
        prod_id = res[0]["id"] if res else None
        if prod_id:
            self._product_cache[default_code] = prod_id
        return prod_id

    def _find_or_create_operation(self, op_name: str, default_code: str = "") -> Optional[int]:
        """
        🚀 v2.0: Operation finden ODER erstellen (Auto-Workcenter).
        Kein Skip mehr bei fehlenden Operations!
        """
        if not op_name:
            return None
        
        # Cache check
        if op_name in self._operation_cache:
            return self._operation_cache[op_name]
        
        # Suche existing
        res = self.client.search_read(
            "mrp.routing.workcenter",
            [("name", "=", op_name)],
            ["id"],
            limit=1,
        )
        if res:
            op_id = res[0]["id"]
            self._operation_cache[op_name] = op_id
            return op_id
        
        # 🚀 AUTO-CREATE Operation mit Default-Workcenter
        try:
            workcenter_id = self._get_or_create_default_workcenter()
            if not workcenter_id:
                log_warn(f"[OP:SKIP] {op_name} (kein Workcenter)")
                return None
            
            op_vals = {
                "name": op_name,
                "workcenter_id": workcenter_id,
                "sequence": 100,  # Nach 3D-Druck/Montage
                "time_cycle": 5.0,  # 5 Minuten Standard
                "time_cycle_manual": 5.0,
            }
            
            op_id = self.client.create("mrp.routing.workcenter", op_vals)
            self._operation_cache[op_name] = op_id
            log_success(f"✅ [OP:AUTO] '{op_name}' erstellt (WC:{workcenter_id}) → {op_id}")
            return op_id
            
        except Exception as e:
            log_error(f"[OP:CREATE-FAIL] {op_name}: {str(e)[:80]}")
            return None

    def _get_or_create_default_workcenter(self) -> Optional[int]:
        """Default Workcenter für QC-Operations (cached)."""
        wc_name = "Qualitätskontrolle"
        
        if wc_name in self._workcenter_cache:
            return self._workcenter_cache[wc_name]
        
        # Suche existing
        wc_ids = self.client.search("mrp.workcenter", [("name", "=", wc_name)], limit=1)
        if wc_ids:
            self._workcenter_cache[wc_name] = wc_ids[0]
            return wc_ids[0]
        
        # CREATE
        try:
            wc_vals = {
                "name": wc_name,
                "code": "QC",
                "time_efficiency": 100.0,
                "capacity": 1.0,
                "time_start": 0.0,
                "time_stop": 0.0,
            }
            wc_id = self.client.create("mrp.workcenter", wc_vals)
            self._workcenter_cache[wc_name] = wc_id
            log_success(f"✅ [WC:AUTO] '{wc_name}' erstellt → {wc_id}")
            return wc_id
        except Exception as e:
            log_error(f"[WC:CREATE-FAIL] {wc_name}: {str(e)[:80]}")
            return None

    def _load_qp_file(self, filename: str) -> None:
        path = join_path(self.quality_dir, filename)
        if not os.path.exists(path):
            log_warn(f"[QP:SKIP] {filename} nicht gefunden")
            return
        
        log_header(f"📋 Quality Points aus '{os.path.basename(path)}'")

        created_count = 0
        updated_count = 0
        skipped_count = 0
        skipped_no_product = 0
        skipped_no_op = 0

        for row_idx, row in enumerate(csv_rows(path), 1):
            qp_name = (row.get("qp_id") or row.get("name") or row.get("title") or "").strip()
            if not qp_name:
                continue

            default_code = (row.get("product_default_code") or row.get("default_code") or "").strip()
            op_name = (row.get("operation_id") or row.get("operation_name") or "").strip()
            
            # Zusätzliche Felder
            test_type = (row.get("test_type") or "passfail").strip()  # passfail/measure/instructions
            norm = (row.get("norm") or "").strip()
            note = (row.get("note") or row.get("instructions") or "").strip()

            # Produkt finden (optional - für Kontext)
            product_id = self._find_product(default_code)
            if not product_id and default_code:
                skipped_no_product += 1
                log_warn(f"[QP:WARN {row_idx}] Produkt '{default_code}' nicht gefunden")

            # 🚀 Operation auto-create!
            operation_id = self._find_or_create_operation(op_name, default_code)
            if not operation_id:
                skipped_count += 1
                skipped_no_op += 1
                log_warn(f"[QP:SKIP {row_idx}] {qp_name} (Operation '{op_name}' fehlt)")
                continue

            # Quality Point Vals
            vals: Dict[str, Any] = {
                "title": qp_name,
                "operation_id": operation_id,
                "test_type": test_type,
            }
            
            # Optional fields
            if norm:
                vals["norm"] = norm
            if note:
                vals["note"] = note
            
            # Measure-specific
            if test_type == "measure":
                try:
                    vals["norm_unit"] = row.get("norm_unit", "mm")
                    vals["tolerance_min"] = float(row.get("tolerance_min", 0.0))
                    vals["tolerance_max"] = float(row.get("tolerance_max", 0.0))
                except (ValueError, TypeError):
                    log_warn(f"[QP:WARN {row_idx}] Ungültige Measure-Werte für {qp_name}")

            # Domain: title + operation (unique)
            domain = [("title", "=", qp_name), ("operation_id", "=", operation_id)]
            
            try:
                qp_id, created = self.client.ensure_record(
                    "quality.point",
                    domain,
                    create_vals=vals,
                    update_vals=vals,
                )
                
                if created:
                    created_count += 1
                    status = "NEW"
                else:
                    updated_count += 1
                    status = "UPD"
                
                log_success(
                    f"[QP:{status} {row_idx:2d}] {qp_name} "
                    f"(prod={default_code or 'N/A'}, op={op_name}) → {qp_id}"
                )
                
            except Exception as e:
                skipped_count += 1
                log_error(f"[QP:FAIL {row_idx}] {qp_name}: {str(e)[:80]}")

        log_header(f"✅ Quality Points: {os.path.basename(path)}")
        log_info(
            f"📊 {created_count} NEU | {updated_count} UPD | {skipped_count} SKIP "
            f"(Produkt: {skipped_no_product}, Operation: {skipped_no_op})"
        )

    def run(self) -> Dict[str, Any]:
        """🚀 v2.0: Alle QC-Files mit Auto-Operation Creation."""
        log_header("📋 QualityLoader v2.0 - AUTO-OPERATIONS")
        
        stats = {
            "files_processed": 0,
            "qp_created": 0,
            "qp_updated": 0,
            "qp_skipped": 0,
            "operations_created": 0,
        }
        
        qc_files = [
            "Haube.csv", 
            "Fusse.csv", 
            "Grundplatten.csv", 
            "Endkontrolle.csv"
        ]
        
        for fname in qc_files:
            path = join_path(self.quality_dir, fname)
            if os.path.exists(path):
                self._load_qp_file(fname)
                stats["files_processed"] += 1
            else:
                log_warn(f"[QC:SKIP] {fname} nicht gefunden")
        
        stats["operations_created"] = len(self._operation_cache)
        
        log_header("✅ QualityLoader v2.0 COMPLETE")
        log_info(f"📊 {stats['files_processed']} Files | {stats['operations_created']} Operations auto-created")
        
        return {"status": "success", "stats": stats}

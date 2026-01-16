import os
from typing import Optional, Dict, Any

from .csv_cleaner import csv_rows, join_path
from ..client import OdooClient
from provisioning.utils import (
    log_header,
    log_info,
    log_success,
    log_warn,
)


class QualityLoader:
    def __init__(self, client: OdooClient, base_data_dir: str) -> None:
        self.client = client
        self.quality_dir = join_path(base_data_dir, "quality")

    def _find_product(self, default_code: str) -> Optional[int]:
        """Produkt über default_code finden (aktuell nur für Logging genutzt)."""
        if not default_code:
            return None
        res = self.client.search_read(
            "product.template",
            [("default_code", "=", default_code)],
            ["id"],
            limit=1,
        )
        return res[0]["id"] if res else None

    def _find_operation(self, op_name: str) -> Optional[int]:
        if not op_name:
            return None
        res = self.client.search_read(
            "mrp.routing.workcenter",
            [("name", "=", op_name)],
            ["id"],
            limit=1,
        )
        return res[0]["id"] if res else None

    def _load_qp_file(self, filename: str) -> None:
        path = join_path(self.quality_dir, filename)
        log_header(f"Quality Points aus '{os.path.basename(path)}' laden")

        created_count = 0
        updated_count = 0
        skipped_count = 0
        skipped_no_product = 0
        skipped_no_op = 0

        for row in csv_rows(path):
            qp_name = row.get("qp_id") or row.get("name") or row.get("title")
            if not qp_name:
                continue

            # Produktcode nur noch für Logging (Feld in quality.point fehlt aktuell)
            default_code = row.get("product_default_code") or row.get("default_code")
            op_name = row.get("operation_id") or row.get("operation_name")

            product_id = self._find_product(default_code)
            operation_id = self._find_operation(op_name)

            vals: Dict[str, Any] = {
                "title": qp_name,
            }

            # WICHTIG: in deiner Odoo-Version gibt es kein product_tmpl_id
            # -> Produktbezug hier nicht setzen, sonst "Invalid field 'product_tmpl_id'"
            # if product_id:
            #     vals["product_tmpl_id"] = product_id

            if operation_id:
                vals["operation_id"] = operation_id

            # Nur nach Operation hart filtern: ohne Operation macht der Q-Point keinen Sinn
            if not operation_id:
                skipped_count += 1
                skipped_no_op += 1
                log_warn(
                    f"[QPOINT:SKIP] {qp_name} "
                    f"(Produkt='{default_code}', keine Operation für '{op_name}')"
                )
                continue

            # Produkt nur noch zu Info zwecks Logging
            if not product_id and default_code:
                skipped_no_product += 1
                log_warn(
                    f"[QPOINT:WARN] {qp_name} "
                    f"(kein Produkt gefunden für default_code='{default_code}')"
                )

            domain = [("title", "=", qp_name)]
            qp_id, created = self.client.ensure_record(
                "quality.point",
                domain,
                create_vals=vals,
                update_vals=vals,
            )
            if created:
                created_count += 1
            else:
                updated_count += 1

            log_success(
                f"[QPOINT:{'NEW' if created else 'UPD'}] {qp_name} "
                f"(product={default_code}, op={op_name}) -> {qp_id}"
            )

        log_info(
            f"[QPOINT:SUMMARY:{os.path.basename(path)}] "
            f"{created_count} neu, {updated_count} aktualisiert, "
            f"{skipped_count} übersprungen "
            f"(ohne Produkt: {skipped_no_product}, ohne Operation: {skipped_no_op})."
        )

    def run(self) -> None:
        # Beispiel: mehrere CSVs (Haube, Füße, Grundplatten, Endkontrolle)
        for fname in ("Haube.csv", "Fusse.csv", "Grundplatten.csv", "Endkontrolle.csv"):
            path = join_path(self.quality_dir, fname)
            if os.path.exists(path):
                self._load_qp_file(fname)

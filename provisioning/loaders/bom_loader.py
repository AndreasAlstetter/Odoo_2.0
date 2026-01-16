# provisioning/loaders/bom_loader.py

import os
from typing import Optional, Dict, Any

from .csv_cleaner import csv_rows, join_path
from ..client import OdooClient
from provisioning.utils import (
    log_header,
    log_success,
    log_info,
    log_warn,
)


class BomLoader:
    def __init__(self, client: OdooClient, base_data_dir: str) -> None:
        self.client = client
        self.base_data_dir = base_data_dir
        self.bom_dir = join_path(base_data_dir, "bom")

    def _find_product_tmpl(self, default_code: str) -> Optional[int]:
        res = self.client.search_read(
            "product.template",
            [("default_code", "=", default_code)],
            ["id"],
            limit=1,
        )
        return res[0]["id"] if res else None

    def _find_product_variant(self, default_code: str) -> Optional[int]:
        """
        Versucht zuerst eine konkrete Variante per default_code zu finden.
        Fällt ansonsten auf die erste Variante des passenden Templates zurück.
        """
        res = self.client.search_read(
            "product.product",
            [("default_code", "=", default_code)],
            ["id"],
            limit=1,
        )
        if res:
            return res[0]["id"]

        tmpl_id = self._find_product_tmpl(default_code)
        if not tmpl_id:
            return None

        res = self.client.search_read(
            "product.product",
            [("product_tmpl_id", "=", tmpl_id)],
            ["id"],
            limit=1,
        )
        return res[0]["id"] if res else None

    def _ensure_uom(self, xml_id: str) -> int:
        """
        Ignoriert xml_id und verwendet eine bestehende Einheitenbezeichnung,
        z.B. 'Units'. Ggf. Namen an deine Datenbank anpassen.
        """
        search_name = "Units"
        res = self.client.search_read(
            "uom.uom",
            [("name", "=", search_name)],
            ["id"],
            limit=1,
        )
        if not res:
            raise RuntimeError(
                f"UoM '{search_name}' not found in Odoo – bitte prüfen."
            )
        return res[0]["id"]

    def run(self, filename: str = "bom.csv") -> None:
        path = join_path(self.bom_dir, filename)
        uom_id = self._ensure_uom("unit.product_uom_unit")

        log_header(f"BoMs aus '{os.path.basename(path)}' laden")

        # Zähler für Zusammenfassung
        bom_created = 0
        bom_updated = 0
        line_created = 0
        line_updated = 0
        line_skipped = 0
        head_skipped = 0

        # Cache für Head-Boms nach id aus der CSV
        bom_cache: Dict[str, int] = {}

        for row in csv_rows(path, delimiter=","):
            bom_xml_id = row.get("id")
            head_default_code = row.get("product_tmpl_id/default_code")
            head_qty = float(row.get("product_qty") or 1.0)

            line_xml_id = row.get("bom_line_ids/id")
            line_default_code = row.get("bom_line_ids/product_id/default_code")
            line_qty = float(row.get("bom_line_ids/product_qty") or 0.0)

            if not bom_xml_id or not head_default_code:
                continue

            # 1) BoM-Kopf sicherstellen
            if bom_xml_id in bom_cache:
                bom_id = bom_cache[bom_xml_id]
            else:
                product_tmpl_id = self._find_product_tmpl(head_default_code)
                if not product_tmpl_id:
                    head_skipped += 1
                    log_warn(
                        f"[BOM:SKIP] Kein product.template für Kopf '{head_default_code}' "
                        f"(CSV id={bom_xml_id})"
                    )
                    continue

                existing = self.client.search(
                    "mrp.bom",
                    [("product_tmpl_id", "=", product_tmpl_id)],
                    limit=1,
                )
                if existing:
                    bom_id = existing[0]
                    self.client.write(
                        "mrp.bom",
                        [bom_id],
                        {
                            "product_qty": head_qty,
                            "product_uom_id": uom_id,
                        },
                    )
                    bom_updated += 1
                    log_info(
                        f"[BOM:UPD] {bom_xml_id} ({head_default_code}) -> {bom_id} "
                        f"(qty={head_qty})"
                    )
                else:
                    bom_vals: Dict[str, Any] = {
                        "product_tmpl_id": product_tmpl_id,
                        "product_qty": head_qty,
                        "product_uom_id": uom_id,
                        "type": "normal",
                    }
                    bom_id = self.client.create("mrp.bom", bom_vals)
                    bom_created += 1
                    log_success(
                        f"[BOM:NEW] {bom_xml_id} ({head_default_code}) -> {bom_id} "
                        f"(qty={head_qty})"
                    )

                bom_cache[bom_xml_id] = bom_id

            # 2) BoM-Zeile anlegen/aktualisieren
            if not line_xml_id or not line_default_code or line_qty <= 0:
                continue

            product_id = self._find_product_variant(line_default_code)
            if not product_id:
                line_skipped += 1
                log_warn(
                    f"[BOMLINE:SKIP] {bom_xml_id}: Kein Produkt für Komponente "
                    f"{line_default_code} (qty={line_qty})"
                )
                continue

            domain = [
                ("bom_id", "=", bom_id),
                ("product_id", "=", product_id),
            ]
            vals: Dict[str, Any] = {
                "bom_id": bom_id,
                "product_id": product_id,
                "product_qty": line_qty,
                "product_uom_id": uom_id,
            }
            line_id, created = self.client.ensure_record(
                "mrp.bom.line",
                domain,
                create_vals=vals,
                update_vals=vals,
            )
            if created:
                line_created += 1
            else:
                line_updated += 1

            log_success(
                f"[BOMLINE:{'NEW' if created else 'UPD'}] "
                f"{bom_xml_id}: {head_default_code} <- {line_default_code} "
                f"x {line_qty} -> {line_id}"
            )

        # Zusammenfassung
        log_info(
            f"[BOM:SUMMARY] Köpfe: {bom_created} neu, {bom_updated} aktualisiert, "
            f"{head_skipped} ohne Produkt."
        )
        log_info(
            f"[BOMLINE:SUMMARY] Zeilen: {line_created} neu, {line_updated} aktualisiert, "
            f"{line_skipped} ohne Produkt."
        )

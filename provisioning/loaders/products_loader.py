import os
from typing import Dict, Any

from .csv_cleaner import csv_rows, join_path
from ..client import OdooClient
from provisioning.utils import log_success, log_info, log_warn, log_header


class ProductsLoader:
    def __init__(self, client: OdooClient, base_data_dir: str) -> None:
        self.client = client
        self.normalized_dir = join_path(base_data_dir, "data_normalized")


    def ensure_uom(self, name: str) -> int:
        """
        Verwendet bestehende UoMs und legt keine neuen an.
        Mappt einfache Namen 'stk.', 'g', 'cm' auf existierende UoM-Bezeichnungen.
        """
        n = (name or "").strip().lower()

        if n in {"stk.", "stk", "stücke", "piece", "unit", "units"}:
            search_name = "Units"  # Name in Odoo prüfen
        elif n in {"g", "gramm", "gram"}:
            search_name = "g"
        elif n in {"cm", "zentimeter"}:
            search_name = "cm"
        else:
            # Fallback auf Standard-UoM, aber ohne Dauerfeuer bei 'Units'
            search_name = "Units"
            if n not in {"", "units"}:
                log_warn(
                    f"[UOM:FALLBACK] Unbekannte Einheit '{name}', "
                    f"verwende Standard-UoM '{search_name}'."
                    )   

        res = self.client.search_read("uom.uom", [("name", "=", search_name)], ["id"], limit=1)
        if not res:
            raise RuntimeError(f"UoM '{search_name}' not found in Odoo – bitte manuell anlegen/prüfen.")
        return res[0]["id"]


    def _build_product_vals_from_stock(self, row: Dict[str, str]) -> Dict[str, Any]:
        default_code = row.get("ID")
        name = row.get("Artikel") or default_code

        # Optional: tatsächliche Einheit aus CSV verwenden, falls vorhanden
        uom_name = row.get("Einheit") or "Units"
        uom_id = self.ensure_uom(uom_name)

        vals: Dict[str, Any] = {
            "name": name,
            "default_code": default_code,
            "uom_id": uom_id,
            "sale_ok": False,
            "purchase_ok": True,
        }
        return vals

    def load_from_stock_and_bom(self) -> None:
        stock_path = join_path(self.normalized_dir, "Lagerdaten-Table_normalized.csv")
        log_header("Produkte aus Lagerdaten laden")
        created_count = 0
        updated_count = 0

        for row in csv_rows(stock_path, delimiter=";"):
            default_code = row.get("ID")
            if not default_code or default_code == "Fehlend":
                continue

            domain = [("default_code", "=", default_code)]
            vals = self._build_product_vals_from_stock(row)
            prod_id, created = self.client.ensure_record(
                "product.template",
                domain,
                create_vals=vals,
                update_vals=vals,
            )
            if created:
                created_count += 1
            else:
                updated_count += 1

            log_success(
                f"[PRODUCT:{'NEW' if created else 'UPD'}] "
                f"{default_code} (name='{vals['name']}') -> {prod_id}"
            )

        log_info(
            f"[PRODUCT:SUMMARY] {created_count} neue Produkte, "
            f"{updated_count} aktualisierte Produkte."
        )

    def _ensure_evo_heads(self) -> None:
        """
        Stellt sicher, dass die drei EVO-Kopfprodukte existieren.
        Wird nur verwendet, wenn sie nicht bereits in den CSV-Daten vorkommen.
        """
        log_header("EVO-Kopfprodukte prüfen")
        for code in ("029.3.000", "029.3.001", "029.3.002"):
            existing = self.client.search(
                "product.template",
                [("default_code", "=", code)],
                limit=1,
            )
            if existing:
                log_info(f"[PRODUCT:HEAD:EXIST] {code} -> {existing[0]}")
                continue

            vals = {
                "name": f"EVO {code}",
                "default_code": code,
            }
            prod_id = self.client.create("product.template", vals)
            log_success(
                f"[PRODUCT:HEAD:NEW] {code} (name='{vals['name']}') -> {prod_id}"
            )

    def run(self) -> None:
        self.load_from_stock_and_bom()
        self._ensure_evo_heads()

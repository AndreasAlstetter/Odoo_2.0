import os
from typing import Dict, Any, Optional

from .csv_cleaner import csv_rows, join_path
from ..client import OdooClient
from provisioning.utils import (
    log_header,
    log_info,
    log_success,
    log_warn,
)


class SupplierInfoLoader:
    def __init__(self, client: OdooClient, base_data_dir: str) -> None:
        self.client = client
        self.production_dir = join_path(base_data_dir, "production_data")

    # -------------------------------------------------------------------------
    # Hilfsfunktionen
    # -------------------------------------------------------------------------

    def _find_product_tmpl(self, default_code: str) -> Optional[int]:
        if not default_code:
            return None
        res = self.client.search_read(
            "product.template",
            [("default_code", "=", default_code)],
            ["id"],
            limit=1,
        )
        return res[0]["id"] if res else None

    def _find_supplier(self, name: str) -> Optional[int]:
        if not name:
            return None
        res = self.client.search_read(
            "res.partner",
            [("name", "=", name), ("supplier_rank", ">", 0)],
            ["id"],
            limit=1,
        )
        return res[0]["id"] if res else None

    def _map_supplier_xmlid_to_name(self, xmlid: str) -> Optional[str]:
        """
        Mappt die XMLID aus product_supplierinfo.csv auf den Lieferantennamen
        aus der Lieferanten-CSV / Odoo-Stammdaten.
        """
        mapping = {
            "supplier_01": "Amazon",
            "supplier_02": "Mouser Electronics Inc.",
            "supplier_03": "meilon GmbH",
            "supplier_04": "meilon GmbH",              # RFID ebenfalls meilon
            "supplier_05": "UWC",
            "supplier_06": "RCTech",
            "supplier_07": "RCTech",                   # Receiver + Kabel
            "supplier_08": "IPS Karton",
            "supplier_09": "Wecando",
            "supplier_10": "Sebastian Meusch",         # Acryl/Fernbedienung laut Setup
        }
        return mapping.get(xmlid)

    # -------------------------------------------------------------------------
    # Hauptlogik
    # -------------------------------------------------------------------------

    def run(self) -> None:
        path = join_path(self.production_dir, "product_supplierinfo.csv")
        if not os.path.exists(path):
            log_info(
                f"[SUPPLIERINFO:SKIP] Keine 'product_supplierinfo.csv' in "
                f"{os.path.basename(self.production_dir)} gefunden."
            )
            return

        log_header(f"Supplierinfos aus '{os.path.basename(path)}' laden")
        created_count = 0
        updated_count = 0
        skipped_count = 0
        skipped_noproduct = 0
        skipped_nosupplier = 0

        for row in csv_rows(path, delimiter=","):
            defaultcode = row.get("product_tmpl_id/default_code")
            supplier_xmlid = row.get("name/id")
            minqty_raw = row.get("min_qty")
            price_raw = row.get("price")

            suppliername = (
                self._map_supplier_xmlid_to_name(supplier_xmlid) if supplier_xmlid else None
            )

            if not defaultcode or not suppliername:
                skipped_count += 1
                log_warn(
                    f"[SUPPLIERINFO:SKIP:ROW] Kein default_code oder kein gemappter "
                    f"supplier_name für Zeile: {row}"
                )
                continue

            producttmpl_id = self._find_product_tmpl(defaultcode)
            partner_id = self._find_supplier(suppliername)

            if not producttmpl_id or not partner_id:
                skipped_count += 1
                if not producttmpl_id and not partner_id:
                    skipped_noproduct += 1
                    skipped_nosupplier += 1
                    log_warn(
                        f"[SUPPLIERINFO:SKIP] {defaultcode}/{suppliername}: "
                        f"kein Produkt, kein Lieferant gefunden."
                    )
                elif not producttmpl_id:
                    skipped_noproduct += 1
                    log_warn(
                        f"[SUPPLIERINFO:SKIP] {defaultcode}/{suppliername}: "
                        f"kein Produkt gefunden."
                    )
                else:
                    skipped_nosupplier += 1
                    log_warn(
                        f"[SUPPLIERINFO:SKIP] {defaultcode}/{suppliername}: "
                        f"kein Lieferant gefunden."
                    )
                continue

            price = 0.0
            if price_raw:
                try:
                    price = float(price_raw)
                except ValueError:
                    log_warn(
                        f"[SUPPLIERINFO:WARN] Ungültiger Preis '{price_raw}' "
                        f"für {defaultcode}/{suppliername}, setze price=0.0."
                    )

            minqty = 0.0
            if minqty_raw:
                try:
                    minqty = float(minqty_raw)
                except ValueError:
                    log_warn(
                        f"[SUPPLIERINFO:WARN] Ungültige min_qty '{minqty_raw}' "
                        f"für {defaultcode}/{suppliername}, setze min_qty=0.0."
                    )

            domain = [
                ("product_tmpl_id", "=", producttmpl_id),
                ("partner_id", "=", partner_id),
            ]

            vals: Dict[str, Any] = {
                "product_tmpl_id": producttmpl_id,
                "partner_id": partner_id,
                "price": price,
                "min_qty": minqty,
            }

            si_id, created = self.client.ensure_record(
                "product.supplierinfo",
                domain,
                create_vals=vals,
                update_vals=vals,
            )
            if created:
                created_count += 1
            else:
                updated_count += 1

            log_success(
                f"[SUPPLIERINFO:{'NEW' if created else 'UPD'}] "
                f"{defaultcode}/{suppliername} -> {si_id} "
                f"(price={price}, min_qty={minqty})"
            )

        log_info(
            f"[SUPPLIERINFO:SUMMARY] {created_count} neu, {updated_count} aktualisiert, "
            f"{skipped_count} übersprungen (ohne Produkt: {skipped_noproduct}, "
            f"ohne Lieferant: {skipped_nosupplier})."
        )

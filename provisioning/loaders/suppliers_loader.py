import os
from typing import Dict, Any

from .csv_cleaner import csv_rows, join_path
from ..client import OdooClient
from provisioning.utils import (
    log_header,
    log_info,
    log_success,
    log_warn,
)


class SuppliersLoader:
    def __init__(self, client: OdooClient, base_data_dir: str) -> None:
        self.client = client
        self.normalized_dir = join_path(base_data_dir, "data_normalized")
        self.production_dir = join_path(base_data_dir, "production_data")

    def _build_partner_vals(self, row: Dict[str, str]) -> Dict[str, Any]:
        name = row.get("Lieferant") or row.get("name") or "Unnamed Supplier"
        email = row.get("email") or row.get("Email")
        phone = row.get("Telefon") or row.get("phone")
        street = row.get("Adresse") or row.get("address")

        return {
            "name": name,
            "email": email or False,
            "phone": phone or False,
            "street": street or False,
            "supplier_rank": 1,
            "customer_rank": 0,
            "is_company": True,
        }

    def load_suppliers(self) -> None:
        suppliers_path = join_path(
            self.normalized_dir,
            "Lieferanten-Table.normalized.csv",
        )
        if not os.path.exists(suppliers_path):
            log_warn(
                f"[SUPPLIER:SKIP] Datei 'Lieferanten-Table.normalized.csv' "
                f"in '{os.path.basename(self.normalized_dir)}' nicht gefunden."
            )
            return

        log_header("Lieferanten aus CSV laden")

        created_count = 0
        updated_count = 0

        for row in csv_rows(suppliers_path):
            name = row.get("Lieferant") or row.get("name")
            if not name:
                continue

            domain = [("name", "=", name), ("supplier_rank", ">", 0)]
            vals = self._build_partner_vals(row)
            partner_id, created = self.client.ensure_record(
                "res.partner",
                domain,
                create_vals=vals,
                update_vals=vals,
            )
            if created:
                created_count += 1
            else:
                updated_count += 1

            log_success(
                f"[SUPPLIER:{'NEW' if created else 'UPD'}] "
                f"{name} -> {partner_id}"
            )

        log_info(
            f"[SUPPLIER:SUMMARY] {created_count} neue, "
            f"{updated_count} aktualisierte Lieferanten."
        )

    def run(self) -> None:
        self.load_suppliers()

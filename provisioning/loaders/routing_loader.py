# provisioning/loaders/routing_loader.py

import os
from typing import Dict, Any, Optional, List

from .csv_cleaner import csv_rows, join_path
from ..client import OdooClient
from provisioning.utils import (
    log_header,
    log_info,
    log_success,
    log_warn,
)


class RoutingLoader:
    def __init__(self, client: OdooClient, base_data_dir: str) -> None:
        self.client = client
        self.routing_dir = join_path(base_data_dir, "routing_data")

    # --- Hilfen für BoMs & Workcenter ---

    def _find_bom_by_head_code(self, head_default_code: str) -> Optional[int]:
        """Findet die BoM zu einem Endprodukt-Default-Code (z.B. 029.3.000)."""
        res = self.client.search_read(
            "mrp.bom",
            [("product_tmpl_id.default_code", "=", head_default_code)],
            ["id"],
            limit=1,
        )
        return res[0]["id"] if res else None

    def _get_evo_bom_ids(self) -> List[int]:
        bom_ids: List[int] = []
        missing_heads: List[str] = []

        for code in ("029.3.000", "029.3.001", "029.3.002"):
            bom_id = self._find_bom_by_head_code(code)
            if bom_id:
                bom_ids.append(bom_id)
                log_info(f"[ROUTING:BOM] Kopf {code} -> BoM {bom_id}")
            else:
                missing_heads.append(code)
                log_warn(f"[ROUTING:WARN] Keine BoM gefunden für Kopf {code}")

        if not bom_ids:
            raise RuntimeError(
                "No BoMs found for EVO variants 029.3.000/001/002. "
                f"Fehlende Köpfe: {', '.join(missing_heads)}"
            )
        return bom_ids

    def _load_workcenters_if_needed(self) -> None:
        """Stellt sicher, dass die Workcenter aus workcenter.csv existieren."""
        path = join_path(self.routing_dir, "workcenter.csv")
        if not os.path.exists(path):
            # wenn du Workcenter schon manuell angelegt hast, kannst du diese Methode leer lassen
            log_info(
                f"[WORKCENTER:SKIP] Keine workcenter.csv gefunden in "
                f"'{os.path.basename(self.routing_dir)}' – Workcenter werden nicht automatisch angelegt."
            )
            return

        log_header("Workcenter aus CSV laden")

        created_count = 0
        updated_count = 0

        for row in csv_rows(path):
            name = row.get("name") or row.get("workcenter_name")
            if not name:
                continue

            domain = [("name", "=", name)]
            vals: Dict[str, Any] = {"name": name}

            cost_per_hour_raw = row.get("cost_per_hour")
            if cost_per_hour_raw:
                try:
                    cost_per_hour = float(cost_per_hour_raw)
                    vals["costs_hour"] = cost_per_hour
                except ValueError:
                    log_warn(
                        f"[WORKCENTER:WARN] Ungültiger cost_per_hour-Wert "
                        f"'{cost_per_hour_raw}' für Workcenter '{name}' – wird ignoriert."
                    )

            wc_id, created = self.client.ensure_record(
                "mrp.workcenter",
                domain,
                create_vals=vals,
                update_vals=vals,
            )
            if created:
                created_count += 1
            else:
                updated_count += 1

            log_success(f"[WORKCENTER:{'NEW' if created else 'UPD'}] {name} -> {wc_id}")

        log_info(
            f"[WORKCENTER:SUMMARY] {created_count} neue, {updated_count} aktualisierte Workcenter."
        )

    def _find_workcenter_by_any(self, wc_key: str) -> Optional[int]:
        """
        Versucht, einen Workcenter anhand eines Keys zu finden.
        wc_key kommt aus 'workcenter_id/id' (z.B. mrp_wc_3d_printer) oder Name.
        Hier wird pragmatisch über Name gemappt.
        """
        if not wc_key:
            return None

        mapping = {
            "mrp_wc_3d_printer": "3D-Drucker",
            "mrp_wc_laser": "Lasercutter",
            "mrp_wc_rework": "Nacharbeit",
            "mrp_wc_electronics": "Elektronik-Montage",
            "mrp_wc_assembly": "Gehäuse & Rotoren",
            "mrp_wc_quality": "Qualität",
        }
        name = mapping.get(wc_key, wc_key)

        res = self.client.search_read(
            "mrp.workcenter",
            [("name", "=", name)],
            ["id"],
            limit=1,
        )
        if not res:
            log_warn(
                f"[WORKCENTER:WARN] Kein Workcenter für Key '{wc_key}' "
                f"(gemappt auf Name '{name}') gefunden."
            )
            return None
        return res[0]["id"]

    def _get_fallback_workcenter(self) -> int:
        for candidate in ("Qualität", "3D-Drucker", "Nacharbeit"):
            res = self.client.search_read(
                "mrp.workcenter",
                [("name", "=", candidate)],
                ["id"],
                limit=1,
            )
            if res:
                log_info(
                    f"[WORKCENTER:FALLBACK] Verwende Workcenter '{candidate}' "
                    f"({res[0]['id']}) als Fallback."
                )
                return res[0]["id"]

        res = self.client.search_read("mrp.workcenter", [], ["id"], limit=1)
        if not res:
            raise RuntimeError("No mrp.workcenter found in Odoo for fallback.")
        log_info(
            f"[WORKCENTER:FALLBACK] Verwende erstes verfügbares Workcenter "
            f"({res[0]['id']}) als Fallback."
        )
        return res[0]["id"]

    # --- Operationen mit bom_id ---

    def _load_operations(self) -> None:
        path = join_path(self.routing_dir, "operations.csv")
        if not os.path.exists(path):
            log_info(
                f"[ROUTING:SKIP] Keine operations.csv gefunden in "
                f"'{os.path.basename(self.routing_dir)}'."
            )
            return

        log_header("Routing-Operationen aus CSV laden")

        bom_ids = self._get_evo_bom_ids()
        fallback_wc_id = self._get_fallback_workcenter()

        created_count = 0
        updated_count = 0

        for row in csv_rows(path):
            op_xml_id = row.get("id")
            name = row.get("name")
            wc_key = row.get("workcenter_id/id")

            if not name:
                continue

            wc_id = self._find_workcenter_by_any(wc_key) or fallback_wc_id

            duration_raw = row.get("time_cycle_manual")
            seq_raw = row.get("sequence")

            duration = None
            if duration_raw:
                try:
                    duration = float(duration_raw)
                except ValueError:
                    log_warn(
                        f"[OP:WARN] Ungültiger time_cycle_manual-Wert "
                        f"'{duration_raw}' für Operation '{name}' – wird ignoriert."
                    )

            sequence = None
            if seq_raw:
                try:
                    sequence = int(seq_raw)
                except ValueError:
                    log_warn(
                        f"[OP:WARN] Ungültiger sequence-Wert "
                        f"'{seq_raw}' für Operation '{name}' – wird ignoriert."
                    )

            # Operationen für jede EVO-BoM anlegen
            for bom_id in bom_ids:
                vals: Dict[str, Any] = {
                    "name": name,
                    "workcenter_id": wc_id,
                    "bom_id": bom_id,
                }
                if duration is not None:
                    vals["time_cycle_manual"] = duration
                if sequence is not None:
                    vals["sequence"] = sequence

                domain = [
                    ("name", "=", name),
                    ("bom_id", "=", bom_id),
                ]
                op_id, created = self.client.ensure_record(
                    "mrp.routing.workcenter",
                    domain,
                    create_vals=vals,
                    update_vals=vals,
                )
                if created:
                    created_count += 1
                else:
                    updated_count += 1

                log_success(
                    f"[OP:{'NEW' if created else 'UPD'}] {name} (BoM {bom_id}) -> {op_id}"
                )

        log_info(
            f"[OP:SUMMARY] Operationen: {created_count} neu, {updated_count} aktualisiert."
        )

    def run(self) -> None:
        self._load_workcenters_if_needed()
        self._load_operations()

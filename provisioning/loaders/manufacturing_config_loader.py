# provisioning/loaders/manufacturing_config_loader.py
import os
from typing import Dict, Any
from provisioning.utils.csv_cleaner import join_path
from ..client import OdooClient
from provisioning.utils import log_header, log_info, log_success, log_warn

class ManufacturingConfigLoader:
    def __init__(self, client: OdooClient, base_data_dir: str) -> None:
        self.client = client
        self.config_dir = join_path(base_data_dir, "manufacturing_config")
        company_ids = self.client.search("res.company", [], limit=1)
        self.company_id = company_ids[0] if company_ids else 1

    def _find_or_create_sequence(self, prefix: str, padding: int = 5) -> int:
        """ir.sequence für MO-References (company-scope)."""
        seq_name = f"Manufacturing Order {prefix}"
        domain = [("name", "=", seq_name), ("company_id", "in", [self.company_id, False])]
        seq = self.client.search_read("ir.sequence", domain, ["id"], limit=1)
        if seq:
            log_info(f"[SEQ] '{seq_name}' existiert → ID {seq[0]['id']}")
            return seq[0]["id"]

        vals = {
            "name": seq_name,
            "code": "mrp.production",
            "prefix": prefix,
            "padding": padding,
            "company_id": self.company_id,
        }
        seq_id = self.client.create("ir.sequence", vals)
        log_success(f"[SEQ:NEW] '{seq_name}' → ID {seq_id}")
        return seq_id

    def _setup_manufacturing_picking_type(self) -> None:
        """Manufacturing Operation Type + Sequence zuweisen."""
        log_header("🏭 Manufacturing Operation Types")

        # Standard Manufacturing PickingType finden (Manufacture)
        domain = [
            ("code", "=", "mrp_operation"),
            ("warehouse_id.company_id", "=", self.company_id),
        ]
        types = self.client.search_read("stock.picking.type", domain, ["id", "name", "sequence_id"])
        
        created_types = []
        for typ in types:
            if typ["sequence_id"]:
                log_info(f"[PICKINGTYPE] '{typ['name']}' hat Seq → OK")
                continue

            # Sequence zuweisen
            seq_id = self._find_or_create_sequence("MO")
            update_vals = {"sequence_id": seq_id}
            self.client.write("stock.picking.type", [typ["id"]], update_vals)
            log_success(f"[PICKINGTYPE:SEQ] '{typ['name']}' → Seq MO/{typ['id']}")

            created_types.append(typ["name"])

        if not types:
            log_warn("[PICKINGTYPE] Kein mrp_operation Type gefunden → Warehouse check!")

        log_info(f"[PICKINGTYPE:SUMMARY] {len(created_types)} Types sequenziert.")

    def run(self) -> None:
        self._setup_manufacturing_picking_type()
        log_success("[MANUFACTURING:CONFIG] ✅ MO-Sequences ready – Reference-Fehler behoben!")

# provisioning/loaders/mailserver_loader.py
import os
import re
from typing import Dict, Any

from ..client import OdooClient
from provisioning.utils import (
    log_header,
    log_info,
    log_success,
    log_warn,
    bump_progress,
)
from provisioning.config import MAILSERVERS_CONFIG  # ← neu!


class MailServerLoader:
    """Legt Odoo Mail-Server aus config.py (MAILSERVERS_CONFIG) per API an."""

    def __init__(self, client: OdooClient, base_data_dir: str) -> None:
        self.client = client
        # Kein CSV mehr – alles in config.py!

    def _resolve_env_vars(self, value: str) -> str:
        """Ersetzt [VARNAME]-Platzhalter durch os.getenv(VARNAME)."""
        pattern = r"\[(.*?)\]"
        def repl(match):
            var_name = match.group(1)
            resolved = os.getenv(var_name)
            if resolved is None:
                log_warn(f"[ENV:MISSING] '{var_name}' nicht gefunden → Platzhalter belassen")
                return match.group(0)
            return resolved
        return re.sub(pattern, repl, value)

    def _ensure_outgoing_server(self, smtp_config: Dict[str, Any]) -> int:
        domain = [("name", "=", smtp_config["name"])]
        server_id, created = self.client.ensure_record(
            "ir.mail_server",
            domain,
            create_vals=smtp_config,
            update_vals=smtp_config,
        )
        status = "NEW" if created else "UPD"
        log_success(f"[MAILSERVER:{status}] {smtp_config['name']} -> {server_id}")
        return server_id

    def _ensure_incoming_server(self, imap_config: Dict[str, Any]) -> int:
        domain = [("name", "=", imap_config["name"])]
        server_id, created = self.client.ensure_record(
            "fetchmail.server",
            domain,
            create_vals=imap_config,
            update_vals=imap_config,
        )
        status = "NEW" if created else "UPD"
        log_success(f"[FETCHMAIL:{status}] {imap_config['name']} → {server_id}")
        return server_id

    def load_from_config(self) -> None:
        log_header("Mail-Server aus config.py laden")
        for config in MAILSERVERS_CONFIG:
            # ENV-Variablen ersetzen (kopiere config)
            odoo_vals = {}
            for key, value in config.items():
                if isinstance(value, str):
                    resolved = self._resolve_env_vars(value)
                    odoo_vals[key] = resolved

            server_type = odoo_vals.pop("type", "").lower()  # ← type entfernen!
            odoo_vals["active"] = odoo_vals.get("active", "1") == "1"

            if server_type == "smtp":
                odoo_vals["smtp_port"] = int(odoo_vals.get("smtp_port", 587))
                odoo_vals["sequence"] = int(odoo_vals.get("sequence", 10))
                self._ensure_outgoing_server(odoo_vals)
            elif server_type == "imap":
                odoo_vals["port"] = int(odoo_vals.get("port", 993))
                odoo_vals["is_ssl"] = odoo_vals.get("is_ssl", "1") == "1"
                # sequence weglassen!
                self._ensure_incoming_server(odoo_vals)
            bump_progress(1.0)

        log_info("[MAILSERVER:SUMMARY] SMTP + IMAP aus config.py geladen")


    def run(self) -> None:
        self.load_from_config()

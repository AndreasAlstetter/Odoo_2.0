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
        """fetchmail.server – KORREKTE ODOO-Felder (user statt login)."""
        vals = {
            "name": imap_config["name"],
            "server": imap_config.get("server", "imap.gmail.com"),
            "port": int(imap_config.get("port", 993)),
            "is_ssl": imap_config.get("is_ssl", True),
            "user": imap_config["user"],          # ← FIX: user statt login!
            "password": imap_config["password"],
            "active": imap_config.get("active", True),
        }
        
        # Optionale Felder
        if "priority" in imap_config:
            vals["priority"] = int(imap_config["priority"])
        if "object_id" in imap_config:
            vals["object_id"] = int(imap_config["object_id"])
        
        domain = [("name", "=", vals["name"])]
        server_id, created = self.client.ensure_record(
            "fetchmail.server",
            domain,
            create_vals=vals,
            update_vals=vals,
        )
        status = "NEW" if created else "UPD"
        log_success(f"[FETCHMAIL:{status}] {vals['name']} → {server_id}")
        return server_id

    def load_from_config(self) -> None:
        log_header("Mail-Server aus config.py laden")
        for config in MAILSERVERS_CONFIG:
            odoo_vals = {}
            for key, value in config.items():
                if isinstance(value, str):
                    resolved = self._resolve_env_vars(value)
                    odoo_vals[key] = resolved
                else:
                    odoo_vals[key] = value

            server_type = odoo_vals.pop("type", "").lower()
            odoo_vals["active"] = odoo_vals.get("active", "1") == "1"

            if server_type == "smtp":
                odoo_vals["smtp_port"] = int(odoo_vals.get("smtp_port", 587))
                self._ensure_outgoing_server(odoo_vals)
            elif server_type == "imap":
                # IMAP-spezifisch: Felder für fetchmail.server
                self._ensure_incoming_server(odoo_vals)
            bump_progress(1.0)
        
        self._setup_mail_parameters()  # Bounce-Fix

    
        log_info("[MAILSERVER:SUMMARY] SMTP + IMAP + Parameter geladen")

    def _setup_mail_parameters(self) -> None:
        """Setzt Bounce/Catchall-Parameter für fehlerfreie Mails."""
        params = [
            ("mail.catchall.domain", "[MAIL_CATCHALL_DOMAIN]"),  # ttz_leipheim.de
            ("mail.bounce.alias", "bounce"),
            ("mail.use_alias", "True"),
        ]
        
        for key, value in params:
            resolved_value = self._resolve_env_vars(value)
            domain = [("key", "=", key)]
            self.client.ensure_record(
                "ir.config_parameter",
                domain,
                create_vals={"key": key, "value": resolved_value},
                update_vals={"value": resolved_value},
            )
            log_success(f"[PARAM] {key} = {resolved_value}")


    def run(self) -> None:
        self.load_from_config()

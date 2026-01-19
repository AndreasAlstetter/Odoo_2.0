# config.py (erweitert – behält aktuelle Struktur)
import os
from dataclasses import dataclass
from typing import List, Dict, Any, Optional


from dotenv import load_dotenv


# Basisverzeichnis des Projekts
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)


# Datenpfad für Mengenstückliste (normalisierte CSV)
MENGE_CSV_PATH = os.path.join(
    BASE_DIR,
    "data",
    "data_normalized",
    "Materialbedarfplanung-Table_normalized.csv",  # Dateiname aus Anhang
)


# Produktnamen der Kopf-Templates (wie sie ProductsLoader anlegt)
PRODUCT_SPARTAN_NAME = "EVO 029.3.000"
PRODUCT_LIGHTWEIGHT_NAME = "EVO 029.3.001"
PRODUCT_BALANCE_NAME = "EVO 029.3.002"


# UMH-Event-Dateien (für End-to-End und Produktionssimulation)
UMH_EVENTS_ENDTOEND_FILE = os.path.join(
    BASE_DIR,
    "data",
    "umh_events_endtoend.json",
)
UMH_EVENTS_PRODUCTION_FILE = os.path.join(
    BASE_DIR,
    "data",
    "umh_events_production.json",
)


# ═══════════════════════════════════════════════════════════════════════════════
# MAILSERVER KONFIGURATION (hardcoded + ENV-Platzhalter)
# ═══════════════════════════════════════════════════════════════════════════════
MAILSERVERS_CONFIG: List[Dict[str, str]] = [
    {
        "type": "smtp",
        "name": "Gmail SMTP",
        "smtp_host": "smtp.gmail.com",
        "smtp_port": "587",
        "smtp_encryption": "starttls",
        "smtp_user": os.getenv("GMAIL_SMTP_USER"),  # aus .env
        "smtp_pass": os.getenv("GMAIL_SMTP_PASS"),  # App-Password!
        "active": "1",
        "sequence": "10",
    },
    {
        "type": "smtp",
        "name": "Office365 SMTP",
        "smtp_host": "smtp.office365.com", 
        "smtp_port": "587",
        "smtp_encryption": "starttls",
        "smtp_user": os.getenv("OFFICE365_USER"),
        "smtp_pass": os.getenv("OFFICE365_PASS"),  # FIX: Doppelter Eintrag entfernt
        "active": "1",
        "sequence": "20",
    },
    {
        "type": "imap",
        "name": "Office365 IMAP", 
        "server_type": "imap",
        "server": "outlook.office365.com",
        "port": "993",
        "is_ssl": "1",
        "user": os.getenv("OFFICE365_USER"),
        "password": os.getenv("OFFICE365_PASS"),
        "active": "1",
        # Kein sequence für fetchmail.server!
    },
]
MAILSERVERS_CSV_PATH = os.path.join(BASE_DIR, "mailserver_config.csv")  # Export-Pfad


@dataclass
class OdooConfig:
    url: str
    db: str
    user: str
    password: str
    base_data_dir: Optional[str] = None  # ← NEU: Für RoutingLoader


    @classmethod
    def from_env(cls) -> "OdooConfig":
        """Erweiterte Config mit Base-Data-Dir aus .env."""
        required = ["ODOO_URL", "ODOO_DB", "ODOO_USER", "ODOO_PASSWORD"]
        missing = [k for k in required if not os.getenv(k)]
        if missing:
            raise ValueError(f"Fehlende .env Variablen: {', '.join(missing)}")

        return cls(
            url=(os.getenv("ODOO_URL") or "").rstrip("/"),
            db=os.getenv("ODOO_DB") or "",
            user=os.getenv("ODOO_USER") or "",
            password=os.getenv("ODOO_PASSWORD") or "",
            base_data_dir=os.getenv("BASE_DATA_DIR", os.path.join(BASE_DIR, "data")),  # Default: ./data
        )

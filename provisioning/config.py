import os
from dataclasses import dataclass

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


@dataclass
class OdooConfig:
    url: str
    db: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "OdooConfig":
        return cls(
            url=(os.getenv("ODOO_URL") or "").rstrip("/"),
            db=os.getenv("ODOO_DB") or "",
            user=os.getenv("ODOO_USER") or "",
            password=os.getenv("ODOO_PASSWORD") or "",
        )

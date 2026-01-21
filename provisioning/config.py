"""
config.py - Zentrale Konfiguration für Odoo Provisioning
Alle Parameter sind hier zentralisiert, ENV-validiert und typisiert.
"""
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urlparse
from dotenv import load_dotenv


# ═══════════════════════════════════════════════════════════════════════════════
# UMGEBUNG LADEN
# ═══════════════════════════════════════════════════════════════════════════════

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")

if not os.path.exists(ENV_PATH):
    raise FileNotFoundError(f".env nicht gefunden: {ENV_PATH}")

load_dotenv(ENV_PATH)


# ═══════════════════════════════════════════════════════════════════════════════
# DATENVERZEICHNISSE (mit Validierung)
# ═══════════════════════════════════════════════════════════════════════════════

def _ensure_data_dir(rel_path: str) -> str:
    """Stelle sicher dass Verzeichnis existiert."""
    full_path = os.path.join(BASE_DIR, "data", rel_path)
    if not os.path.isdir(full_path):
        raise ValueError(f"Datenverzeichnis nicht gefunden: {full_path}")
    return full_path


DATA_DIR = os.path.join(BASE_DIR, "data")
DATA_NORMALIZED_DIR = _ensure_data_dir("data_normalized")
BOM_DIR = _ensure_data_dir("bom")
QUALITY_DIR = _ensure_data_dir("quality")
ROUTING_DIR = _ensure_data_dir("routing_data")
PRODUCTION_DATA_DIR = _ensure_data_dir("production_data")


# ═══════════════════════════════════════════════════════════════════════════════
# DATEI-PFADE (mit Typo-Fixes)
# ═══════════════════════════════════════════════════════════════════════════════

class DataPaths:
    """Zentralisierte Datei-Pfade für alle Loader."""
    
    # CSV Strukturdaten (Typos korrigiert!)
    STRUKTUR_CSV = os.path.join(DATA_NORMALIZED_DIR, "Strukturl-ekkiliste-Table_normalized.csv")
    MATERIALBEDARF_CSV = os.path.join(DATA_NORMALIZED_DIR, "Materialbedarfsplanung-Table_normalized.csv")
    LAGERDATEN_CSV = os.path.join(DATA_NORMALIZED_DIR, "Lagerdaten-Table_normalized.csv")
    LIEFERANTEN_CSV = os.path.join(DATA_NORMALIZED_DIR, "Lieferanten-Table_normalized.csv")
    FERTIGUNGSKOSTEN_CSV = os.path.join(DATA_NORMALIZED_DIR, "Fertigungskosten-Table_normalized.csv")
    
    # BoM-Varianten
    BOM_SPARTAN = os.path.join(BOM_DIR, "spartan_bom.csv")
    BOM_LIGHTWEIGHT = os.path.join(BOM_DIR, "lightweight_bom.csv")
    BOM_BALANCE = os.path.join(BOM_DIR, "balance_bom.csv")
    BOM_DEFAULT = os.path.join(BOM_DIR, "bom.csv")
    
    # Quality & Routing
    ROUTING_OPERATIONS = os.path.join(ROUTING_DIR, "operations.csv")
    ROUTING_WORKCENTERS = os.path.join(ROUTING_DIR, "workcenter.csv")
    
    # Produktionsdaten
    WORKCENTER_DATA = os.path.join(PRODUCTION_DATA_DIR, "workcenter.csv")
    LAGERPLAETZE_DATA = os.path.join(PRODUCTION_DATA_DIR, "Lagerplätze.csv")
    PRODUCT_PRICES = os.path.join(PRODUCTION_DATA_DIR, "Produktpreise.csv")
    PRODUCT_SUPPLIERINFO = os.path.join(PRODUCTION_DATA_DIR, "product_supplierinfo.csv")
    
    @classmethod
    def validate_all(cls) -> None:
        """Validiere dass alle kritischen Dateien existieren."""
        critical_files = [
            cls.STRUKTUR_CSV,
            cls.MATERIALBEDARF_CSV,
            cls.ROUTING_OPERATIONS,
            cls.WORKCENTER_DATA,
        ]
        missing = [f for f in critical_files if not os.path.isfile(f)]
        if missing:
            raise FileNotFoundError(f"Kritische Dateien fehlen: {missing}")


# ═══════════════════════════════════════════════════════════════════════════════
# PRODUKT-TEMPLATES (Drohnen-Varianten)
# ═══════════════════════════════════════════════════════════════════════════════

class ProductTemplates:
    """Produktkopf-Templates für die 3 Drohnen-Varianten."""
    
    SPARTAN = {
        'code': 'EVO 029.3.000',
        'name': 'EVO 029.3.000 - Spartan',
        'list_price': 499.00,  # EUR
        'variant': 'Spartan',
    }
    
    LIGHTWEIGHT = {
        'code': 'EVO 029.3.001',
        'name': 'EVO 029.3.001 - Lightweight',
        'list_price': 599.00,
        'variant': 'Lightweight',
    }
    
    BALANCE = {
        'code': 'EVO 029.3.002',
        'name': 'EVO 029.3.002 - Balance',
        'list_price': 699.00,
        'variant': 'Balance',
    }
    
    ALL_CODES = [SPARTAN['code'], LIGHTWEIGHT['code'], BALANCE['code']]
    
    @classmethod
    def get_by_code(cls, code: str) -> Dict[str, Any]:
        """Hole Template by Produktcode."""
        mapping = {
            cls.SPARTAN['code']: cls.SPARTAN,
            cls.LIGHTWEIGHT['code']: cls.LIGHTWEIGHT,
            cls.BALANCE['code']: cls.BALANCE,
        }
        if code not in mapping:
            raise ValueError(f"Unbekannter Produktcode: {code}")
        return mapping[code]


# ═══════════════════════════════════════════════════════════════════════════════
# ODOO RPC KONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class OdooRPCConfig:
    """Konfiguration für Odoo RPC-Verbindung."""
    
    url: str
    db: str
    user: str
    password: str
    
    # RPC-Parameter (skaliert für >500 Drohnen/Tag)
    timeout: int = 60  # Sekunden (erhöht von 30!)
    max_retries: int = 5  # Erhöht von 3
    backoff_factor: float = 1.5
    batch_size: int = 500  # Erhöht von 100
    search_limit: int = 100  # Erhöht von 1 (kritisch!)
    
    # Endpoints
    rpc_endpoints: Dict[str, str] = field(default_factory=lambda: {
        'common': '/xmlrpc/2/common',
        'object': '/xmlrpc/2/object',
    })
    
    @classmethod
    def from_env(cls) -> 'OdooRPCConfig':
        """Lade Konfiguration aus .env mit Validierung."""
        url = os.getenv('ODOO_URL', '').rstrip('/')
        db = os.getenv('ODOO_DB', '')
        user = os.getenv('ODOO_USER', '')
        password = os.getenv('ODOO_PASSWORD', '')
        
        # Validierung
        if not all([url, db, user, password]):
            missing = []
            if not url:
                missing.append('ODOO_URL')
            if not db:
                missing.append('ODOO_DB')
            if not user:
                missing.append('ODOO_USER')
            if not password:
                missing.append('ODOO_PASSWORD')
            raise ValueError(f"Fehlende .env Variablen: {', '.join(missing)}")
        
        # URL validieren
        try:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                raise ValueError(f"Ungültige ODOO_URL: {url}")
        except Exception as e:
            raise ValueError(f"ODOO_URL Parsing fehlgeschlagen: {e}")
        
        return cls(
            url=url,
            db=db,
            user=user,
            password=password,
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Für RPC-Client."""
        return {
            'url': self.url,
            'db': self.db,
            'user': self.user,
            'password': self.password,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# CSV VERARBEITUNG
# ═══════════════════════════════════════════════════════════════════════════════

class CSVConfig:
    """CSV-Import Konfiguration."""
    
    # Standard-Delimiters
    DELIMITER_PRIMARY = ','
    DELIMITER_SECONDARY = ';'
    ENCODING = 'utf-8'
    
    # Fallback bei Encoding-Fehler
    ENCODING_FALLBACK = 'latin-1'
    
    # Erforderliche Spalten pro Loader (validierung)
    REQUIRED_COLUMNS = {
        'bom': ['id', 'product_tmpl_id/default_code', 'bom_line_ids/product_id/default_code'],
        'routing': ['id', 'name', 'operation_ids/workcenter_id/name'],
        'quality': ['name', 'test_type', 'stage_id'],
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PREISKALKULATIONEN (KRITISCH!)
# ═══════════════════════════════════════════════════════════════════════════════

class PricingConfig:
    """
    Preiskalkulationen für Produkte.
    
    LOGIK:
    - cost_price: Einkaufspreis vom Lieferanten
    - list_price: Verkaufspreis = cost_price * markup_factor
    
    KRITISCH: Fallbacks nur für fehlende Daten verwenden!
    """
    
    # Markup-Faktoren nach Produkttyp
    MARKUP_FACTORS = {
        'raw_material': 1.15,      # Rohstoffe: 15%
        'component': 1.25,          # Komponenten: 25%
        'finished_good': 1.35,      # Fertigprodukte: 35%
        'service': 1.45,            # Services: 45%
    }
    
    # Fallback-Preise (NUR wenn CSV nicht abrufbar)
    FALLBACK_COST_PRICE = 5.00
    FALLBACK_LIST_PRICE = 10.00
    
    # Standardmäßiger Markup wenn Typ unbekannt
    DEFAULT_MARKUP = 1.25
    
    # Währung
    CURRENCY = 'EUR'
    
    @staticmethod
    def calculate_list_price(cost_price: float, markup: float = None) -> float:
        """Berechne Verkaufspreis aus Einkaufspreis."""
        if markup is None:
            markup = PricingConfig.DEFAULT_MARKUP
        
        if cost_price <= 0:
            raise ValueError(f"cost_price muss > 0 sein: {cost_price}")
        
        return round(cost_price * markup, 2)


# ═══════════════════════════════════════════════════════════════════════════════
# LAGERVERWALTUNG & KATEGORIEN
# ═══════════════════════════════════════════════════════════════════════════════

class StockConfig:
    """Lagerverwaltung."""
    
    # Kategorien mit Hierarchie
    CATEGORIES = {
        'Drohnen': {
            'parent': None,
            'tracked': True,  # Seriennummern
        },
        'Kernkomponenten': {
            'parent': None,
            'tracked': True,
        },
        'Elektronik': {
            'parent': 'Kernkomponenten',
            'tracked': True,
        },
        '3D-Druck': {
            'parent': 'Kernkomponenten',
            'tracked': False,
        },
        'Verpackung': {
            'parent': None,
            'tracked': False,
        },
    }
    
    # Lot-Tracking für Serien/Chargen
    LOT_TRACKED_CATEGORIES = ['Drohnen', 'Elektronik', 'Kernkomponenten']
    
    # Barcode-Prefix für verschiedene Kategorien
    BARCODE_PREFIXES = {
        'Drohnen': 'DRO',
        'Elektronik': 'ELE',
        'Komponenten': 'KOM',
        '3D-Druck': '3DP',
    }


# ═══════════════════════════════════════════════════════════════════════════════
# FERTIGUNGSSTEUERUNG (MRP/ROUTING)
# ═══════════════════════════════════════════════════════════════════════════════

class ManufacturingConfig:
    """Fertigungssteuerung für >500 Drohnen/Tag."""
    
    # Arbeitsplan (Routing)
    WORKCENTERS = {
        'WC-3D': {
            'name': '3D-Drucker',
            'capacity': 10,  # Teile parallel
            'efficiency': 0.95,
        },
        'WC-LC': {
            'name': 'Lasercutter',
            'capacity': 5,
            'efficiency': 0.98,
        },
        'WC-NACH': {
            'name': 'Nacharbeit',
            'capacity': 8,
            'efficiency': 0.90,
        },
        'WC-WTB': {
            'name': 'WT bestücken',
            'capacity': 15,
            'efficiency': 0.92,
        },
        'WC-LOET': {
            'name': 'Löten Elektronik',
            'capacity': 6,
            'efficiency': 0.88,
        },
        'WC-MONT': {
            'name': 'Montage Elektronik',
            'capacity': 12,
            'efficiency': 0.93,
        },
        'WC-FLASH': {
            'name': 'Flashen Flugcontroller',
            'capacity': 20,
            'efficiency': 0.99,
        },
        'WC-MONT2': {
            'name': 'Montage Gehäuse Rotoren',
            'capacity': 10,
            'efficiency': 0.94,
        },
        'WC-QM-END': {
            'name': 'End-Qualitätskontrolle',
            'capacity': 8,
            'efficiency': 0.96,
        },
    }
    
    # Produktionssequenz-Nummern
    MO_SEQUENCE_PREFIX = 'MO'
    MO_SEQUENCE_PADDING = 7  # MO0000001 - MO9999999 (bis 10M Orders)
    MRP_SEQUENCE_CODE = 'mrp.production'
    
    # Fallback-Arbeitsplätze (wenn keine anderen auffindbar)
    FALLBACK_WORKCENTERS = ['WC-QM-END', 'WC-3D', 'WC-NACH']
    
    # Produktdurchsatz pro Tag
    EXPECTED_DAILY_OUTPUT = 500  # Drohnen/Tag
    SHIFTS_PER_DAY = 2
    HOURS_PER_SHIFT = 8


# ═══════════════════════════════════════════════════════════════════════════════
# QUALITÄTSKONTROLLE
# ═══════════════════════════════════════════════════════════════════════════════

class QualityConfig:
    """Qualitätskontrolle - Inspektionspunkte."""
    
    # Inspektionspunkte pro Fertigungsstufe
    INSPECTION_POINTS = {
        'Haube': {
            'stage': '3D-Druck',
            'test_type': 'Dimension',
            'pass_min': 149.5,  # mm
            'pass_max': 150.5,
            'test_method': 'Schieblehre',
            'critical': False,
        },
        'Füße': {
            'stage': '3D-Druck',
            'test_type': 'Dimension',
            'pass_min': 24.8,
            'pass_max': 25.2,
            'test_method': 'Schieblehre',
            'critical': False,
        },
        'Grundplatten': {
            'stage': 'Elektronikbestückung',
            'test_type': 'Optical',
            'pass_criteria': 'Keine Kratzer, Lötpunkte OK',
            'test_method': 'Visuelle Kontrolle',
            'critical': True,
        },
        'Endkontrolle': {
            'stage': 'Montage',
            'test_type': 'Functional',
            'pass_criteria': 'Flugtest bestanden, Datenverbindung OK',
            'test_method': 'Testflug + Diagnose',
            'critical': True,
        },
    }
    
    # Qualitätspunkte als Odoo QualityPoint
    QUALITY_POINT_TEMPLATE = {
        'model_id': 'mrp.production',
        'check_execute_now': False,
        'instructions': 'Standard-Inspektionsanweisung',
    }


# ═══════════════════════════════════════════════════════════════════════════════
# MAIL-KONFIGURATION (ODOO IR.MAIL_SERVER)
# ═══════════════════════════════════════════════════════════════════════════════

class MailConfig:
    """Mail-Server Konfiguration für Odoo."""
    
    # SMTP-Server
    SMTP_SERVERS = [
        {
            'sequence': 10,
            'name': 'Gmail SMTP',
            'smtp_host': 'smtp.gmail.com',
            'smtp_port': 587,
            'smtp_encryption': 'starttls',
            'smtp_user': os.getenv('GMAIL_SMTP_USER', ''),
            'smtp_password': os.getenv('GMAIL_SMTP_PASS', ''),
            'active': bool(os.getenv('GMAIL_SMTP_USER')),
        },
        {
            'sequence': 20,
            'name': 'Office365 SMTP',
            'smtp_host': 'smtp.office365.com',
            'smtp_port': 587,
            'smtp_encryption': 'starttls',
            'smtp_user': os.getenv('OFFICE365_SMTP_USER', ''),
            'smtp_password': os.getenv('OFFICE365_SMTP_PASS', ''),
            'active': bool(os.getenv('OFFICE365_SMTP_USER')),
        },
    ]
    
    # IMAP-Server
    IMAP_SERVERS = [
        {
            'priority': 10,
            'name': 'Gmail IMAP',
            'server': 'imap.gmail.com',
            'port': 993,
            'is_ssl': True,
            'user': os.getenv('GMAIL_IMAP_USER', ''),
            'password': os.getenv('GMAIL_IMAP_PASS', ''),
            'active': bool(os.getenv('GMAIL_IMAP_USER')),
        },
        {
            'priority': 20,
            'name': 'Office365 IMAP',
            'server': 'outlook.office365.com',
            'port': 993,
            'is_ssl': True,
            'user': os.getenv('OFFICE365_IMAP_USER', ''),
            'password': os.getenv('OFFICE365_IMAP_PASS', ''),
            'active': bool(os.getenv('OFFICE365_IMAP_USER')),
        },
    ]
    
    # Config-Parameter (ir.config_parameter)
    CONFIG_PARAMETERS = [
        ('mail.catchall.domain', os.getenv('MAIL_CATCHALL_DOMAIN', 'drohnen-gmbh.de')),
        ('mail.bounce.alias', 'bounce'),
        ('mail.reply.alias', 'reply'),
        ('mail.use_alias', 'True'),
    ]
    
    # Test-Konfiguration
    SMTP_TEST_EMAIL = os.getenv('SMTP_TEST_EMAIL', 'test@drohnen-gmbh.de')
    SMTP_TEST_RECIPIENT = os.getenv('SMTP_TEST_RECIPIENT', 'admin@drohnen-gmbh.de')


# ═══════════════════════════════════════════════════════════════════════════════
# UOM (EINHEITEN)
# ═══════════════════════════════════════════════════════════════════════════════

class UOMConfig:
    """Maßeinheiten für Produkte."""
    
    # CSV → Odoo UOM Mapping
    MAPPING = {
        'stk': 'Stück',
        'st': 'Stück',
        'units': 'Stück',
        'pcs': 'Stück',
        'g': 'Gramm',
        'kg': 'Kilogramm',
        'cm': 'Zentimeter',
        'm': 'Meter',
        'mm': 'Millimeter',
        'h': 'Stunden',
        'min': 'Minuten',
    }
    
    # Standard UOM für Produkte
    DEFAULT_UOM = 'Stück'
    
    # Konversionen (Basis = kg)
    CONVERSIONS = {
        'g': 0.001,
        'mg': 0.000001,
        'kg': 1.0,
        't': 1000.0,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING & DEBUG
# ═══════════════════════════════════════════════════════════════════════════════

class LoggingConfig:
    """Logging-Konfiguration."""
    
    LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    LOG_DIR = os.path.join(BASE_DIR, 'logs')
    LOG_FILE = os.path.join(LOG_DIR, 'provisioning.log')
    
    # Audit-Logging für RPC-Calls
    AUDIT_ENABLED = os.getenv('AUDIT_ENABLED', 'true').lower() == 'true'
    AUDIT_LOG_FILE = os.path.join(LOG_DIR, 'audit.log')


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLETON: Globale Config laden
# ═══════════════════════════════════════════════════════════════════════════════

def get_odoo_config() -> OdooRPCConfig:
    """Get cached Odoo RPC config."""
    global _ODOO_CONFIG
    if '_ODOO_CONFIG' not in globals():
        _ODOO_CONFIG = OdooRPCConfig.from_env()
    return _ODOO_CONFIG


def validate_config() -> None:
    """Validiere komplette Konfiguration beim Startup."""
    try:
        # Lade RPC-Config
        odoo_cfg = get_odoo_config()
        
        # Validiere Daten-Verzeichnisse
        DataPaths.validate_all()
        
        # Validiere kritische ENV-Variablen
        mail_enabled = bool(os.getenv('GMAIL_SMTP_USER')) or bool(os.getenv('OFFICE365_SMTP_USER'))
        
        print(f"✓ Odoo RPC: {odoo_cfg.url}")
        print(f"✓ Daten-Verzeichnisse: OK")
        print(f"✓ Mail-Server: {'aktiviert' if mail_enabled else 'nicht konfiguriert'}")
        print(f"✓ Konfiguration: OK")
        
    except Exception as e:
        raise RuntimeError(f"Konfigurationsvalidierung fehlgeschlagen: {e}")


# Auto-validate on import (optional - kann auskommentiert werden)
if os.getenv('SKIP_CONFIG_VALIDATION', 'false').lower() != 'true':
    try:
        validate_config()
    except Exception as e:
        print(f"WARNUNG: {e}")



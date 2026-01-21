"""
products_loader.py - Produktladeung mit Preiskalkulationen

Lädt Produkte aus:
1. Strukturliste (BoM mit Preisen)
2. Lagerdaten (Stock/Inventory)

Optimiert für >500 Drohnen/Tag mit:
- Batch-RPC Calls
- Preis-Audit Trail
- Schema-Validierung
- Fehlerresilienz
"""

import os
import csv
import logging
import re
from pathlib import Path
from decimal import Decimal
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime

from client import OdooClient, RecordAmbiguousError, ValidationError
from config import (
    DataPaths,
    PricingConfig,
    UOMConfig,
    CSVConfig,
    ProductTemplates,
    StockConfig,
)
from utils import log_success, log_info, log_warn, log_header, log_error


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

STRUCT_CSV_REQUIRED_COLS = [
    'default_code', 'Artikelbezeichnung', 'Gesamtpreis_raw'
]
STOCK_CSV_REQUIRED_COLS = ['ID', 'name', 'price']

# CSV Delimiters (Struktur nutzt `;`, Stock nutzt `,`)
CSV_DELIM_STRUKTUR = ';'
CSV_DELIM_STOCK = ','


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class ProductLoaderError(Exception):
    """Base exception for ProductsLoader."""
    pass


class PriceParseError(ProductLoaderError):
    """Price parsing error."""
    pass


class CSVSchemaError(ProductLoaderError):
    """CSV schema validation error."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# PRICE PARSER
# ═══════════════════════════════════════════════════════════════════════════════

class PriceParser:
    """Robust price parsing von verschiedenen Formaten."""
    
    # Regex für Preise: unterstützt deutsche & US Formate
    PRICE_REGEX = re.compile(
        r'(?:EUR|€|\$)?\s*'  # Optional currency prefix
        r'([0-9]{1,3}(?:[.,][0-9]{3})*[.,][0-9]{2}|[0-9]+[.,][0-9]{2})'  # Number
        r'(?:\s*(?:EUR|€|\$))?',  # Optional currency suffix
        re.IGNORECASE
    )
    
    @staticmethod
    def parse(price_str: str) -> Decimal:
        """
        Parse price string zu Decimal.
        
        Args:
            price_str: Preis im Format "0,08€", "67,60 EUR", "1.234,56", etc.
        
        Returns:
            Decimal mit 2 Dezimalstellen
        
        Raises:
            PriceParseError: Wenn Parse fehlschlägt
        """
        if not price_str or not isinstance(price_str, str):
            raise PriceParseError(f"Invalid price input: {price_str}")
        
        match = PriceParser.PRICE_REGEX.search(price_str.strip())
        if not match:
            raise PriceParseError(f"No price pattern found in: {price_str}")
        
        price_part = match.group(1)
        
        # Normalize: entferne 1000-er Separator, konvertiere Dezimal zu .
        # Deutsche Format: 1.234,56 → 1234.56
        # US Format: 1,234.56 → 1234.56
        
        # Strategy: Wenn mehrere . oder , → assume 1000-er Separator
        dot_count = price_part.count('.')
        comma_count = price_part.count(',')
        
        if dot_count > 1 or comma_count > 1:
            # Mehrere Separators → letzter ist Dezimal
            if price_part.rfind('.') > price_part.rfind(','):
                # Letzter ist . → US Format
                price_part = price_part.replace(',', '').replace('.', '_')
            else:
                # Letzter ist , → Deutsche Format
                price_part = price_part.replace('.', '').replace(',', '_')
            price_part = price_part.replace('_', '.')
        
        elif dot_count == 1 and comma_count == 1:
            # Ein . und ein , → bestimme nach Position
            if price_part.rfind('.') > price_part.rfind(','):
                # . kommt später → US Format (1,234.56)
                price_part = price_part.replace(',', '')
            else:
                # , kommt später → Deutsche Format (1.234,56)
                price_part = price_part.replace('.', '').replace(',', '.')
        
        else:
            # Nur ein Separator
            if ',' in price_part and '.' not in price_part:
                # Deutsche Format: 99,99
                price_part = price_part.replace(',', '.')
            elif '.' in price_part and ',' not in price_part:
                # US Format: 99.99
                pass
            else:
                # Nur Ziffer
                pass
        
        try:
            price = Decimal(price_part)
            
            if price < 0:
                raise PriceParseError(f"Negative price not allowed: {price_str} → {price}")
            
            # Round to 2 decimal places
            price = price.quantize(Decimal('0.01'))
            
            return price
            
        except (ValueError, InvalidOperation) as e:
            raise PriceParseError(f"Decimal conversion failed: {price_str} → {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# CSV VALIDATOR
# ═══════════════════════════════════════════════════════════════════════════════

class CSVValidator:
    """CSV schema validation."""
    
    @staticmethod
    def validate_schema(
        csv_path: Path,
        required_columns: List[str],
        delimiter: str = ',',
    ) -> None:
        """
        Validate CSV schema.
        
        Raises:
            CSVSchemaError: Wenn Schema ungültig
        """
        if not csv_path.exists():
            raise CSVSchemaError(f"CSV file not found: {csv_path}")
        
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=delimiter)
                
                if not reader.fieldnames:
                    raise CSVSchemaError(f"CSV has no header: {csv_path}")
                
                missing = [
                    col for col in required_columns
                    if col not in reader.fieldnames
                ]
                
                if missing:
                    raise CSVSchemaError(
                        f"CSV missing required columns: {missing}\n"
                        f"Available: {list(reader.fieldnames)}"
                    )
        
        except csv.Error as e:
            raise CSVSchemaError(f"CSV parse error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# WAREHOUSE MAPPING
# ═══════════════════════════════════════════════════════════════════════════════

class WarehouseMapper:
    """Load warehouse ID mapping from external CSV."""
    
    @staticmethod
    def load_mapping(csv_path: Path) -> Dict[str, str]:
        """
        Load warehouse mapping from CSV.
        
        CSV Format:
            old_warehouse_id,new_warehouse_id
            22,000.1.000
            14,001.1.000
        
        Args:
            csv_path: Path zu lager_mapping.csv
        
        Returns:
            Dict: old_id → new_id
        
        Raises:
            FileNotFoundError: Wenn Datei nicht existiert
            CSVSchemaError: Wenn Schema ungültig
        """
        if not csv_path.exists():
            raise FileNotFoundError(f"Warehouse mapping file not found: {csv_path}")
        
        mapping = {}
        
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                
                if not reader.fieldnames or len(reader.fieldnames) < 2:
                    raise CSVSchemaError(
                        f"Warehouse mapping CSV must have at least 2 columns, "
                        f"got: {reader.fieldnames}"
                    )
                
                for row_idx, row in enumerate(reader, start=2):
                    old_id = row.get('old_warehouse_id', '').strip()
                    new_id = row.get('new_warehouse_id', '').strip()
                    
                    if not old_id or not new_id:
                        logger.warning(f"Warehouse mapping row {row_idx}: empty ID skipped")
                        continue
                    
                    mapping[old_id] = new_id
        
        except csv.Error as e:
            raise CSVSchemaError(f"Warehouse mapping CSV error: {e}")
        
        return mapping


# ═══════════════════════════════════════════════════════════════════════════════
# PRODUCTS LOADER
# ═══════════════════════════════════════════════════════════════════════════════

class ProductsLoader:
    """
    Produktladeung mit Preiskalkulationen.
    
    Optimiert für >500 Drohnen/Tag:
    - Batch RPC Calls (nicht Row-by-Row)
    - Preis-Audit Trail
    - Schema-Validierung
    - Fehlerresilienz
    """
    
    def __init__(
        self,
        client: OdooClient,
        base_data_dir: str,
    ) -> None:
        self.client = client
        self.base_data_dir = Path(base_data_dir)
        self.data_normalized_dir = self.base_data_dir / 'data_normalized'
        
        # Price cache: product_code → {name, standard_price, list_price, ...}
        self.price_cache: Dict[str, Dict[str, Any]] = {}
        
        # Audit trail
        self.audit_log: List[Dict[str, Any]] = []
        
        # Statistics
        self.stats = {
            'products_created': 0,
            'products_updated': 0,
            'products_skipped': 0,
            'supplier_created': 0,
            'supplierinfo_created': 0,
            'supplierinfo_updated': 0,
            'prices_from_struktur': 0,
            'prices_from_csv': 0,
            'prices_fallback': 0,
        }
        
        logger.info(f"ProductsLoader initialized: {self.base_data_dir}")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PRICE LOADING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _load_prices_from_struktur(self) -> Dict[str, Dict[str, Any]]:
        """
        Load prices from Strukturliste (BoM mit Preisen).
        
        Returns:
            Dict: product_code → {name, standard_price, list_price, source}
        """
        csv_path = self.data_normalized_dir / 'Strukturl-ekkiliste-Table_normalized.csv'
        
        # Validate
        CSVValidator.validate_schema(
            csv_path,
            STRUCT_CSV_REQUIRED_COLS,
            delimiter=CSV_DELIM_STRUKTUR
        )
        
        products = {}
        
        log_header("Loading prices from Strukturliste")
        
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=CSV_DELIM_STRUKTUR)
                
                for row_idx, row in enumerate(reader, start=2):
                    try:
                        code = row.get('default_code', '').strip()
                        if not code:
                            continue
                        
                        name = row.get('Artikelbezeichnung', f'Product_{code}').strip()
                        
                        # Parse price
                        price_raw = row.get('Gesamtpreis_raw', '')
                        
                        try:
                            cost_price = PriceParser.parse(price_raw)
                        except PriceParseError as e:
                            logger.warning(f"Row {row_idx}: {e}, skipping")
                            self.stats['prices_fallback'] += 1
                            continue
                        
                        if cost_price <= 0:
                            logger.warning(f"Row {row_idx}: Zero price for {code}")
                            continue
                        
                        # Calculate list price
                        list_price = PricingConfig.calculate_list_price(
                            float(cost_price),
                            markup=PricingConfig.MARKUP_FACTORS.get('finished_good')
                        )
                        
                        products[code] = {
                            'name': name,
                            'standard_price': cost_price,
                            'list_price': Decimal(str(list_price)),
                            'source': 'struktur',
                            'source_row': row_idx,
                            'price_raw': price_raw,
                        }
                        
                        self.stats['prices_from_struktur'] += 1
                        
                        # Audit
                        self._audit_log({
                            'action': 'price_loaded',
                            'source': 'struktur',
                            'product_code': code,
                            'price': float(cost_price),
                            'row': row_idx,
                        })
                    
                    except Exception as e:
                        logger.error(f"Row {row_idx}: Unexpected error: {e}", exc_info=True)
        
        except csv.Error as e:
            raise ProductLoaderError(f"Strukturliste CSV error: {e}")
        
        log_success(f"Loaded {len(products)} products with prices from Strukturliste")
        
        return products
    
    def _audit_log(self, data: Dict[str, Any]) -> None:
        """Add entry to audit log."""
        data['timestamp'] = datetime.now().isoformat()
        self.audit_log.append(data)
    
    def _get_price_for_product(
        self,
        code: str,
        csv_row: Dict[str, str],
        struct_prices: Dict[str, Dict[str, Any]],
    ) -> Tuple[Decimal, Decimal]:
        """
        Get price mit Fallback-Chain:
        1. Struktur (pre-loaded)
        2. CSV-Feld
        3. Hardcoded Fallback
        
        Args:
            code: Product code
            csv_row: CSV row dict
            struct_prices: Pre-loaded Struktur prices
        
        Returns:
            (standard_price, list_price)
        """
        # 1) Try Struktur
        if code in struct_prices:
            sp = struct_prices[code]
            self._audit_log({
                'action': 'price_source',
                'product_code': code,
                'source': 'struktur',
                'price': float(sp['standard_price']),
            })
            return sp['standard_price'], sp['list_price']
        
        # 2) Try CSV field
        csv_price_str = csv_row.get('price', '').strip()
        if csv_price_str:
            try:
                cost_price = PriceParser.parse(csv_price_str)
                list_price = PricingConfig.calculate_list_price(
                    float(cost_price),
                    markup=PricingConfig.MARKUP_FACTORS.get('component')
                )
                
                self.stats['prices_from_csv'] += 1
                self._audit_log({
                    'action': 'price_source',
                    'product_code': code,
                    'source': 'csv',
                    'price': float(cost_price),
                })
                
                return cost_price, Decimal(str(list_price))
            
            except PriceParseError as e:
                logger.warning(f"Price parse error for {code}: {e}")
        
        # 3) Fallback
        self.stats['prices_fallback'] += 1
        self._audit_log({
            'action': 'price_source',
            'product_code': code,
            'source': 'fallback',
            'price': float(PricingConfig.FALLBACK_COST_PRICE),
        })
        
        return (
            Decimal(str(PricingConfig.FALLBACK_COST_PRICE)),
            Decimal(str(PricingConfig.FALLBACK_LIST_PRICE)),
        )
    
    # ═══════════════════════════════════════════════════════════════════════════
    # SUPPLIER MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _ensure_supplier(self, supplier_name: str) -> int:
        """Get or create supplier."""
        supplier_id, is_new = self.client.ensure_record(
            'res.partner',
            [('name', '=', supplier_name), ('supplier_rank', '>', 0)],
            {
                'name': supplier_name,
                'supplier_rank': 1,
                'company_type': 'company',
            },
            unique=False,  # Nicht kritisch wenn mehrere existieren
        )
        
        if is_new:
            self.stats['supplier_created'] += 1
            log_success(f"Created supplier: {supplier_name}")
        
        return supplier_id
    
    def _ensure_supplierinfo(
        self,
        product_id: int,
        supplier_id: int,
        cost_price: Decimal,
    ) -> Tuple[int, bool]:
        """Get or create product.supplierinfo."""
        si_id, is_new = self.client.ensure_record(
            'product.supplierinfo',
            [
                ('product_tmpl_id', '=', product_id),
                ('partner_id', '=', supplier_id),
            ],
            {
                'product_tmpl_id': product_id,
                'partner_id': supplier_id,
                'price': float(cost_price),
                'min_qty': 1,
                'currency_id': 1,  # EUR
                'sequence': 10,
            },
            {
                'price': float(cost_price),
            },
        )
        
        if is_new:
            self.stats['supplierinfo_created'] += 1
        else:
            self.stats['supplierinfo_updated'] += 1
        
        return si_id, is_new
    
    # ═══════════════════════════════════════════════════════════════════════════
    # UOM MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _get_uom_id(self, uom_code: str) -> int:
        """Get UoM by code."""
        uom_name = UOMConfig.MAPPING.get(
            uom_code.lower(),
            UOMConfig.DEFAULT_UOM
        )
        
        result = self.client.search_read(
            'uom.uom',
            [('name', '=', uom_name)],
            ['id'],
            limit=1
        )
        
        if result:
            return result[0]['id']
        
        # Create
        uom_id = self.client.create('uom.uom', {'name': uom_name})
        logger.info(f"Created UoM: {uom_name} → {uom_id}")
        
        return uom_id
    
    # ═══════════════════════════════════════════════════════════════════════════
    # BATCH LOADING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def run(self) -> Dict[str, int]:
        """Main entry point."""
        try:
            log_header("PRODUCTS LOADER")
            
            # 1) Load prices from Struktur
            struct_prices = self._load_prices_from_struktur()
            
            # 2) Load from Stock CSV
            self._load_from_stock_csv(struct_prices)
            
            # 3) Persist audit log
            self._persist_audit_log()
            
            # Return statistics
            log_success(f"Products loader completed")
            log_info(f"Statistics:")
            for key, value in self.stats.items():
                log_info(f"  {key}: {value}")
            
            return self.stats
        
        except Exception as e:
            log_error(f"Products loader failed: {e}", exc_info=True)
            raise
    
    def _load_from_stock_csv(self, struct_prices: Dict[str, Dict[str, Any]]) -> None:
        """Load products from Stock CSV (batch optimized)."""
        csv_path = self.data_normalized_dir / 'Lagerdaten-Table_normalized.csv'
        
        # Validate
        CSVValidator.validate_schema(
            csv_path,
            STOCK_CSV_REQUIRED_COLS,
            delimiter=CSV_DELIM_STOCK
        )
        
        log_header("Loading products from Stock CSV")
        
        # Read all rows first
        products_to_ensure = []
        
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=CSV_DELIM_STOCK)
                
                for row_idx, row in enumerate(reader, start=2):
                    code = row.get('ID', '').strip()
                    
                    if not code:
                        self.stats['products_skipped'] += 1
                        continue
                    
                    name = row.get('name', f'Product_{code}').strip()
                    cost_price, list_price = self._get_price_for_product(
                        code, row, struct_prices
                    )
                    
                    uom_id = self._get_uom_id(row.get('uom', 'stk'))
                    
                    product_vals = {
                        'name': name[:128],
                        'default_code': code,
                        'standard_price': float(cost_price),
                        'list_price': float(list_price),
                        'uom_id': uom_id,
                        'type': 'consu',
                        'sale_ok': True,
                        'purchase_ok': True,
                        'company_id': 1,
                    }
                    
                    products_to_ensure.append({
                        'values': product_vals,
                        'code': code,
                        'cost_price': cost_price,
                    })
        
        except csv.Error as e:
            raise ProductLoaderError(f"Stock CSV error: {e}")
        
        # Batch ensure
        log_info(f"Batch-ensuring {len(products_to_ensure)} products...")
        
        supplier_id = self._ensure_supplier('Drohnen GmbH Internal')
        
        for prod_data in products_to_ensure:
            try:
                code = prod_data['code']
                vals = prod_data['values']
                cost_price = prod_data['cost_price']
                
                # Ensure product
                prod_id, is_new = self.client.ensure_record(
                    'product.template',
                    [('default_code', '=', code)],
                    vals,
                    vals,
                )
                
                if is_new:
                    self.stats['products_created'] += 1
                else:
                    self.stats['products_updated'] += 1
                
                # Ensure supplier info
                self._ensure_supplierinfo(prod_id, supplier_id, cost_price)
                
                log_success(
                    f"{'[NEW]' if is_new else '[UPD]'} {code} "
                    f"'{vals['name'][:40]}' €{float(cost_price):.2f}"
                )
            
            except Exception as e:
                log_error(f"Error processing product {code}: {e}")
                # Nicht abbrechen, weitermachen mit nächstem Produkt
        
        log_success(
            f"Products loaded: "
            f"{self.stats['products_created']} created, "
            f"{self.stats['products_updated']} updated"
        )
    
    def _persist_audit_log(self) -> None:
        """Write audit log to file."""
        import json
        
        audit_path = self.base_data_dir / 'audit' / 'products_audit.json'
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(audit_path, 'w', encoding='utf-8') as f:
                json.dump(self.audit_log, f, indent=2, default=str)
            
            logger.info(f"Audit log written: {audit_path}")
        
        except Exception as e:
            logger.error(f"Failed to write audit log: {e}")

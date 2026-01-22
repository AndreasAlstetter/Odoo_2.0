
"""
ProductsLoader - FIXED v2.1 (Windows UTF-8 Compatible)
═════════════════════════════════════════════════════════════════

🎯 KERNFUNKTIONALITÄT:
─────────────────────
1. ✅ Dual-Code-Suche: Merge ALT (22, L_23) + NEU (000.1.000) Codes
2. ✅ Automatische Deduplizierung: Vereint Duplikate
3. ✅ Typo-Fixing: 008.1.00 → 008.1.000, 59 g → numerischer Code
4. ✅ Preis-Migration: Nutzt NEUEN Preis, löscht ALT-Duplikate
5. ✅ Audit-Trail: Vollständiges Logging aller Operationen
6. ✅ Production-Ready: Fehlerresilienz, Batch-Processing, Performance
7. ✅ WINDOWS KOMPATIBEL: UTF-8 safe, keine Unicode-Fehler
8. ✅ STOCK MOVE SAFE: Archiviert statt zu löschen

⚠️  REGEX-FREE: Nutzt EXACT MATCHING statt ~ Operator!
    (Odoo 19 RPC unterstützt ~ nicht!)

FIX 2.1:
───────
- Keine Unicode Checkmarks/Dashes in log_* Funktionen
- Verwendet ASCII-only Logging
- ARCHIVE statt DELETE für alte Produkte (wegen stock.move constraints)
- Windows cp1252 kompatibel
- Sauberes Error Handling ohne Unicode
"""

import os
import csv
import json
import logging
import re
from pathlib import Path
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, Optional, Tuple, List, Set
from datetime import datetime
from collections import defaultdict


# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING & UTILITIES - WINDOWS COMPATIBLE (ASCII ONLY)
# ═══════════════════════════════════════════════════════════════════════════════

logger = logging.getLogger(__name__)

def log_header(msg: str) -> None:
    logger.info(f"\n{'-' * 80}")
    logger.info(f"  {msg}")
    logger.info(f"{'-' * 80}\n")

def log_success(msg: str) -> None:
    logger.info(f"[OK] {msg}")

def log_warn(msg: str) -> None:
    logger.warning(f"[WARN] {msg}")

def log_error(msg: str, exc: Exception = None) -> None:
    if exc:
        logger.error(f"[ERROR] {msg}\n   {exc}", exc_info=True)
    else:
        logger.error(f"[ERROR] {msg}")


# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

STRUCT_CSV_FILE = 'Strukturstu-eckliste-Table_normalized.csv'
STRUCT_CSV_REQUIRED_COLS = [
    'warehouse_id',
    'Artikelbezeichnung',
    'Gesamtpreis_raw',
    'Artikelart',
    'default_code',
]

CSV_DELIMITER = ','

# TYPO-Fixes
TYPO_FIXES = {
    '008.1.00': '008.1.000',
    '59 g': '019.1.000',
}

# ALT Codes zum Archivieren
ALT_CODES_TO_ARCHIVE = [
    '22', '15', '67', '74', '21', '16', '17', '25', '24', '62', 
    '61', '63', '64', '66', '54', '14', '08', '18', '19', '20',
    'L_23', 'L_24', 'L_25', 'L_26', 'L_27',
    'R_23', 'R_24', 'R_25',
    'V_WHITE_13', 'V_WHITE_14', 'V_WHITE_15',
    'V_BLUE_31', 'V_BLUE_32', 'V_BLUE_33',
    'V_BLACK_75', 'V_BLACK_76',
    'V_RED_45', 'V_RED_46',
    'H_001', 'H_002', 'H_003', 'H_004', 'H_005',
    'F_SMALL_01', 'F_SMALL_02', 'F_LARGE_02', 'F_LARGE_03', 'F_MEDIUM_03',
    'G_BLUE_11', 'G_GREEN_21', 'G_YELLOW_35', 'G_RED_42',
]

ARTICLE_TYPE_MAPPING = {
    'Kaufartikel': 'consu',
    'Lagerartikel': 'consu',
    'Rohstoff': 'consu',
    'Eigenfertigung': 'service',
    'Baugruppe': 'product',
    'consu': 'consu',
    'product': 'product',
    'service': 'service',
}


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class ProductLoaderError(Exception):
    pass

class PriceParseError(ProductLoaderError):
    pass

class CSVError(ProductLoaderError):
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# PRICE PARSER
# ═══════════════════════════════════════════════════════════════════════════════

class PriceParser:
    PRICE_REGEX = re.compile(
        r'(?:EUR|EUR|\$)?\s*'
        r'([0-9]{1,3}(?:[.,][0-9]{3})*[.,][0-9]{2}|[0-9]+[.,][0-9]{2}|[0-9]+)'
        r'(?:\s*(?:EUR|EUR|\$))?',
        re.IGNORECASE
    )
    
    @staticmethod
    def parse(price_str: str) -> Decimal:
        if not price_str or not isinstance(price_str, str):
            raise PriceParseError(f"Invalid input: {repr(price_str)}")
        
        price_str = price_str.strip()
        match = PriceParser.PRICE_REGEX.search(price_str)
        
        if not match:
            raise PriceParseError(f"No price pattern: {price_str}")
        
        price_part = match.group(1)
        
        dot_count = price_part.count('.')
        comma_count = price_part.count(',')
        
        if dot_count > 1 or comma_count > 1:
            if price_part.rfind('.') > price_part.rfind(','):
                price_part = price_part.replace('.', '').replace(',', '.')
            else:
                price_part = price_part.replace(',', '')
        elif comma_count == 1 and dot_count == 0:
            price_part = price_part.replace(',', '.')
        
        try:
            price = Decimal(price_part)
            if price < 0:
                raise PriceParseError(f"Negative price: {price_str}")
            return price.quantize(Decimal('0.01'))
        except (ValueError, InvalidOperation) as e:
            raise PriceParseError(f"Conversion failed: {price_str} -> {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# CSV LOADER
# ═══════════════════════════════════════════════════════════════════════════════

class CSVLoader:
    @staticmethod
    def load(
        csv_path: Path,
        required_cols: List[str],
        delimiter: str = ',',
    ) -> Tuple[Dict[str, Dict[str, Any]], int]:
        
        if not csv_path.exists():
            raise CSVError(f"File not found: {csv_path}")
        
        products = {}
        typos_fixed = 0
        
        try:
            for delim in [delimiter, ';', ',', '\t']:
                with open(csv_path, 'r', encoding='utf-8-sig') as f:
                    try:
                        reader = csv.DictReader(f, delimiter=delim, quotechar='"')
                        
                        if not reader.fieldnames:
                            continue
                        
                        fieldnames = [fn.strip() if fn else fn for fn in reader.fieldnames]
                        missing = [col for col in required_cols if col not in fieldnames]
                        
                        if missing:
                            continue
                        
                        for row_idx, row in enumerate(reader, start=2):
                            warehouse_id = row.get('warehouse_id', '').strip()
                            
                            if not warehouse_id:
                                continue
                            
                            if warehouse_id in TYPO_FIXES:
                                warehouse_id = TYPO_FIXES[warehouse_id]
                                typos_fixed += 1
                            
                            if warehouse_id in products:
                                log_warn(
                                    f"Row {row_idx}: Duplicate {warehouse_id} (keeping first)"
                                )
                                continue
                            
                            row['warehouse_id'] = warehouse_id
                            row['_row'] = row_idx
                            products[warehouse_id] = row
                        
                        if products:
                            log_success(
                                f"CSV loaded: {len(products)} products "
                                f"(delimiter='{delim}', typos_fixed={typos_fixed})"
                            )
                            return products, typos_fixed
                    
                    except csv.Error:
                        continue
            
            raise CSVError(f"Could not parse CSV: {csv_path}")
        
        except Exception as e:
            raise CSVError(f"CSV loading failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# ADVANCED PRODUCTS LOADER - Main Class
# ═══════════════════════════════════════════════════════════════════════════════

class ProductsLoader:
    """Advanced Odoo 19 Product Loader with Merge & Dedup"""
    
    def __init__(self, client, base_data_dir: str):
        self.client = client
        self.base_data_dir = Path(base_data_dir)
        self.data_dir = self.base_data_dir / 'data_normalized'
        
        self.audit_trail: List[Dict[str, Any]] = []
        self.stats = {
            'csv_rows_processed': 0,
            'typos_fixed': 0,
            'duplicates_merged': 0,
            'products_created': 0,
            'products_updated': 0,
            'old_codes_archived': 0,
            'products_skipped': 0,
            'prices_updated': 0,
        }
        
        self._supplier_cache = {}
        self._uom_cache = {}
        
        log_header("ProductsLoader Initialized")
    
    # ═════════════════════════════════════════════════════════════════════════
    # PHASE 1: PREP
    # ═════════════════════════════════════════════════════════════════════════
    
    def _phase1_load_csv(self) -> Tuple[Dict[str, Dict], int]:
        log_header("PHASE 1: PREP - Load CSV, Fix TYPOs, Deduplicate")
        
        csv_path = self.data_dir / STRUCT_CSV_FILE
        products, typos_fixed = CSVLoader.load(
            csv_path,
            STRUCT_CSV_REQUIRED_COLS,
            delimiter=CSV_DELIMITER,
        )
        
        self.stats['csv_rows_processed'] = len(products)
        self.stats['typos_fixed'] = typos_fixed
        
        log_success(f"Phase 1 complete: {len(products)} products loaded")
        return products, typos_fixed
    
    # ═════════════════════════════════════════════════════════════════════════
    # PHASE 2: MERGE
    # ═════════════════════════════════════════════════════════════════════════
    
    def _phase2_merge_codes(self, products: Dict[str, Dict]) -> None:
        log_header("PHASE 2: MERGE - Dual-Code Search (ALT + NEU)")
        
        supplier_id = self._ensure_supplier('Drohnen GmbH Internal')
        
        for idx, (warehouse_id, row) in enumerate(products.items(), 1):
            try:
                old_code = row.get('default_code', '').strip()
                name = row.get('Artikelbezeichnung', f'Product_{warehouse_id}').strip()[:128]
                artikel_art = row.get('Artikelart', 'Kaufartikel').strip()
                price_raw = row.get('Gesamtpreis_raw', '').strip()
                
                if not price_raw:
                    self.stats['products_skipped'] += 1
                    continue
                
                try:
                    cost_price = PriceParser.parse(price_raw)
                except PriceParseError as e:
                    log_warn(f"{warehouse_id}: Price parse failed: {e}")
                    self.stats['products_skipped'] += 1
                    continue
                
                if cost_price < Decimal('0.01'):
                    self.stats['products_skipped'] += 1
                    continue
                
                product_type = ARTICLE_TYPE_MAPPING.get(artikel_art, 'consu')
                
                # Search by NEU code
                existing_neu = self.client.search(
                    'product.template',
                    [('default_code', '=', warehouse_id)],
                    limit=1
                )
                
                if existing_neu:
                    prod_id = existing_neu[0]
                    
                    self.client.write(
                        'product.template',
                        [prod_id],
                        {
                            'standard_price': float(cost_price),
                            'list_price': float(cost_price * Decimal('1.25')),
                        }
                    )
                    
                    self.stats['products_updated'] += 1
                    self._audit_log({
                        'action': 'update_new_code',
                        'warehouse_id': warehouse_id,
                        'product_id': prod_id,
                        'cost_price': float(cost_price),
                    })
                
                else:
                    # Search by ALT code
                    existing_alt = self.client.search(
                        'product.template',
                        [('default_code', '=', old_code)],
                        limit=1
                    )
                    
                    if existing_alt:
                        prod_id = existing_alt[0]
                        
                        self.client.write(
                            'product.template',
                            [prod_id],
                            {
                                'default_code': warehouse_id,
                                'name': name,
                                'standard_price': float(cost_price),
                                'list_price': float(cost_price * Decimal('1.25')),
                                'type': product_type,
                            }
                        )
                        
                        self.stats['duplicates_merged'] += 1
                        self._audit_log({
                            'action': 'merge_old_to_new',
                            'old_code': old_code,
                            'new_code': warehouse_id,
                            'product_id': prod_id,
                            'cost_price': float(cost_price),
                        })
                        
                        log_success(
                            f"[{idx:3d}] MERGE {old_code:12s} -> {warehouse_id} "
                            f"'{name[:35]}' EUR {float(cost_price):.2f}"
                        )
                    
                    else:
                        prod_vals = {
                            'name': name,
                            'default_code': warehouse_id,
                            'standard_price': float(cost_price),
                            'list_price': float(cost_price * Decimal('1.25')),
                            'type': product_type,
                            'uom_id': self._get_uom_id('stk'),
                            'sale_ok': True,
                            'purchase_ok': (artikel_art != 'Eigenfertigung'),
                        }
                        
                        prod_id = self.client.create('product.template', prod_vals)
                        
                        self.stats['products_created'] += 1
                        self._audit_log({
                            'action': 'product_created',
                            'warehouse_id': warehouse_id,
                            'old_code': old_code if old_code else None,
                            'product_type': product_type,
                            'cost_price': float(cost_price),
                        })
                        
                        log_success(
                            f"[{idx:3d}] NEW    {warehouse_id} "
                            f"'{name[:40]}' EUR {float(cost_price):.2f}"
                        )
                
                if prod_id:
                    self._ensure_supplierinfo(prod_id, supplier_id, cost_price)
            
            except Exception as e:
                log_error(f"{warehouse_id}: Processing failed", e)
                self.stats['products_skipped'] += 1
        
        log_success(
            f"Phase 2 complete: "
            f"merged={self.stats['duplicates_merged']}, "
            f"created={self.stats['products_created']}, "
            f"updated={self.stats['products_updated']}"
        )
    
    # ═════════════════════════════════════════════════════════════════════════
    # PHASE 4: ARCHIVE (not DELETE - respects stock.move constraints)
    # ═════════════════════════════════════════════════════════════════════════
    
    def _phase4_archive_old_codes(self) -> None:
        log_header("PHASE 4: ARCHIVE - Archive ALT Codes (No Deletion)")
        
        archived_count = 0
        
        for old_code in ALT_CODES_TO_ARCHIVE:
            try:
                existing = self.client.search(
                    'product.template',
                    [('default_code', '=', old_code)]
                )
                
                if not existing:
                    continue
                
                for prod_id in existing:
                    try:
                        prod_data = self.client.read(
                            'product.template',
                            [prod_id],
                            ['name', 'default_code']
                        )
                        
                        if not prod_data:
                            continue
                        
                        prod_name = prod_data[0]['name']
                        
                        # Check if NEU variant exists
                        neu_variants = self.client.search(
                            'product.template',
                            [
                                ('name', '=', prod_name),
                                ('default_code', '!=', old_code),
                            ]
                        )
                        
                        if neu_variants:
                            # ARCHIVE instead of delete (safe for stock.move)
                            self.client.write(
                                'product.template',
                                [prod_id],
                                {
                                    'active': False,  # Archive, not delete
                                }
                            )
                            
                            self.stats['old_codes_archived'] += 1
                            archived_count += 1
                            
                            self._audit_log({
                                'action': 'archive_old_code',
                                'old_code': old_code,
                                'product_name': prod_name,
                                'new_code_exists': True,
                            })
                            
                            log_success(f"ARCHIVE {old_code} (replaced by NEU)")
                    
                    except Exception as e:
                        log_warn(f"Error archiving {old_code}: {e}")
            
            except Exception as e:
                log_warn(f"Error searching {old_code}: {e}")
        
        log_success(f"Phase 4 complete: {archived_count} old codes archived")
    
    # ═════════════════════════════════════════════════════════════════════════
    # PHASE 5: AUDIT
    # ═════════════════════════════════════════════════════════════════════════
    
    def _phase5_save_audit(self) -> None:
        log_header("PHASE 5: AUDIT - Save Audit Trail")
        
        audit_dir = self.base_data_dir / 'audit'
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        audit_file = audit_dir / 'products_audit_advanced.json'
        
        try:
            with open(audit_file, 'w', encoding='utf-8') as f:
                json.dump(self.audit_trail, f, indent=2, default=str)
            
            log_success(f"Audit trail saved: {audit_file}")
            log_success(f"Total entries: {len(self.audit_trail)}")
        
        except Exception as e:
            log_error(f"Failed to save audit trail", e)
    
    # ═════════════════════════════════════════════════════════════════════════
    # HELPERS
    # ═════════════════════════════════════════════════════════════════════════
    
    def _audit_log(self, data: Dict[str, Any]) -> None:
        data['timestamp'] = datetime.now().isoformat()
        self.audit_trail.append(data)
    
    def _ensure_supplier(self, supplier_name: str) -> int:
        if supplier_name in self._supplier_cache:
            return self._supplier_cache[supplier_name]
        
        existing = self.client.search(
            'res.partner',
            [('name', '=', supplier_name), ('supplier_rank', '>', 0)],
            limit=1
        )
        
        if existing:
            supplier_id = existing[0]
        else:
            supplier_id = self.client.create(
                'res.partner',
                {
                    'name': supplier_name,
                    'supplier_rank': 1,
                    'company_type': 'company',
                }
            )
        
        self._supplier_cache[supplier_name] = supplier_id
        return supplier_id
    
    def _ensure_supplierinfo(
        self,
        product_id: int,
        supplier_id: int,
        cost_price: Decimal,
    ) -> int:
        existing = self.client.search(
            'product.supplierinfo',
            [
                ('product_tmpl_id', '=', product_id),
                ('partner_id', '=', supplier_id),
            ],
            limit=1
        )
        
        vals = {
            'product_tmpl_id': product_id,
            'partner_id': supplier_id,
            'price': float(cost_price),
            'min_qty': 1,
        }
        
        if existing:
            self.client.write('product.supplierinfo', existing, vals)
            return existing[0]
        else:
            si_id = self.client.create('product.supplierinfo', vals)
            return si_id
    
    def _get_uom_id(self, uom_code: str) -> int:
        if uom_code in self._uom_cache:
            return self._uom_cache[uom_code]
        
        uom_name = {
            'stk': 'Stueck',
            'kg': 'kg',
            'm': 'm',
        }.get(uom_code.lower(), 'Stueck')
        
        existing = self.client.search(
            'uom.uom',
            [('name', '=', uom_name)],
            limit=1
        )
        
        if existing:
            uom_id = existing[0]
        else:
            uom_id = self.client.create('uom.uom', {'name': uom_name})
        
        self._uom_cache[uom_code] = uom_id
        return uom_id
    
    # ═════════════════════════════════════════════════════════════════════════
    # MAIN ENTRY POINT
    # ═════════════════════════════════════════════════════════════════════════
    
    def run(self) -> Dict[str, Any]:
        try:
            log_header("PRODUCTS LOADER ADVANCED - COMPLETE WORKFLOW")
            
            products, _ = self._phase1_load_csv()
            
            if not products:
                log_warn("No products loaded")
                return {'status': 'skipped'}
            
            self._phase2_merge_codes(products)
            self._phase4_archive_old_codes()
            self._phase5_save_audit()
            
            log_header("[SUCCESS] PRODUCTS LOADER COMPLETED")
            logger.info("FINAL STATISTICS:")
            for key, value in self.stats.items():
                logger.info(f"  {key:<40} {value}")
            
            return {
                'status': 'success',
                'stats': self.stats,
                'audit_entries': len(self.audit_trail),
            }
        
        except Exception as e:
            log_error("Products loader failed", e)
            raise


# ═════════════════════════════════════════════════════════════════════════════
# STANDALONE EXECUTION
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        encoding='utf-8'  # Windows UTF-8 support
    )
    

"""
supplierinfo_loader.py - Product Supplier Info Loader

Verknüpft Produkte mit Lieferanten-Preisen und Bedingungen:
- Product ↔ Supplier Mapping
- Preise und Min-Mengen
- Währung-Handling
- Fehlerresilienz
"""

import csv
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

from client import OdooClient, RecordAmbiguousError
from config import DataPaths
from utils import log_header, log_info, log_success, log_warn, log_error


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class SupplierInfoError(Exception):
    """Base exception for supplier info operations."""
    pass


class SupplierInfoValidationError(SupplierInfoError):
    """Supplier info validation error."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATOR
# ═══════════════════════════════════════════════════════════════════════════════

class SupplierInfoValidator:
    """Validate supplier info data."""
    
    @staticmethod
    def validate_price(price: float, field_name: str = "price") -> bool:
        """Validate price > 0."""
        if not isinstance(price, (int, float)):
            return False
        return price > 0
    
    @staticmethod
    def validate_min_qty(qty: float) -> bool:
        """Validate min_qty >= 0."""
        if not isinstance(qty, (int, float)):
            return False
        return qty >= 0
    
    @staticmethod
    def validate_sequence(seq: int) -> bool:
        """Validate sequence number."""
        if not isinstance(seq, int):
            return False
        return seq >= 1


# ═══════════════════════════════════════════════════════════════════════════════
# SUPPLIER MAPPER
# ═══════════════════════════════════════════════════════════════════════════════

class SupplierMapper:
    """Map supplier IDs/codes to supplier names."""
    
    def __init__(self, csv_path: Optional[Path] = None):
        """
        Initialize mapper.
        
        Args:
            csv_path: Optional path to supplier_mapping.csv
        """
        self.mapping: Dict[str, str] = {}
        
        if csv_path and csv_path.exists():
            self._load_from_csv(csv_path)
    
    def _load_from_csv(self, csv_path: Path) -> None:
        """Load mapping from CSV file."""
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    supplier_id = row.get('supplier_id', '').strip()
                    supplier_name = row.get('supplier_name', '').strip()
                    
                    if supplier_id and supplier_name:
                        self.mapping[supplier_id] = supplier_name
            
            logger.info(f"Loaded {len(self.mapping)} supplier mappings from {csv_path}")
        
        except Exception as e:
            logger.warning(f"Failed to load supplier mapping CSV: {e}")
    
    def get_supplier_name(self, supplier_id: str) -> Optional[str]:
        """Get supplier name by ID."""
        return self.mapping.get(supplier_id)
    
    def set_mapping(self, supplier_id: str, supplier_name: str) -> None:
        """Add or update mapping."""
        self.mapping[supplier_id] = supplier_name


# ═══════════════════════════════════════════════════════════════════════════════
# SUPPLIER INFO LOADER
# ═══════════════════════════════════════════════════════════════════════════════

class SupplierInfoLoader:
    """Load supplier info (pricing, min quantities) from CSV."""
    
    def __init__(self, client: OdooClient, base_data_dir: str):
        self.client = client
        self.base_data_dir = Path(base_data_dir)
        self.production_dir = self.base_data_dir / 'production_data'
        
        # Supplier mapper
        supplier_mapping_csv = self.base_data_dir / 'data_normalized' / 'supplier_mapping.csv'
        self.supplier_mapper = SupplierMapper(supplier_mapping_csv)
        
        # Currency
        self.currency_id = self._get_currency_id('EUR')
        
        # Statistics
        self.stats = {
            'supplierinfo_created': 0,
            'supplierinfo_updated': 0,
            'rows_processed': 0,
            'rows_skipped': 0,
            'errors_product_not_found': 0,
            'errors_supplier_not_found': 0,
            'errors_invalid_price': 0,
            'errors_invalid_qty': 0,
            'errors': 0,
        }
        
        # Audit log
        self.audit_log: List[Dict[str, Any]] = []
        
        logger.info(f"SupplierInfoLoader initialized")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # LOOKUPS
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _find_product_template(self, default_code: str) -> Optional[int]:
        """Find product template by code."""
        if not default_code:
            return None
        
        products = self.client.search_read(
            'product.template',
            [('default_code', '=', default_code.strip())],
            ['id'],
            limit=1
        )
        
        return products[0]['id'] if products else None
    
    def _find_supplier(self, name: str) -> Optional[int]:
        """Find supplier partner by name."""
        if not name:
            return None
        
        suppliers = self.client.search_read(
            'res.partner',
            [('name', '=', name), ('supplier_rank', '>', 0)],
            ['id'],
            limit=1
        )
        
        return suppliers[0]['id'] if suppliers else None
    
    def _get_currency_id(self, currency_code: str = 'EUR') -> int:
        """Get currency ID."""
        currencies = self.client.search_read(
            'res.currency',
            [('name', '=', currency_code)],
            ['id'],
            limit=1
        )
        
        if currencies:
            return currencies[0]['id']
        
        # Fallback to first currency
        currencies = self.client.search_read(
            'res.currency',
            [],
            ['id'],
            limit=1
        )
        
        return currencies[0]['id'] if currencies else 1
    
    # ═══════════════════════════════════════════════════════════════════════════
    # CSV READING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _read_supplierinfo_csv(self) -> List[Dict[str, str]]:
        """Read supplier info CSV."""
        possible_paths = [
            self.production_dir / 'product_supplierinfo.csv',
            self.base_data_dir / 'product_supplierinfo.csv',
        ]
        
        for csv_path in possible_paths:
            if csv_path.exists():
                logger.info(f"Reading supplier info from: {csv_path}")
                
                rows = []
                
                for encoding in ['utf-8-sig', 'utf-8', 'latin-1']:
                    try:
                        with open(csv_path, 'r', encoding=encoding) as f:
                            reader = csv.DictReader(f, delimiter=',')
                            rows = list(reader)
                        
                        logger.info(f"Read {len(rows)} rows (encoding: {encoding})")
                        return rows
                    
                    except UnicodeDecodeError:
                        continue
                    except csv.Error as e:
                        raise SupplierInfoError(f"CSV parse error: {e}")
        
        logger.warning(f"No supplier info CSV found in: {possible_paths}")
        return []
    
    # ═══════════════════════════════════════════════════════════════════════════
    # DATA BUILDING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _build_supplierinfo_vals(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Build product.supplierinfo values from CSV row."""
        price_raw = row.get('price', '').strip()
        min_qty_raw = row.get('min_qty', '').strip()
        sequence_raw = row.get('sequence', '').strip()
        
        # Parse price (required)
        try:
            price = float(price_raw) if price_raw else 0.0
        except ValueError:
            raise SupplierInfoValidationError(f"Invalid price: {price_raw}")
        
        if not SupplierInfoValidator.validate_price(price):
            raise SupplierInfoValidationError(f"Price must be > 0: {price}")
        
        # Parse min_qty (optional, default 1)
        try:
            min_qty = float(min_qty_raw) if min_qty_raw else 1.0
        except ValueError:
            raise SupplierInfoValidationError(f"Invalid min_qty: {min_qty_raw}")
        
        if not SupplierInfoValidator.validate_min_qty(min_qty):
            raise SupplierInfoValidationError(f"min_qty must be >= 0: {min_qty}")
        
        # Parse sequence (optional, default 10)
        try:
            sequence = int(sequence_raw) if sequence_raw else 10
        except ValueError:
            raise SupplierInfoValidationError(f"Invalid sequence: {sequence_raw}")
        
        if not SupplierInfoValidator.validate_sequence(sequence):
            raise SupplierInfoValidationError(f"Sequence must be >= 1: {sequence}")
        
        # Build vals
        vals = {
            'price': price,
            'min_qty': min_qty,
            'currency_id': self.currency_id,
            'sequence': sequence,
        }
        
        return vals
    
    # ═══════════════════════════════════════════════════════════════════════════
    # LOADING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _load_supplierinfo(self, rows: List[Dict[str, str]]) -> None:
        """Load supplier info from CSV rows."""
        if not rows:
            log_warn("No supplier info rows to load")
            return
        
        log_header("Loading Supplier Info")
        
        for row_idx, row in enumerate(rows, start=2):
            try:
                self.stats['rows_processed'] += 1
                
                # Extract fields
                product_code = row.get('product_code', '').strip() or row.get('default_code', '').strip()
                supplier_id = row.get('supplier_id', '').strip()
                
                if not product_code or not supplier_id:
                    logger.debug(f"Row {row_idx}: missing product_code or supplier_id")
                    self.stats['rows_skipped'] += 1
                    continue
                
                # Map supplier ID to name
                supplier_name = self.supplier_mapper.get_supplier_name(supplier_id)
                
                if not supplier_name:
                    logger.warning(f"Row {row_idx}: supplier_id '{supplier_id}' not mapped")
                    self.stats['rows_skipped'] += 1
                    continue
                
                # Find product
                product_id = self._find_product_template(product_code)
                if not product_id:
                    logger.warning(f"Row {row_idx}: product '{product_code}' not found")
                    self.stats['errors_product_not_found'] += 1
                    self.stats['rows_skipped'] += 1
                    continue
                
                # Find supplier
                supplier_partner_id = self._find_supplier(supplier_name)
                if not supplier_partner_id:
                    logger.warning(f"Row {row_idx}: supplier '{supplier_name}' not found")
                    self.stats['errors_supplier_not_found'] += 1
                    self.stats['rows_skipped'] += 1
                    continue
                
                # Validate and build vals
                try:
                    vals = self._build_supplierinfo_vals(row)
                except SupplierInfoValidationError as e:
                    logger.warning(f"Row {row_idx}: {e}")
                    if 'price' in str(e):
                        self.stats['errors_invalid_price'] += 1
                    elif 'qty' in str(e):
                        self.stats['errors_invalid_qty'] += 1
                    else:
                        self.stats['errors'] += 1
                    self.stats['rows_skipped'] += 1
                    continue
                
                # Complete vals
                vals.update({
                    'product_tmpl_id': product_id,
                    'partner_id': supplier_partner_id,
                })
                
                # Ensure in Odoo
                domain = [
                    ('product_tmpl_id', '=', product_id),
                    ('partner_id', '=', supplier_partner_id),
                ]
                
                try:
                    si_id, is_new = self.client.ensure_record(
                        'product.supplierinfo',
                        domain,
                        vals,
                        vals,  # Update same vals
                    )
                    
                    if is_new:
                        self.stats['supplierinfo_created'] += 1
                        log_success(f"[NEW] {product_code}/{supplier_name} → {si_id}")
                    else:
                        self.stats['supplierinfo_updated'] += 1
                        log_info(f"[UPD] {product_code}/{supplier_name} → {si_id}")
                    
                    self._audit_log({
                        'action': 'created' if is_new else 'updated',
                        'supplierinfo_id': si_id,
                        'product_code': product_code,
                        'supplier_name': supplier_name,
                        'price': vals['price'],
                        'min_qty': vals['min_qty'],
                        'csv_row': row_idx,
                    })
                
                except RecordAmbiguousError as e:
                    logger.error(f"Row {row_idx}: {e}")
                    self.stats['errors'] += 1
            
            except Exception as e:
                logger.error(f"Row {row_idx}: {e}", exc_info=True)
                self.stats['errors'] += 1
        
        log_success(
            f"Supplier info loaded: "
            f"{self.stats['supplierinfo_created']} created, "
            f"{self.stats['supplierinfo_updated']} updated, "
            f"{self.stats['rows_skipped']} skipped"
        )
    
    # ═══════════════════════════════════════════════════════════════════════════
    # AUDIT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _audit_log(self, data: Dict[str, Any]) -> None:
        """Add audit entry."""
        data['timestamp'] = datetime.now().isoformat()
        self.audit_log.append(data)
    
    def _persist_audit_log(self) -> None:
        """Write audit log to file."""
        import json
        
        audit_path = self.base_data_dir / 'audit' / 'supplierinfo_audit.json'
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(audit_path, 'w') as f:
                json.dump(self.audit_log, f, indent=2, default=str)
            logger.info(f"Audit log: {audit_path}")
        except Exception as e:
            logger.error(f"Failed to write audit log: {e}")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # MAIN
    # ═══════════════════════════════════════════════════════════════════════════
    
    def run(self) -> Dict[str, int]:
        """Main entry point."""
        try:
            log_header("SUPPLIER INFO LOADER")
            
            # Read CSV
            rows = self._read_supplierinfo_csv()
            
            if not rows:
                log_warn("No supplier info CSV found, skipping")
                return {'skipped': True}
            
            # Load
            self._load_supplierinfo(rows)
            
            # Persist audit
            self._persist_audit_log()
            
            # Summary
            log_success("Supplier info loader completed")
            log_info("Statistics:")
            for key, value in self.stats.items():
                log_info(f"  {key}: {value}")
            
            return self.stats
        
        except Exception as e:
            log_error(f"Supplier info loader failed: {e}", exc_info=True)
            raise

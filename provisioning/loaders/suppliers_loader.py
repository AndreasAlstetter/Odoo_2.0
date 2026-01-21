"""
suppliers_loader.py - Supplier (Vendor) Master Data Loader

Lädt Lieferanten aus CSV mit:
- Schema-Validierung
- Email/Phone/Address Validierung
- Selektives Update (nicht alles überschreiben)
- Deduplication
- Fehlerresilienz
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Set
from datetime import datetime

from client import OdooClient, RecordAmbiguousError
from config import DataPaths
from utils import log_header, log_info, log_success, log_warn, log_error


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class SupplierError(Exception):
    """Base exception for supplier operations."""
    pass


class SupplierValidationError(SupplierError):
    """Supplier data validation error."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATOR
# ═══════════════════════════════════════════════════════════════════════════════

class SupplierValidator:
    """Validate supplier data."""
    
    @staticmethod
    def validate_email(email: str) -> bool:
        """Validate email format."""
        if not email or not isinstance(email, str):
            return False
        
        email = email.strip()
        
        # Basic validation
        if '@' not in email or '.' not in email:
            return False
        
        if len(email) < 5 or len(email) > 254:
            return False
        
        return True
    
    @staticmethod
    def validate_phone(phone: str) -> bool:
        """Validate phone format."""
        if not phone or not isinstance(phone, str):
            return False
        
        phone = phone.strip()
        
        # At least 5 digits/symbols
        if len(phone) < 5:
            return False
        
        # Max 20 chars
        if len(phone) > 20:
            return False
        
        return True
    
    @staticmethod
    def validate_name(name: str) -> bool:
        """Validate supplier name."""
        if not name or not isinstance(name, str):
            return False
        
        name = name.strip()
        
        # Not generic
        if name.lower() in ['unnamed', 'unknown', 'supplier', 'vendor']:
            return False
        
        # Min length
        if len(name) < 2:
            return False
        
        # Max length
        if len(name) > 255:
            return False
        
        return True


# ═══════════════════════════════════════════════════════════════════════════════
# COLUMN MAPPER
# ═══════════════════════════════════════════════════════════════════════════════

class ColumnMapper:
    """Map CSV columns to Odoo partner fields."""
    
    # Possible column names (in priority order)
    COLUMN_MAPPING = {
        'name': ['Lieferant', 'Supplier Name', 'name', 'supplier_name'],
        'email': ['Email', 'E-Mail', 'email', 'EmailAddress'],
        'phone': ['Telefon', 'Phone', 'phone', 'Phone Number'],
        'address': ['Adresse', 'Street', 'address', 'Street Address', 'Strasse'],
        'city': ['Stadt', 'City', 'city'],
        'country': ['Land', 'Country', 'country'],
    }
    
    @staticmethod
    def get_field(row: Dict[str, str], field_name: str) -> Optional[str]:
        """
        Get field value from row with multiple column name fallbacks.
        
        Args:
            row: CSV row dict
            field_name: 'name', 'email', 'phone', etc.
        
        Returns:
            Value or None if not found/empty
        """
        possible_cols = ColumnMapper.COLUMN_MAPPING.get(field_name, [])
        
        for col_name in possible_cols:
            if col_name in row:
                value = row[col_name]
                if isinstance(value, str):
                    value = value.strip()
                    if value:
                        return value
        
        return None
    
    @staticmethod
    def validate_schema(first_row: Dict[str, str]) -> None:
        """
        Validate CSV schema.
        
        Raises:
            SupplierValidationError: Wenn Schema ungültig
        """
        if not first_row:
            raise SupplierValidationError("CSV is empty")
        
        # At least one name column must exist
        name_cols = ColumnMapper.COLUMN_MAPPING['name']
        actual_cols = set(first_row.keys())
        
        if not any(col in actual_cols for col in name_cols):
            raise SupplierValidationError(
                f"CSV missing supplier name column. "
                f"Expected one of: {name_cols}, "
                f"got: {list(actual_cols)}"
            )


# ═══════════════════════════════════════════════════════════════════════════════
# SUPPLIERS LOADER
# ═══════════════════════════════════════════════════════════════════════════════

class SuppliersLoader:
    """Load supplier master data from CSV."""
    
    def __init__(self, client: OdooClient, base_data_dir: str):
        self.client = client
        self.base_data_dir = Path(base_data_dir)
        self.data_normalized_dir = self.base_data_dir / 'data_normalized'
        
        # Statistics
        self.stats = {
            'suppliers_created': 0,
            'suppliers_updated': 0,
            'suppliers_skipped': 0,
            'errors': 0,
        }
        
        # Audit log
        self.audit_log: List[Dict[str, Any]] = []
        
        logger.info(f"SuppliersLoader initialized: {self.base_data_dir}")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # CSV READING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _read_suppliers_csv(self) -> List[Dict[str, str]]:
        """Read suppliers CSV with multiple path fallbacks."""
        possible_paths = [
            self.data_normalized_dir / 'Lieferanten-Table_normalized.csv',
            self.data_normalized_dir / 'Lieferanten-Table.normalized.csv',
            self.base_data_dir / 'Lieferanten.csv',
        ]
        
        for csv_path in possible_paths:
            if csv_path.exists():
                logger.info(f"Reading suppliers from: {csv_path}")
                
                import csv
                rows = []
                
                for encoding in ['utf-8-sig', 'utf-8', 'latin-1']:
                    try:
                        with open(csv_path, 'r', encoding=encoding) as f:
                            reader = csv.DictReader(f, delimiter=';')
                            rows = list(reader)
                        
                        logger.info(f"Read {len(rows)} rows (encoding: {encoding})")
                        return rows
                    
                    except UnicodeDecodeError:
                        continue
                    except csv.Error as e:
                        raise SupplierError(f"CSV parse error: {e}")
        
        logger.warning(f"No suppliers CSV found in: {possible_paths}")
        return []
    
    # ═══════════════════════════════════════════════════════════════════════════
    # DATA BUILDING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _build_partner_vals(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Build Odoo partner.vals from CSV row."""
        name = ColumnMapper.get_field(row, 'name')
        email = ColumnMapper.get_field(row, 'email')
        phone = ColumnMapper.get_field(row, 'phone')
        address = ColumnMapper.get_field(row, 'address')
        city = ColumnMapper.get_field(row, 'city')
        country = ColumnMapper.get_field(row, 'country')
        
        # Validate name (required)
        if not SupplierValidator.validate_name(name):
            raise SupplierValidationError(f"Invalid supplier name: {name}")
        
        # Validate optional fields
        if email and not SupplierValidator.validate_email(email):
            logger.warning(f"Invalid email for {name}: {email}, ignoring")
            email = None
        
        if phone and not SupplierValidator.validate_phone(phone):
            logger.warning(f"Invalid phone for {name}: {phone}, ignoring")
            phone = None
        
        # Build vals
        vals = {
            'name': name,
            'supplier_rank': 1,  # Mark as supplier
            'customer_rank': 0,  # Not customer
            'is_company': True,
        }
        
        # Add optional fields (only if valid)
        if email:
            vals['email'] = email
        
        if phone:
            vals['phone'] = phone
        
        if address:
            vals['street'] = address
        
        if city:
            vals['city'] = city
        
        if country:
            vals['country_id'] = self._get_country_id(country)
        
        return vals
    
    def _get_country_id(self, country_code_or_name: str) -> Optional[int]:
        """Get country ID by code or name."""
        if not country_code_or_name:
            return None
        
        # Try code first (e.g., 'DE')
        countries = self.client.search_read(
            'res.country',
            [('code', '=', country_code_or_name.upper())],
            ['id'],
            limit=1
        )
        
        if countries:
            return countries[0]['id']
        
        # Try name
        countries = self.client.search_read(
            'res.country',
            [('name', 'ilike', country_code_or_name)],
            ['id'],
            limit=1
        )
        
        if countries:
            return countries[0]['id']
        
        logger.warning(f"Country not found: {country_code_or_name}")
        return None
    
    # ═══════════════════════════════════════════════════════════════════════════
    # LOADING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _load_suppliers(self, rows: List[Dict[str, str]]) -> None:
        """Load suppliers from CSV rows."""
        if not rows:
            log_warn("No supplier rows to load")
            return
        
        log_header("Loading Suppliers")
        
        # Validate schema
        try:
            ColumnMapper.validate_schema(rows[0])
        except SupplierValidationError as e:
            raise SupplierError(f"CSV schema invalid: {e}")
        
        # Deduplicate
        seen_names: Set[str] = set()
        
        for row_idx, row in enumerate(rows, start=2):
            try:
                # Get name
                name = ColumnMapper.get_field(row, 'name')
                
                if not name:
                    logger.debug(f"Row {row_idx}: missing supplier name")
                    self.stats['suppliers_skipped'] += 1
                    continue
                
                # Deduplicate
                if name in seen_names:
                    logger.debug(f"Row {row_idx}: duplicate supplier '{name}'")
                    self.stats['suppliers_skipped'] += 1
                    continue
                
                seen_names.add(name)
                
                # Validate & build
                try:
                    vals = self._build_partner_vals(row)
                except SupplierValidationError as e:
                    logger.warning(f"Row {row_idx}: {e}")
                    self.stats['suppliers_skipped'] += 1
                    continue
                
                # Ensure in Odoo
                domain = [('name', '=', name)]
                
                try:
                    partner_id, is_new = self.client.ensure_record(
                        'res.partner',
                        domain,
                        vals,
                        vals,
                    )
                    
                    if is_new:
                        self.stats['suppliers_created'] += 1
                        log_success(f"[NEW] {name} → {partner_id}")
                    else:
                        self.stats['suppliers_updated'] += 1
                        log_info(f"[UPD] {name} → {partner_id}")
                    
                    self._audit_log({
                        'action': 'created' if is_new else 'updated',
                        'supplier_id': partner_id,
                        'supplier_name': name,
                        'csv_row': row_idx,
                    })
                
                except RecordAmbiguousError as e:
                    logger.error(f"Row {row_idx}: {e}")
                    self.stats['errors'] += 1
                    continue
            
            except Exception as e:
                logger.error(f"Row {row_idx}: {e}", exc_info=True)
                self.stats['errors'] += 1
                continue
        
        log_success(
            f"Suppliers loaded: "
            f"{self.stats['suppliers_created']} created, "
            f"{self.stats['suppliers_updated']} updated, "
            f"{self.stats['suppliers_skipped']} skipped, "
            f"{self.stats['errors']} errors"
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
        
        audit_path = self.base_data_dir / 'audit' / 'suppliers_audit.json'
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
            log_header("SUPPLIERS LOADER")
            
            # Read CSV
            rows = self._read_suppliers_csv()
            
            if not rows:
                log_warn("No suppliers CSV found, skipping")
                return {'skipped': True}
            
            # Load
            self._load_suppliers(rows)
            
            # Persist audit
            self._persist_audit_log()
            
            # Summary
            log_success("Suppliers loader completed")
            log_info("Statistics:")
            for key, value in self.stats.items():
                log_info(f"  {key}: {value}")
            
            return self.stats
        
        except Exception as e:
            log_error(f"Suppliers loader failed: {e}", exc_info=True)
            raise

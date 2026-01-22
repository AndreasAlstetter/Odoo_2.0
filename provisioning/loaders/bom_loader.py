"""
bom_loader.py - Bill of Materials Loader (FIXED)

Lädt BoMs aus CSV mit:
- Korrekte Template/Varianten-Trennung
- Interne Referenzen (default_code ohne EVO Prefix)
- Bulk-Create statt Single-Create
- Konsistenz mit ProductTemplates
"""

import csv
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Tuple, Literal
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from datetime import datetime

from provisioning.client import OdooClient
from provisioning.config import DataPaths, ProductTemplates
from provisioning.utils import log_header, log_success, log_info, log_warn, log_error

logger = logging.getLogger(__name__)

# Expected product codes from ProductTemplates
EXPECTED_PRODUCT_CODES = set(ProductTemplates.ALL_CODES)

BATCH_SIZE = 500
BOM_CONFIG = {
    'filename': DataPaths.BOM_DEFAULT,
    'delimiter': ',',
    'encoding': 'utf-8',
    'uom_name': 'Units',
    'bom_type': 'normal',
}


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class BomError(Exception):
    """Base exception for BoM operations."""
    pass

class BomValidationError(BomError):
    """BoM data validation error."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATOR
# ═══════════════════════════════════════════════════════════════════════════════

class BomValidator:
    """Validate BoM data."""
    
    @staticmethod
    def validate_quantity(qty_str: str, min_qty: Decimal = Decimal('0.001')) -> Decimal:
        """Validate and parse quantity."""
        if not qty_str or not qty_str.strip():
            raise BomValidationError("Quantity is empty")
        
        try:
            qty = Decimal(qty_str.strip())
        except (ValueError, InvalidOperation):
            raise BomValidationError(f"Invalid quantity format: {qty_str}")
        
        if qty < min_qty:
            raise BomValidationError(f"Quantity must be >= {min_qty}, got: {qty}")
        
        return qty
    
    @staticmethod
    def validate_sequence(seq_str: str) -> int:
        """Validate sequence number."""
        if not seq_str or not seq_str.strip():
            return 10
        
        try:
            seq = int(seq_str.strip())
        except ValueError:
            raise BomValidationError(f"Invalid sequence: {seq_str}")
        
        if seq < 1:
            raise BomValidationError(f"Sequence must be >= 1, got: {seq}")
        
        return seq


# ═══════════════════════════════════════════════════════════════════════════════
# CSV READER
# ═══════════════════════════════════════════════════════════════════════════════

class BomCsvReader:
    """Read and validate BoM CSV with format detection."""
    
    REQUIRED_COLUMNS_SIMPLE = {
        'bom_id', 'bom_name', 'product_code',
        'product_qty', 'component_code', 'component_qty', 'sequence',
    }
    
    REQUIRED_COLUMNS_ODOO = {
        'id', 'product_tmpl_id/default_code', 'product_qty',
        'bom_line_ids/id', 'bom_line_ids/product_id/default_code',
        'bom_line_ids/product_qty',
    }
    
    @staticmethod
    def detect_format(fieldnames: List[str]) -> Literal['simple', 'odoo_native']:
        """Detect CSV format."""
        cols = set(fieldnames)
        
        if BomCsvReader.REQUIRED_COLUMNS_ODOO.issubset(cols):
            return 'odoo_native'
        
        if BomCsvReader.REQUIRED_COLUMNS_SIMPLE.issubset(cols):
            return 'simple'
        
        raise BomError(
            f"CSV format not recognized.\n"
            f"Expected columns:\n"
            f"  Simple: {BomCsvReader.REQUIRED_COLUMNS_SIMPLE}\n"
            f"  Odoo: {BomCsvReader.REQUIRED_COLUMNS_ODOO}\n"
            f"Got: {cols}"
        )
    
    @staticmethod
    def read_and_validate(csv_path: Path) -> Tuple[List[Dict[str, str]], Literal['simple', 'odoo_native']]:
        """Read CSV with format detection."""
        if not csv_path.exists():
            raise BomError(f"BoM CSV not found: {csv_path}")
        
        for encoding in ['utf-8-sig', 'utf-8', 'latin-1']:
            try:
                with open(csv_path, 'r', encoding=encoding, newline='') as f:
                    for delimiter in [',', ';', '\t']:
                        f.seek(0)
                        reader = csv.DictReader(f, delimiter=delimiter, quotechar='"')
                        
                        if not reader.fieldnames:
                            continue
                        
                        try:
                            fmt = BomCsvReader.detect_format(reader.fieldnames)
                            rows = list(reader)
                            logger.info(
                                f"Read {len(rows)} BoM rows "
                                f"(format: {fmt}, encoding: {encoding})"
                            )
                            return rows, fmt
                        except BomError:
                            continue
            
            except (UnicodeDecodeError, csv.Error):
                continue
        
        raise BomError(f"Failed to read {csv_path} - format not recognized")


# ═══════════════════════════════════════════════════════════════════════════════
# BoM LOADER
# ═══════════════════════════════════════════════════════════════════════════════

class BomLoader:
    """Load Bill of Materials from CSV."""
    
    def __init__(self, client: OdooClient, base_data_dir: str):
        self.client = client
        self.base_data_dir = Path(base_data_dir)
        self.bom_dir = self.base_data_dir / 'bom'
        
        self.uom_id = self._get_or_create_uom()
        
        # Cache: code → id (BEIDE Templates UND Varianten!)
        self.product_tmpl_cache: Dict[str, int] = {}
        self.product_variant_cache: Dict[str, int] = {}
        self.bom_cache: Dict[str, int] = {}
        
        self.stats = {
            'bom_created': 0,
            'bom_updated': 0,
            'bom_lines_created': 0,
            'errors': 0,
        }
        
        logger.info(f"BomLoader initialized: {self.bom_dir}")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # SETUP
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _get_or_create_uom(self) -> int:
        """Get or create UoM 'Units'."""
        uom_name = BOM_CONFIG.get('uom_name', 'Units')
        
        uoms = self.client.search_read(
            'uom.uom',
            [('name', '=', uom_name)],
            ['id'],
            limit=1
        )
        
        if uoms:
            logger.info(f"Using existing UoM: {uom_name} (id={uoms[0]['id']})")
            return uoms[0]['id']
        
        logger.warning(f"UoM '{uom_name}' not found, creating...")
        uom_id = self.client.create(
            'uom.uom',
            {
                'name': uom_name,
                'category_id': 1,
                'rounding': 0.01,
            }
        )
        
        logger.info(f"Created UoM: {uom_name} (id={uom_id})")
        return uom_id
    
    def _prefetch_products(self, rows: List[Dict[str, str]], fmt: Literal['simple', 'odoo_native']) -> None:
        """Pre-fetch all products (Templates AND Variants) in batch."""
        tmpl_codes: Set[str] = set()
        component_codes: Set[str] = set()
        
        if fmt == 'odoo_native':
            for row in rows:
                if code := row.get('product_tmpl_id/default_code', '').strip():
                    tmpl_codes.add(code)
                if code := row.get('bom_line_ids/product_id/default_code', '').strip():
                    component_codes.add(code)
        else:
            for row in rows:
                if code := row.get('product_code', '').strip():
                    tmpl_codes.add(code)
                if code := row.get('component_code', '').strip():
                    component_codes.add(code)
        
        # Fetch TEMPLATES (parents)
        if tmpl_codes:
            templates = self.client.search_read(
                'product.template',
                [('default_code', 'in', list(tmpl_codes))],
                ['default_code', 'id', 'name'],
            )
            
            for tmpl in templates:
                self.product_tmpl_cache[tmpl['default_code']] = tmpl['id']
                log_info(f"  Template: {tmpl['default_code']} → {tmpl['name']} (id={tmpl['id']})")
        
        # Fetch VARIANTS (components) - IMPORTANT: product.product, nicht template!
        if component_codes:
            variants = self.client.search_read(
                'product.product',  # ← Varianten!
                [('default_code', 'in', list(component_codes))],
                ['default_code', 'id', 'name'],
            )
            
            for variant in variants:
                self.product_variant_cache[variant['default_code']] = variant['id']
                log_info(f"  Variant: {variant['default_code']} → {variant['name']} (id={variant['id']})")
        
        # Validation: Alle erwarteten Produkte gefunden?
        found_templates = set(self.product_tmpl_cache.keys())
        missing_templates = EXPECTED_PRODUCT_CODES - found_templates
        
        if missing_templates:
            log_warn(f"Missing expected products in Odoo: {missing_templates}")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # LOADING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _load_boms(self, rows: List[Dict[str, str]], fmt: Literal['simple', 'odoo_native']) -> None:
        """Load BoMs from CSV rows."""
        if not rows:
            log_warn("No BoM rows to load")
            return
        
        log_header("Loading Bills of Materials")
        self._prefetch_products(rows, fmt)
        
        # Group by BoM
        bom_groups: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        
        for row in rows:
            bom_id = row.get('id' if fmt == 'odoo_native' else 'bom_id', '').strip()
            if bom_id:
                bom_groups[bom_id].append(row)
        
        logger.info(f"Processing {len(bom_groups)} BoMs ({fmt} format)")
        
        # Process each BoM
        for bom_id, group_rows in bom_groups.items():
            try:
                if fmt == 'odoo_native':
                    self._process_bom_odoo(bom_id, group_rows)
                else:
                    self._process_bom_simple(bom_id, group_rows)
            except Exception as e:
                logger.error(f"Failed to process BoM {bom_id}: {e}", exc_info=True)
                self.stats['errors'] += 1
        
        log_success(
            f"BoM loading completed: "
            f"{self.stats['bom_created']} created, "
            f"{self.stats['bom_updated']} updated, "
            f"{self.stats['bom_lines_created']} lines created"
        )
    
    # ─────────────────────────────────────────────────────────────────────────
    # Simple Format
    # ─────────────────────────────────────────────────────────────────────────
    
    def _process_bom_simple(self, bom_id: str, rows: List[Dict[str, str]]) -> None:
        """Process BoM from simple format."""
        header_row = rows[0]
        product_code = header_row.get('product_code', '').strip()
        bom_name = header_row.get('bom_name', f"BoM {bom_id}").strip()
        
        if not product_code:
            logger.warning(f"BoM {bom_id}: missing product_code")
            return
        
        # Find parent template
        product_tmpl_id = self.product_tmpl_cache.get(product_code)
        if not product_tmpl_id:
            logger.error(f"BoM {bom_id}: product '{product_code}' not found in Odoo")
            self.stats['errors'] += 1
            return
        
        try:
            product_qty = BomValidator.validate_quantity(
                header_row.get('product_qty', '1').strip()
            )
        except BomValidationError as e:
            logger.error(f"BoM {bom_id}: {e}")
            self.stats['errors'] += 1
            return
        
        # Get or create BoM header
        bom_header_id, is_new = self._ensure_bom_header(
            product_tmpl_id, float(product_qty)
        )
        
        if is_new:
            self.stats['bom_created'] += 1
            log_success(f"[BOM:NEW] {product_code} → BoM id={bom_header_id}")
        else:
            self.stats['bom_updated'] += 1
            log_info(f"[BOM:UPD] {product_code} → BoM id={bom_header_id}")
        
        # Process lines
        self._batch_create_lines(bom_header_id, bom_id, rows)
    
    def _process_bom_odoo(self, bom_id: str, rows: List[Dict[str, str]]) -> None:
        """Process BoM from Odoo native format."""
        header_row = rows[0]
        product_code = header_row.get('product_tmpl_id/default_code', '').strip()
        
        if not product_code:
            logger.warning(f"BoM {bom_id}: missing product_tmpl_id/default_code")
            return
        
        product_tmpl_id = self.product_tmpl_cache.get(product_code)
        if not product_tmpl_id:
            logger.error(f"BoM {bom_id}: product '{product_code}' not found in Odoo")
            self.stats['errors'] += 1
            return
        
        try:
            product_qty = BomValidator.validate_quantity(
                header_row.get('product_qty', '1').strip()
            )
        except BomValidationError as e:
            logger.error(f"BoM {bom_id}: {e}")
            self.stats['errors'] += 1
            return
        
        bom_header_id, is_new = self._ensure_bom_header(
            product_tmpl_id, float(product_qty)
        )
        
        if is_new:
            self.stats['bom_created'] += 1
            log_success(f"[BOM:NEW] {product_code} → BoM id={bom_header_id}")
        else:
            self.stats['bom_updated'] += 1
            log_info(f"[BOM:UPD] {product_code} → BoM id={bom_header_id}")
        
        self._batch_create_lines(bom_header_id, bom_id, rows)
    
    # ═══════════════════════════════════════════════════════════════════════════
    # SHARED: BoM Header & Lines
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _ensure_bom_header(self, product_tmpl_id: int, product_qty: float) -> Tuple[int, bool]:
        """Get or create BoM header."""
        boms = self.client.search_read(
            'mrp.bom',
            [('product_tmpl_id', '=', product_tmpl_id)],
            ['id'],
            limit=1
        )
        
        if boms:
            bom_id = boms[0]['id']
            self.client.write('mrp.bom', [bom_id], {
                'product_qty': product_qty,
                'product_uom_id': self.uom_id,
            })
            return bom_id, False
        
        # Create new
        bom_id = self.client.create('mrp.bom', {
            'product_tmpl_id': product_tmpl_id,
            'product_qty': product_qty,
            'product_uom_id': self.uom_id,
            'type': BOM_CONFIG.get('bom_type', 'normal'),
        })
        
        return bom_id, True
    
    def _batch_create_lines(
        self,
        bom_header_id: int,
        bom_id: str,
        rows: List[Dict[str, str]],
    ) -> None:
        """Batch create BoM lines with deduplication."""
        # Determine format from first row
        fmt = 'odoo_native' if 'bom_line_ids/product_id/default_code' in rows[0] else 'simple'
        
        # Delete existing lines
        existing = self.client.search(
            'mrp.bom.line',
            [('bom_id', '=', bom_header_id)],
        )
        
        if existing:
            self.client.unlink('mrp.bom.line', existing)
            logger.debug(f"Deleted {len(existing)} old lines for BoM {bom_id}")
        
        # Collect unique components
        seen_components: Set[str] = set()
        line_vals_list: List[Dict[str, Any]] = []
        
        for row_idx, row in enumerate(rows, start=1):
            if fmt == 'odoo_native':
                component_code = row.get('bom_line_ids/product_id/default_code', '').strip()
                component_qty_str = row.get('bom_line_ids/product_qty', '1').strip()
            else:
                component_code = row.get('component_code', '').strip()
                component_qty_str = row.get('component_qty', '1').strip()
            
            if not component_code:
                logger.debug(f"Row {row_idx}: missing component_code")
                continue
            
            # Deduplicate
            if component_code in seen_components:
                logger.warning(f"Row {row_idx}: duplicate component '{component_code}'")
                continue
            
            seen_components.add(component_code)
            
            # Find component variant
            component_id = self.product_variant_cache.get(component_code)
            if not component_id:
                logger.error(f"Row {row_idx}: component '{component_code}' not found in Odoo")
                self.stats['errors'] += 1
                continue
            
            # Validate quantity
            try:
                component_qty = BomValidator.validate_quantity(component_qty_str)
            except BomValidationError as e:
                logger.error(f"Row {row_idx}, component '{component_code}': {e}")
                self.stats['errors'] += 1
                continue
            
            # Validate sequence
            try:
                seq_str = row.get('sequence', '10').strip() if fmt == 'simple' else '10'
                sequence = BomValidator.validate_sequence(seq_str)
            except BomValidationError:
                sequence = 10
            
            line_vals_list.append({
                'bom_id': bom_header_id,
                'product_id': component_id,
                'product_qty': float(component_qty),
                'product_uom_id': self.uom_id,
                'sequence': sequence,
            })
        
        # ✅ BULK CREATE (not single!)
        if line_vals_list:
            try:
                created_ids = self.client.create_batch('mrp.bom.line', line_vals_list)
                self.stats['bom_lines_created'] += len(created_ids)
                log_success(f"Created {len(created_ids)} lines for BoM {bom_id}")
            except Exception as e:
                # Fallback: single create
                logger.warning(f"Bulk create failed for BoM {bom_id}, using single creates: {e}")
                for line_vals in line_vals_list:
                    try:
                        self.client.create('mrp.bom.line', line_vals)
                        self.stats['bom_lines_created'] += 1
                    except Exception as e2:
                        logger.error(f"Failed to create line: {e2}")
                        self.stats['errors'] += 1
    
    # ═══════════════════════════════════════════════════════════════════════════
    # MAIN
    # ═══════════════════════════════════════════════════════════════════════════
    
    def run(self, filename: str = 'bom.csv') -> Dict[str, int]:
        """Main entry point."""
        try:
            log_header("BOM LOADER")
            
            csv_path = self.bom_dir / filename
            rows, fmt = BomCsvReader.read_and_validate(csv_path)
            
            if not rows:
                log_warn("No BoM rows found")
                return self.stats
            
            logger.info(f"Detected CSV format: {fmt}")
            self._load_boms(rows, fmt)
            
            log_success("BoM loader completed")
            logger.info("Statistics:")
            for key, value in self.stats.items():
                logger.info(f"  {key}: {value}")
            
            return self.stats
        
        except Exception as e:
            log_error(f"BoM loader failed: {e}", exc_info=True)
            raise

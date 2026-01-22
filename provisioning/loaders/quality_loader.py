"""
quality_loader.py - Quality Control Point Loader

Setzt up Qualitätsprüfpunkte mit:
- Product + Operation Linkage
- Test Type Definitions
- Pass/Fail Thresholds
- Audit Trail
- Fehlerresilienz für >500/day Produktion
"""

import csv
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from decimal import Decimal
from datetime import datetime

from provisioning.client import OdooClient, RecordAmbiguousError
from provisioning.config import QUALITY_CONFIG, BATCH_SIZE
from provisioning.utils import log_header, log_info, log_success, log_warn, log_error


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class QualityError(Exception):
    """Base exception for quality operations."""
    pass


class QualityValidationError(QualityError):
    """Quality data validation error."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATOR
# ═══════════════════════════════════════════════════════════════════════════════

class QualityValidator:
    """Validate quality point data."""
    
    @staticmethod
    def validate_thresholds(
        pass_threshold: Optional[str],
        fail_threshold: Optional[str],
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Validate pass/fail thresholds.
        
        Args:
            pass_threshold: Pass threshold string
            fail_threshold: Fail threshold string
        
        Returns:
            (pass_threshold, fail_threshold)
        
        Raises:
            QualityValidationError: Wenn ungültig
        """
        pass_val = None
        fail_val = None
        
        # Parse pass threshold
        if pass_threshold and pass_threshold.strip():
            try:
                pass_val = float(pass_threshold.strip())
                if pass_val < 0:
                    raise QualityValidationError(f"Pass threshold cannot be negative: {pass_val}")
            except ValueError:
                raise QualityValidationError(f"Invalid pass threshold: {pass_threshold}")
        
        # Parse fail threshold
        if fail_threshold and fail_threshold.strip():
            try:
                fail_val = float(fail_threshold.strip())
                if fail_val < 0:
                    raise QualityValidationError(f"Fail threshold cannot be negative: {fail_val}")
            except ValueError:
                raise QualityValidationError(f"Invalid fail threshold: {fail_threshold}")
        
        # Validate relationship
        if pass_val is not None and fail_val is not None:
            if pass_val >= fail_val:
                raise QualityValidationError(
                    f"Pass threshold ({pass_val}) must be < fail threshold ({fail_val})"
                )
        
        return pass_val, fail_val
    
    @staticmethod
    def validate_title(title: Optional[str]) -> str:
        """Validate and clean QP title."""
        if not title or not title.strip():
            raise QualityValidationError("Quality point title is required")
        
        title = title.strip()
        
        if len(title) > 255:
            raise QualityValidationError(f"Title too long (max 255 chars): {title[:50]}...")
        
        return title


# ═══════════════════════════════════════════════════════════════════════════════
# QUALITY POINT LOADER
# ═══════════════════════════════════════════════════════════════════════════════

class QualityLoader:
    """Load Quality Control Points from CSV."""
    
    def __init__(self, client: OdooClient, base_data_dir: str):
        self.client = client
        self.quality_dir = Path(base_data_dir) / QUALITY_CONFIG.get('quality_dir', 'quality')
        
        # Cache: code → id
        self.product_cache: Dict[str, int] = {}
        self.workcenter_cache: Dict[str, int] = {}
        self.qc_type_cache: Dict[str, int] = {}
        
        # Statistics
        self.stats = {
            'qp_created': 0,
            'qp_updated': 0,
            'qp_skipped': 0,
            'errors_missing_operation': 0,
            'errors_missing_product': 0,
            'errors_invalid_data': 0,
            'errors': 0,
        }
        
        # Audit log
        self.audit_log: List[Dict[str, Any]] = []
        
        logger.info(f"QualityLoader initialized: {self.quality_dir}")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # LOOKUPS
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _find_product(self, default_code: str) -> Optional[int]:
        """Find product template by code."""
        if not default_code:
            return None
        
        default_code = default_code.strip()
        
        # Check cache
        if default_code in self.product_cache:
            return self.product_cache[default_code]
        
        # Search in Odoo
        products = self.client.search_read(
            'product.template',
            [('default_code', '=', default_code)],
            ['id'],
            limit=1
        )
        
        if products:
            product_id = products[0]['id']
            self.product_cache[default_code] = product_id
            return product_id
        
        return None
    
    def _find_workcenter(self, workcenter_name: str) -> Optional[int]:
        """Find workcenter/operation."""
        if not workcenter_name:
            return None
        
        workcenter_name = workcenter_name.strip()
        
        # Check cache
        if workcenter_name in self.workcenter_cache:
            return self.workcenter_cache[workcenter_name]
        
        # Search in Odoo (mrp.workcenter)
        workcenters = self.client.search_read(
            'mrp.workcenter',
            [('name', '=', workcenter_name)],
            ['id'],
            limit=1
        )
        
        if workcenters:
            wc_id = workcenters[0]['id']
            self.workcenter_cache[workcenter_name] = wc_id
            return wc_id
        
        return None
    
    def _get_or_create_qc_type(self, test_type: str) -> int:
        """Get or create quality check type."""
        if not test_type:
            test_type = 'Manual'
        
        test_type = test_type.strip()
        
        # Check cache
        if test_type in self.qc_type_cache:
            return self.qc_type_cache[test_type]
        
        # Search in Odoo
        qc_types = self.client.search_read(
            'quality.check.type',
            [('name', '=', test_type)],
            ['id'],
            limit=1
        )
        
        if qc_types:
            qc_type_id = qc_types[0]['id']
            self.qc_type_cache[test_type] = qc_type_id
            return qc_type_id
        
        # Create new
        vals = {
            'name': test_type,
            'technical_name': test_type.lower().replace(' ', '_'),
        }
        
        qc_type_id = self.client.create('quality.check.type', vals)
        self.qc_type_cache[test_type] = qc_type_id
        
        logger.info(f"Created QC Type: {test_type} → {qc_type_id}")
        
        return qc_type_id
    
    # ═══════════════════════════════════════════════════════════════════════════
    # CSV READING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _read_quality_csv(self, filepath: Path) -> List[Dict[str, str]]:
        """Read quality CSV with error handling."""
        if not filepath.exists():
            logger.warning(f"Quality CSV not found: {filepath}")
            return []
        
        rows = []
        delimiter = QUALITY_CONFIG.get('csv_delimiter', ';')
        
        for encoding in ['utf-8-sig', 'utf-8', 'latin-1']:
            try:
                with open(filepath, 'r', encoding=encoding, newline='') as f:
                    reader = csv.DictReader(f, delimiter=delimiter)
                    
                    if not reader.fieldnames:
                        logger.warning(f"CSV has no header: {filepath}")
                        return []
                    
                    rows = list(reader)
                
                logger.info(f"Read {len(rows)} quality rows from {filepath.name}")
                return rows
            
            except UnicodeDecodeError:
                continue
            except csv.Error as e:
                logger.error(f"CSV parse error in {filepath.name}: {e}")
                raise QualityError(f"Failed to read {filepath.name}: {e}")
        
        logger.warning(f"Failed to read {filepath} with multiple encodings")
        return []
    
    # ═══════════════════════════════════════════════════════════════════════════
    # LOADING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _load_quality_points(self, rows: List[Dict[str, str]], csv_name: str) -> None:
        """Load quality points from CSV rows."""
        if not rows:
            log_warn(f"No quality point rows in {csv_name}")
            return
        
        log_header(f"Loading Quality Points: {csv_name}")
        
        for row_idx, row in enumerate(rows, start=2):
            try:
                self._process_qp_row(row, row_idx, csv_name)
            except Exception as e:
                logger.error(f"Row {row_idx}: {e}", exc_info=True)
                self.stats['errors'] += 1
        
        log_success(
            f"{csv_name}: "
            f"created={self.stats['qp_created']}, "
            f"updated={self.stats['qp_updated']}, "
            f"skipped={self.stats['qp_skipped']}"
        )
    
    def _process_qp_row(
        self,
        row: Dict[str, str],
        row_idx: int,
        csv_name: str,
    ) -> None:
        """Process single quality point row."""
        # Get title
        qp_name = (
            row.get('qp_id', '').strip() or
            row.get('name', '').strip() or
            row.get('title', '').strip()
        )
        
        if not qp_name:
            logger.debug(f"Row {row_idx}: missing QP name")
            self.stats['qp_skipped'] += 1
            return
        
        # Validate title
        try:
            qp_title = QualityValidator.validate_title(qp_name)
        except QualityValidationError as e:
            logger.warning(f"Row {row_idx}: {e}")
            self.stats['errors_invalid_data'] += 1
            self.stats['qp_skipped'] += 1
            return
        
        # Find workcenter (REQUIRED)
        workcenter_name = row.get('operation_id', '').strip() or row.get('workcenter_name', '').strip()
        workcenter_id = self._find_workcenter(workcenter_name)
        
        if not workcenter_id:
            logger.warning(f"Row {row_idx}: Workcenter not found: '{workcenter_name}'")
            self.stats['errors_missing_operation'] += 1
            self.stats['qp_skipped'] += 1
            return
        
        # Find product (optional but recommended)
        product_code = row.get('product_default_code', '').strip() or row.get('default_code', '').strip()
        product_id = self._find_product(product_code) if product_code else None
        
        if product_code and not product_id:
            logger.warning(f"Row {row_idx}: Product not found: '{product_code}'")
            self.stats['errors_missing_product'] += 1
        
        # Get QC type
        test_type = row.get('test_type', '').strip() or 'Manual'
        qc_type_id = self._get_or_create_qc_type(test_type)
        
        # Validate thresholds
        try:
            pass_threshold, fail_threshold = QualityValidator.validate_thresholds(
                row.get('pass_threshold'),
                row.get('fail_threshold'),
            )
        except QualityValidationError as e:
            logger.warning(f"Row {row_idx}: {e}")
            pass_threshold = None
            fail_threshold = None
        
        # Build vals
        vals = {
            'title': qp_title[:64],
            'workcenter_id': workcenter_id,
            'check_type_id': qc_type_id,
        }
        
        # Add optional fields
        if product_id:
            vals['product_tmpl_id'] = product_id
        
        if pass_threshold is not None:
            vals['pass_threshold'] = pass_threshold
        
        if fail_threshold is not None:
            vals['fail_threshold'] = fail_threshold
        
        # Optional test criteria
        test_criteria = row.get('test_criteria', '').strip()
        if test_criteria:
            vals['test_criteria'] = test_criteria
        
        # Ensure in Odoo
        # Domain: product + workcenter (unique combination)
        if product_id:
            domain = [
                ('product_tmpl_id', '=', product_id),
                ('workcenter_id', '=', workcenter_id),
            ]
        else:
            # Fallback: just workcenter (less ideal)
            domain = [
                ('workcenter_id', '=', workcenter_id),
                ('title', '=', qp_title),
            ]
        
        try:
            qp_id, is_new = self.client.ensure_record(
                'quality.point',
                domain,
                vals,
                vals,
            )
            
            if is_new:
                self.stats['qp_created'] += 1
                log_success(f"[NEW] {qp_title} → {qp_id}")
            else:
                self.stats['qp_updated'] += 1
                log_info(f"[UPD] {qp_title} → {qp_id}")
            
            self._audit_log({
                'action': 'created' if is_new else 'updated',
                'qp_id': qp_id,
                'qp_title': qp_title,
                'workcenter_id': workcenter_id,
                'product_id': product_id,
                'csv_row': row_idx,
                'csv_file': csv_name,
            })
        
        except RecordAmbiguousError as e:
            logger.error(f"Row {row_idx}: Ambiguous record: {e}")
            self.stats['errors'] += 1
    
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
        
        audit_path = Path(self.quality_dir).parent / 'audit' / 'quality_audit.json'
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
            log_header("QUALITY LOADER")
            
            # Get CSV files to load
            quality_csv_files = QUALITY_CONFIG.get('csv_files', ['quality_points.csv'])
            
            if isinstance(quality_csv_files, str):
                quality_csv_files = [quality_csv_files]
            
            logger.info(f"Loading quality CSVs: {quality_csv_files}")
            
            # Load each CSV
            for csv_file in quality_csv_files:
                csv_path = self.quality_dir / csv_file
                
                try:
                    rows = self._read_quality_csv(csv_path)
                    if rows:
                        self._load_quality_points(rows, csv_file)
                
                except QualityError as e:
                    logger.error(f"Failed to load {csv_file}: {e}")
                    self.stats['errors'] += 1
            
            # Persist audit
            self._persist_audit_log()
            
            # Summary
            log_success("Quality loader completed")
            log_info("Statistics:")
            for key, value in self.stats.items():
                log_info(f"  {key}: {value}")
            
            return self.stats
        
        except Exception as e:
            log_error(f"Quality loader failed: {e}", exc_info=True)
            raise

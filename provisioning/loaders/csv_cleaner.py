"""
csv_cleaner.py - CSV Reading & Cleaning Utilities

Provides:
- Schema validation
- Encoding fallback
- Type coercion (Decimal, etc.)
- Row cleaning & normalization
- Robust error handling
"""

import csv
import logging
from pathlib import Path
from typing import Iterator, Dict, Any, List, Optional, Tuple, TextIO
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

# Default settings (should be in config.py, but can be overridden)
CSV_DEFAULT_DELIMITER = ';'
CSV_ENCODINGS = ['utf-8-sig', 'utf-8', 'latin-1']
QUANTITY_COLUMNS = {'product_qty', 'component_qty', 'min_qty', 'bom_line_product_qty'}


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class CsvError(Exception):
    """Base exception for CSV operations."""
    pass


class CsvSchemaError(CsvError):
    """CSV schema validation error."""
    pass


class CsvEncodingError(CsvError):
    """CSV encoding error."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# CSV READER
# ═══════════════════════════════════════════════════════════════════════════════

class CsvReader:
    """Robust CSV reader with validation and normalization."""
    
    @staticmethod
    def _try_open_file(
        path: Path,
        encodings: List[str] = None,
    ) -> Tuple[TextIO, str]:
        """
        Try opening file with multiple encodings.
        
        Args:
            path: File path
            encodings: List of encodings to try (default: utf-8-sig, utf-8, latin-1)
        
        Returns:
            (file_object, encoding_used)
        
        Raises:
            CsvEncodingError: If all encodings fail
        """
        if encodings is None:
            encodings = CSV_ENCODINGS
        
        for encoding in encodings:
            try:
                f = open(path, 'r', newline='', encoding=encoding)
                logger.debug(f"Successfully opened {path} with encoding {encoding}")
                return f, encoding
            except UnicodeDecodeError:
                continue
            except Exception as e:
                raise CsvError(f"Failed to open {path}: {e}")
        
        raise CsvEncodingError(
            f"Failed to open {path} with encodings: {encodings}"
        )
    
    @staticmethod
    def read_csv(
        path: str | Path,
        delimiter: str = CSV_DEFAULT_DELIMITER,
        required_cols: Optional[List[str]] = None,
        skip_empty_rows: bool = False,
        coerce_quantities: bool = True,
        quantity_columns: Optional[set] = None,
    ) -> Iterator[Dict[str, str]]:
        """
        Read and validate CSV file.
        
        Args:
            path: Path to CSV file
            delimiter: Field delimiter (default: ';')
            required_cols: List of required columns
            skip_empty_rows: Skip rows where all values are empty
            coerce_quantities: Coerce quantity columns to Decimal strings
            quantity_columns: Set of quantity column names
        
        Yields:
            Dict of cleaned row data
        
        Raises:
            FileNotFoundError: If file doesn't exist
            CsvSchemaError: If required columns missing
            CsvError: For other CSV errors
        """
        path = Path(path)
        
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")
        
        # Open file
        try:
            f, encoding = CsvReader._try_open_file(path)
        except CsvEncodingError as e:
            raise CsvError(f"Failed to read {path.name}: {e}")
        
        try:
            reader = csv.DictReader(f, delimiter=delimiter, skipinitialspace=True)
            
            # Validate schema
            if not reader.fieldnames:
                raise CsvSchemaError(f"CSV is empty or has no header")
            
            if required_cols:
                missing = set(required_cols) - set(reader.fieldnames)
                if missing:
                    raise CsvSchemaError(
                        f"CSV missing required columns: {missing}. "
                        f"Found: {list(reader.fieldnames)}"
                    )
            
            logger.info(
                f"Reading {path.name} with {len(reader.fieldnames)} columns"
            )
            
            # Use provided quantity columns or default
            if quantity_columns is None:
                quantity_columns = QUANTITY_COLUMNS
            
            # Process rows
            row_idx = 0
            for row in reader:
                row_idx += 1
                
                # Clean row (strip whitespace)
                cleaned = {
                    k.strip(): (v or '').strip()
                    for k, v in row.items()
                    if k  # Skip None keys
                }
                
                # Skip empty rows if requested
                if skip_empty_rows:
                    if not any(v for v in cleaned.values()):
                        logger.debug(f"Skipping empty row {row_idx}")
                        continue
                
                # Coerce quantities to Decimal strings
                if coerce_quantities:
                    try:
                        cleaned = CsvReader._coerce_quantities(
                            cleaned,
                            quantity_columns,
                        )
                    except ValueError as e:
                        logger.error(f"Row {row_idx}: {e}, skipping")
                        continue
                
                yield cleaned
        
        finally:
            f.close()
    
    @staticmethod
    def _coerce_quantities(
        row: Dict[str, str],
        quantity_columns: set,
    ) -> Dict[str, str]:
        """
        Coerce quantity columns to valid Decimal strings.
        
        Args:
            row: Row dict
            quantity_columns: Set of column names to coerce
        
        Returns:
            Row with coerced quantities
        
        Raises:
            ValueError: If quantity is invalid
        """
        for col in quantity_columns:
            if col in row and row[col]:
                try:
                    qty = Decimal(row[col])
                    
                    # Validate quantity is not negative
                    if qty < 0:
                        raise ValueError(
                            f"Invalid {col}: quantity cannot be negative ({qty})"
                        )
                    
                    # Convert back to string with 2 decimal places
                    row[col] = str(qty.quantize(Decimal('0.01')))
                
                except (InvalidOperation, ValueError) as e:
                    raise ValueError(f"Invalid {col}: {row[col]} → {e}")
        
        return row


# ═══════════════════════════════════════════════════════════════════════════════
# PATH UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def safe_join_path(
    base_dir: str | Path,
    *parts: str,
    must_exist: bool = False,
    is_file: bool = False,
) -> Path:
    """
    Safely join paths with validation.
    
    Args:
        base_dir: Base directory
        parts: Path parts to join
        must_exist: Raise error if path doesn't exist
        is_file: If True, check it's a file (not directory)
    
    Returns:
        Joined Path object
    
    Raises:
        FileNotFoundError: If must_exist=True and path doesn't exist
        ValueError: If is_file=True and path is directory
    """
    result = Path(base_dir).joinpath(*parts)
    
    if must_exist:
        if not result.exists():
            raise FileNotFoundError(f"Path not found: {result}")
        
        if is_file and result.is_dir():
            raise ValueError(f"Expected file, got directory: {result}")
    
    return result


def ensure_dir(path: str | Path) -> Path:
    """
    Ensure directory exists, create if needed.
    
    Args:
        path: Directory path
    
    Returns:
        Path object
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


# ═══════════════════════════════════════════════════════════════════════════════
# LEGACY COMPATIBILITY (csv_rows, join_path)
# ═══════════════════════════════════════════════════════════════════════════════

def csv_rows(
    path: str | Path,
    delimiter: str = CSV_DEFAULT_DELIMITER,
    required_cols: Optional[List[str]] = None,
    skip_empty: bool = False,
    coerce_quantities: bool = True,
) -> Iterator[Dict[str, str]]:
    """
    Alias for CsvReader.read_csv() for backward compatibility.
    
    Deprecated: Use CsvReader.read_csv() directly.
    """
    return CsvReader.read_csv(
        path,
        delimiter=delimiter,
        required_cols=required_cols,
        skip_empty_rows=skip_empty,
        coerce_quantities=coerce_quantities,
    )


def join_path(base_dir: str | Path, *parts: str) -> Path:
    """
    Alias for safe_join_path() for backward compatibility.
    
    Deprecated: Use safe_join_path() directly.
    """
    return safe_join_path(base_dir, *parts, must_exist=False)

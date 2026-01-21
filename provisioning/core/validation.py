"""
validation.py - Type Conversion & Validation Utilities

Provides:
- Safe type conversions (float, int, decimal, bool)
- Email/URL/Phone validation
- Range & length validation
- Proper error handling with logging
- Custom validation rules
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional, Union, Callable, Type
from decimal import Decimal, InvalidOperation
from pathlib import Path


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class ValidationError(Exception):
    """Base validation exception."""
    pass


class ConversionError(ValidationError):
    """Type conversion error."""
    pass


class RangeError(ValidationError):
    """Value out of allowed range."""
    pass


class FormatError(ValidationError):
    """Invalid format (email, phone, etc.)."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# SAFE CONVERSIONS (WITH DEFAULTS)
# ═══════════════════════════════════════════════════════════════════════════════

def safe_float(
    value: Any,
    default: float = 0.0,
    allow_negative: bool = True,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    field_name: str = "field",
) -> float:
    """
    Safely convert value to float.
    
    Args:
        value: Value to convert
        default: Default if conversion fails
        allow_negative: Allow negative values
        min_value: Minimum allowed value
        max_value: Maximum allowed value
        field_name: Field name for logging context
    
    Returns:
        Converted float or default
    """
    try:
        f = float(value)
    except (TypeError, ValueError) as e:
        logger.warning(
            f"Failed to convert {field_name} to float: {value!r} → {default}",
            exc_info=False,
        )
        return default
    
    # Validate range
    if not allow_negative and f < 0:
        logger.warning(f"Negative value not allowed for {field_name}: {f} → 0.0")
        return 0.0
    
    if min_value is not None and f < min_value:
        logger.warning(
            f"Value {f} below minimum {min_value} for {field_name} → {min_value}"
        )
        return min_value
    
    if max_value is not None and f > max_value:
        logger.warning(
            f"Value {f} above maximum {max_value} for {field_name} → {max_value}"
        )
        return max_value
    
    return f


def safe_int(
    value: Any,
    default: int = 0,
    allow_negative: bool = True,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
    field_name: str = "field",
) -> int:
    """Safely convert value to int."""
    try:
        i = int(value)
    except (TypeError, ValueError):
        logger.warning(
            f"Failed to convert {field_name} to int: {value!r} → {default}"
        )
        return default
    
    if not allow_negative and i < 0:
        logger.warning(f"Negative value not allowed for {field_name}: {i} → 0")
        return 0
    
    if min_value is not None and i < min_value:
        logger.warning(
            f"Value {i} below minimum {min_value} for {field_name} → {min_value}"
        )
        return min_value
    
    if max_value is not None and i > max_value:
        logger.warning(
            f"Value {i} above maximum {max_value} for {field_name} → {max_value}"
        )
        return max_value
    
    return i


def safe_decimal(
    value: Any,
    default: Decimal = Decimal('0'),
    allow_negative: bool = True,
    min_value: Optional[Decimal] = None,
    max_value: Optional[Decimal] = None,
    field_name: str = "field",
) -> Decimal:
    """Safely convert value to Decimal (financial values)."""
    try:
        d = Decimal(str(value))
    except (TypeError, ValueError, InvalidOperation):
        logger.warning(
            f"Failed to convert {field_name} to Decimal: {value!r} → {default}"
        )
        return default
    
    if not allow_negative and d < 0:
        logger.warning(f"Negative value not allowed for {field_name}: {d} → 0")
        return Decimal('0')
    
    if min_value is not None and d < min_value:
        logger.warning(
            f"Value {d} below minimum {min_value} for {field_name} → {min_value}"
        )
        return min_value
    
    if max_value is not None and d > max_value:
        logger.warning(
            f"Value {d} above maximum {max_value} for {field_name} → {max_value}"
        )
        return max_value
    
    return d


def safe_bool(
    value: Any,
    default: bool = False,
    field_name: str = "field",
) -> bool:
    """Safely convert value to bool."""
    if isinstance(value, bool):
        return value
    
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes', 'on')
    
    if isinstance(value, (int, float)):
        return bool(value)
    
    logger.warning(f"Failed to convert {field_name} to bool: {value!r} → {default}")
    return default


def safe_str(
    value: Any,
    default: str = "",
    strip: bool = True,
    field_name: str = "field",
) -> str:
    """Safely convert value to string."""
    try:
        s = str(value)
        if strip:
            s = s.strip()
        return s
    except Exception:
        logger.warning(f"Failed to convert {field_name} to str: {value!r} → {default}")
        return default


# ═══════════════════════════════════════════════════════════════════════════════
# STRICT VALIDATIONS (WITH EXCEPTIONS)
# ═══════════════════════════════════════════════════════════════════════════════

def validate_float(
    value: Any,
    allow_negative: bool = True,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    field_name: str = "field",
) -> float:
    """
    Validate and convert to float (raises on error).
    
    Raises:
        ConversionError: If conversion fails
        RangeError: If value out of range
    """
    try:
        f = float(value)
    except (TypeError, ValueError) as e:
        raise ConversionError(
            f"Cannot convert {field_name} to float: {value!r}"
        ) from e
    
    if not allow_negative and f < 0:
        raise RangeError(f"{field_name} cannot be negative: {f}")
    
    if min_value is not None and f < min_value:
        raise RangeError(
            f"{field_name} must be >= {min_value}, got {f}"
        )
    
    if max_value is not None and f > max_value:
        raise RangeError(
            f"{field_name} must be <= {max_value}, got {f}"
        )
    
    return f


def validate_int(
    value: Any,
    allow_negative: bool = True,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
    field_name: str = "field",
) -> int:
    """Validate and convert to int (raises on error)."""
    try:
        i = int(value)
    except (TypeError, ValueError) as e:
        raise ConversionError(
            f"Cannot convert {field_name} to int: {value!r}"
        ) from e
    
    if not allow_negative and i < 0:
        raise RangeError(f"{field_name} cannot be negative: {i}")
    
    if min_value is not None and i < min_value:
        raise RangeError(f"{field_name} must be >= {min_value}, got {i}")
    
    if max_value is not None and i > max_value:
        raise RangeError(f"{field_name} must be <= {max_value}, got {i}")
    
    return i


def validate_decimal(
    value: Any,
    allow_negative: bool = True,
    min_value: Optional[Decimal] = None,
    max_value: Optional[Decimal] = None,
    field_name: str = "field",
) -> Decimal:
    """Validate and convert to Decimal (raises on error)."""
    try:
        d = Decimal(str(value))
    except (TypeError, ValueError, InvalidOperation) as e:
        raise ConversionError(
            f"Cannot convert {field_name} to Decimal: {value!r}"
        ) from e
    
    if not allow_negative and d < 0:
        raise RangeError(f"{field_name} cannot be negative: {d}")
    
    if min_value is not None and d < min_value:
        raise RangeError(f"{field_name} must be >= {min_value}, got {d}")
    
    if max_value is not None and d > max_value:
        raise RangeError(f"{field_name} must be <= {max_value}, got {d}")
    
    return d


# ═══════════════════════════════════════════════════════════════════════════════
# FORMAT VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

class FormatValidator:
    """Validate common formats (email, phone, URL, etc.)."""
    
    # Regex patterns
    EMAIL_PATTERN = re.compile(
        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    )
    
    PHONE_PATTERN = re.compile(
        r'^\+?1?\d{9,15}$'  # Basic international phone
    )
    
    URL_PATTERN = re.compile(
        r'^https?://[^\s/$.?#].[^\s]*$'
    )
    
    DOMAIN_PATTERN = re.compile(
        r'^([a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$'
    )
    
    @staticmethod
    def validate_email(email: str, field_name: str = "email") -> str:
        """Validate email format."""
        if not isinstance(email, str):
            raise FormatError(f"{field_name} must be string, got {type(email)}")
        
        email = email.strip()
        
        if not FormatValidator.EMAIL_PATTERN.match(email):
            raise FormatError(f"Invalid email format: {email}")
        
        if len(email) > 254:
            raise FormatError(f"Email too long: {len(email)} > 254")
        
        return email
    
    @staticmethod
    def validate_phone(phone: str, field_name: str = "phone") -> str:
        """Validate phone number format."""
        if not isinstance(phone, str):
            raise FormatError(f"{field_name} must be string, got {type(phone)}")
        
        # Remove common separators
        phone_clean = re.sub(r'[\s\-\.\(\)]', '', phone)
        
        if not FormatValidator.PHONE_PATTERN.match(phone_clean):
            raise FormatError(f"Invalid phone format: {phone}")
        
        return phone
    
    @staticmethod
    def validate_url(url: str, field_name: str = "url") -> str:
        """Validate URL format."""
        if not isinstance(url, str):
            raise FormatError(f"{field_name} must be string, got {type(url)}")
        
        url = url.strip()
        
        if not FormatValidator.URL_PATTERN.match(url):
            raise FormatError(f"Invalid URL format: {url}")
        
        return url
    
    @staticmethod
    def validate_domain(domain: str, field_name: str = "domain") -> str:
        """Validate domain name format."""
        if not isinstance(domain, str):
            raise FormatError(f"{field_name} must be string, got {type(domain)}")
        
        domain = domain.strip().lower()
        
        if not FormatValidator.DOMAIN_PATTERN.match(domain):
            raise FormatError(f"Invalid domain format: {domain}")
        
        return domain


# ═══════════════════════════════════════════════════════════════════════════════
# RANGE VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def validate_range(
    value: Union[int, float],
    min_value: Union[int, float],
    max_value: Union[int, float],
    field_name: str = "field",
) -> Union[int, float]:
    """Validate value is within range."""
    if value < min_value or value > max_value:
        raise RangeError(
            f"{field_name} must be between {min_value} and {max_value}, got {value}"
        )
    return value


def validate_length(
    value: str,
    min_length: int = 0,
    max_length: Optional[int] = None,
    field_name: str = "field",
) -> str:
    """Validate string length."""
    if len(value) < min_length:
        raise ValidationError(
            f"{field_name} too short: {len(value)} < {min_length}"
        )
    
    if max_length and len(value) > max_length:
        raise ValidationError(
            f"{field_name} too long: {len(value)} > {max_length}"
        )
    
    return value


# ═══════════════════════════════════════════════════════════════════════════════
# CUSTOM VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def validate_with(
    value: Any,
    validator: Callable[[Any], bool],
    error_message: str = "Validation failed",
    field_name: str = "field",
) -> Any:
    """
    Validate using custom function.
    
    Args:
        value: Value to validate
        validator: Function that returns True if valid
        error_message: Error message if validation fails
        field_name: Field name for logging
    
    Returns:
        Value if valid
    
    Raises:
        ValidationError: If validation fails
    """
    if not validator(value):
        raise ValidationError(f"{field_name}: {error_message}")
    
    return value


# ═══════════════════════════════════════════════════════════════════════════════
# BATCH VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def validate_dict(
    data: dict,
    schema: dict,
    strict: bool = False,
) -> dict:
    """
    Validate dictionary against schema.
    
    Args:
        data: Dictionary to validate
        schema: {field_name: validator_func}
        strict: Raise if extra fields present
    
    Returns:
        Validated data
    
    Raises:
        ValidationError: If validation fails
    """
    validated = {}
    
    for field_name, validator_func in schema.items():
        if field_name not in data:
            raise ValidationError(f"Missing required field: {field_name}")
        
        try:
            validated[field_name] = validator_func(data[field_name])
        except ValidationError as e:
            raise ValidationError(f"Field {field_name}: {e}") from e
    
    if strict:
        extra_fields = set(data.keys()) - set(schema.keys())
        if extra_fields:
            raise ValidationError(f"Unexpected fields: {extra_fields}")
    
    return validated


# ═══════════════════════════════════════════════════════════════════════════════
# USAGE EXAMPLES
# ═══════════════════════════════════════════════════════════════════════════════

"""
SAFE conversions (return defaults on error):
    price = safe_float(row['price'], default=0.0, allow_negative=False)
    qty = safe_int(row['qty'], default=1, min_value=1)
    email = safe_str(row['email']).lower()

STRICT validations (raise on error):
    price = validate_decimal(row['price'], allow_negative=False)
    FormatValidator.validate_email(row['email'])
    validate_phone(row['phone'])

FORMAT validation:
    FormatValidator.validate_url(config['webhook_url'])
    FormatValidator.validate_domain(config['smtp_host'])

CUSTOM validation:
    validate_with(value, lambda x: x > 0, "Must be positive", "price")

BATCH validation:
    schema = {
        'email': lambda x: FormatValidator.validate_email(x),
        'price': lambda x: validate_decimal(x, allow_negative=False),
        'phone': lambda x: FormatValidator.validate_phone(x),
    }
    validated = validate_dict(row, schema)
"""

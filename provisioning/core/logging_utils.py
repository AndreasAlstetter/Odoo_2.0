"""
utils.py - Logging & Utility Functions

Provides:
- Production-ready logging with file output, rotation, structured logging
- Progress tracking
- Timing utilities
- Context managers for operations
"""

from __future__ import annotations

import logging
import logging.handlers
import sys
import json
import time
from pathlib import Path
from typing import Any, Optional, Dict, Generator
from datetime import datetime
from contextlib import contextmanager


# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════════════════

class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)


class ColoredFormatter(logging.Formatter):
    """Colored formatter for console output."""
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record: logging.LogRecord) -> str:
        """Format with colors."""
        levelname = record.levelname
        color = self.COLORS.get(levelname, '')
        
        # Format: [TIMESTAMP] [LEVEL] [MODULE] Message
        timestamp = datetime.fromtimestamp(record.created).strftime('%H:%M:%S')
        colored_level = f"{color}{levelname:8s}{self.RESET}"
        message = record.getMessage()
        
        result = f"[{timestamp}] {colored_level} {record.module:20s} {message}"
        
        # Add exception if present
        if record.exc_info:
            result += f"\n{self.formatException(record.exc_info)}"
        
        return result


def setup_logging(
    log_dir: Path = Path('./logs'),
    level: str = 'INFO',
    json_format: bool = False,
) -> logging.Logger:
    """
    Setup production-ready logging.
    
    Args:
        log_dir: Directory for log files
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_format: Use JSON format for files
    
    Returns:
        Configured logger
    """
    # Create log directory
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Root logger
    logger = logging.getLogger('provisioning')
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    # Console handler (colored)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(ColoredFormatter())
    logger.addHandler(console_handler)
    
    # File handler (rotating)
    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / 'provisioning.log',
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
    )
    file_handler.setLevel(logging.DEBUG)
    
    if json_format:
        file_handler.setFormatter(JsonFormatter())
    else:
        file_handler.setFormatter(logging.Formatter(
            '[%(asctime)s] %(levelname)-8s %(name)s:%(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        ))
    
    logger.addHandler(file_handler)
    
    # Error file handler
    error_handler = logging.FileHandler(log_dir / 'errors.log')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter(
        '[%(asctime)s] %(levelname)-8s %(name)s:%(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    ))
    logger.addHandler(error_handler)
    
    return logger


# Get module logger
logger = logging.getLogger('provisioning')


# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def debug(message: str, *args: Any, **kwargs: Any) -> None:
    """Log debug message."""
    logger.debug(message, *args, **kwargs)


def info(message: str, *args: Any, **kwargs: Any) -> None:
    """Log info message."""
    logger.info(message, *args, **kwargs)


def log_info(message: str, *args: Any, **kwargs: Any) -> None:
    """Alias for info (backward compatibility)."""
    info(message, *args, **kwargs)


def warning(message: str, *args: Any, **kwargs: Any) -> None:
    """Log warning message."""
    logger.warning(message, *args, **kwargs)


def log_warn(message: str, *args: Any, **kwargs: Any) -> None:
    """Alias for warning (backward compatibility)."""
    warning(message, *args, **kwargs)


def error(message: str, *args: Any, exc_info: bool = False, **kwargs: Any) -> None:
    """
    Log error message.
    
    Args:
        message: Error message
        args: Message format args
        exc_info: Include exception traceback
    """
    logger.error(message, *args, exc_info=exc_info, **kwargs)


def log_error(message: str, *args: Any, exc_info: bool = False, **kwargs: Any) -> None:
    """Alias for error (backward compatibility)."""
    error(message, *args, exc_info=exc_info, **kwargs)


def critical(message: str, *args: Any, exc_info: bool = False, **kwargs: Any) -> None:
    """Log critical message."""
    logger.critical(message, *args, exc_info=exc_info, **kwargs)


def success(message: str, *args: Any, **kwargs: Any) -> None:
    """Log success message (as INFO with prefix)."""
    logger.info(f"✓ {message}", *args, **kwargs)


def log_success(message: str, *args: Any, **kwargs: Any) -> None:
    """Alias for success (backward compatibility)."""
    success(message, *args, **kwargs)


def log_header(message: str, *args: Any, **kwargs: Any) -> None:
    """Log section header."""
    header = f"\n{'=' * 80}\n{message}\n{'=' * 80}"
    logger.info(header, *args, **kwargs)


# ═══════════════════════════════════════════════════════════════════════════════
# TIMING UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

class Timer:
    """Simple timer for measuring operation duration."""
    
    def __init__(self, name: str = "Operation"):
        self.name = name
        self.start_time: Optional[float] = None
        self.elapsed: Optional[float] = None
    
    def start(self) -> None:
        """Start timer."""
        self.start_time = time.time()
        logger.debug(f"Timer started: {self.name}")
    
    def stop(self) -> float:
        """Stop timer and return elapsed time."""
        if self.start_time is None:
            raise RuntimeError("Timer not started")
        
        self.elapsed = time.time() - self.start_time
        logger.debug(f"Timer stopped: {self.name} ({self.elapsed:.2f}s)")
        
        return self.elapsed
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, *args):
        self.stop()


@contextmanager
def timed_operation(name: str) -> Generator[Timer, None, None]:
    """Context manager for timed operations."""
    timer = Timer(name)
    try:
        with timer:
            yield timer
        
        logger.info(f"✓ {name} completed in {timer.elapsed:.2f}s")
    
    except Exception as e:
        logger.error(f"✗ {name} failed after {timer.elapsed:.2f}s: {e}", exc_info=True)
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# PROGRESS TRACKING
# ═══════════════════════════════════════════════════════════════════════════════

class ProgressTracker:
    """Simple progress tracker."""
    
    def __init__(self, total: int, prefix: str = "Progress"):
        self.total = total
        self.prefix = prefix
        self.current = 0
    
    def update(self, amount: int = 1) -> None:
        """Update progress."""
        self.current += amount
        percent = (self.current / self.total * 100) if self.total > 0 else 0
        logger.info(f"{self.prefix}: {self.current}/{self.total} ({percent:.1f}%)")
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        logger.info(f"✓ {self.prefix} completed: {self.current}/{self.total}")


def bump_progress(amount: float) -> None:
    """Bump progress (dummy for compatibility)."""
    # TODO: Implement with context-aware progress tracking
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# STRUCTURED LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

def log_operation(
    operation: str,
    status: str,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Log structured operation.
    
    Args:
        operation: Operation name
        status: 'started', 'completed', 'failed'
        details: Additional details
    """
    log_data = {
        'operation': operation,
        'status': status,
        'timestamp': datetime.now().isoformat(),
    }
    
    if details:
        log_data.update(details)
    
    message = json.dumps(log_data)
    
    if status == 'failed':
        logger.error(message)
    elif status == 'completed':
        logger.info(f"✓ {message}")
    else:
        logger.info(message)


# ═══════════════════════════════════════════════════════════════════════════════
# INITIALIZATION
# ═══════════════════════════════════════════════════════════════════════════════

# Setup logging on module import
setup_logging(Path('./logs'), level='INFO', json_format=False)

from __future__ import annotations
from typing import Any


def _log(level: str, message: str, *args: Any) -> None:
    if args:
        try:
            message = message.format(*args)
        except Exception:
            pass
    print(f"[{level}] {message}")


def info(message: str, *args: Any) -> None:
    _log("INFO", message, *args)


def warning(message: str, *args: Any) -> None:
    _log("WARN", message, *args)


def error(message: str, *args: Any) -> None:
    _log("ERROR", message, *args)


def success(message: str, *args: Any) -> None:
    _log("OK", message, *args)

# provisioning/core/validation.py

from __future__ import annotations

from typing import Any, Optional


def safe_float(
    value: Any,
    default: float = 0.0,
    allow_negative: bool = True,
) -> float:
    """
    Versucht, einen Wert robust in float zu konvertieren.

    - Bei Fehlern wird `default` zurückgegeben.
    - Wenn `allow_negative` False ist, werden negative Werte auf 0 begrenzt.
    """
    try:
        f = float(value)
    except (TypeError, ValueError):
        return default

    if not allow_negative and f < 0:
        return 0.0

    return f

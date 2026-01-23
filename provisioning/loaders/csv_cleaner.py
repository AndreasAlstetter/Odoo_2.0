import csv
import os
from typing import Dict, Iterator

from provisioning.utils import log_warn  # falls verfügbar


def csv_rows(path: str, delimiter: str = ",") -> Iterator[Dict[str, str]]:
    """
    Liest eine CSV-Datei und liefert bereinigte Dict-Zeilen:
    - Trimmt Header- und Feldwerte.
    - Ignoriert leere Header-Spalten.
    - Überspringt vollständig leere Zeilen.
    """
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            cleaned: Dict[str, str] = {}
            for k, v in row.items():
                if k is None:
                    continue
                key = k.strip()
                if isinstance(v, str):
                    cleaned[key] = v.strip()
                else:
                    cleaned[key] = v
            if not any(cleaned.values()):
                continue
            yield cleaned


def join_path(base_dir: str, *parts: str) -> str:
    return os.path.join(base_dir, *parts)

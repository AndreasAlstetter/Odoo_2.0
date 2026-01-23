# provisioning/flows/production_routing.py

"""
Python-basierte Routingdefinitionen für die Drohnenvarianten.

Ziele:
- Zentrale Definition der Fertigungsoperationen (Arbeitsplan) je Variante.
- Nutzung in Produktionssimulation, UMH-Masterdata-Export und Demos.

Jede Operation enthält:
- seq             : Reihenfolge im Routing
- name            : Bezeichnung der Operation
- workcenter_code : Referenz auf das Odoo-Workcenter
- setup_time_min  : Rüstzeit in Minuten
- run_time_min    : Laufzeit in Minuten pro Zyklus
- qty_per_cycle   : Stückzahl pro Zyklus (z. B. 4 Füße pro Druck)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Literal


VariantName = Literal["spartan", "balance", "lightweight"]


@dataclass
class OperationDef:
    seq: int
    name: str
    workcenter_code: str
    setup_time_min: float
    run_time_min: float
    qty_per_cycle: float = 1.0


ROUTINGS: Dict[VariantName, List[OperationDef]] = {
    "spartan": [
        OperationDef(10, "3D-Druck Füße", "WC-3D", 10, 60, 4),
        OperationDef(20, "3D-Druck Haube", "WC-3D", 10, 240, 1),
        OperationDef(30, "Laserschneiden Grundplatte", "WC-LC", 10, 10, 1),
        OperationDef(40, "Nacharbeit Grundplatte", "WC-NACH", 5, 15, 1),
        OperationDef(50, "Nacharbeit Füße", "WC-NACH", 5, 10, 4),
        OperationDef(60, "Nacharbeit Haube", "WC-NACH", 5, 10, 1),
        OperationDef(70, "WT bestücken", "WC-WTB", 5, 8, 1),
        OperationDef(80, "Löten Elektronik", "WC-LOET", 5, 15, 1),
        OperationDef(90, "Montage Elektronik", "WC-MONT1", 5, 10, 1),
        OperationDef(100, "Flashen Flugcontroller", "WC-FLASH", 5, 8, 1),
        OperationDef(110, "Montage Gehäuse & Rotoren", "WC-MONT2", 5, 12, 1),
        OperationDef(120, "End-Qualitätskontrolle", "WC-QM-END", 2, 5, 1),
    ],
    "balance": [
        # identisch, später ggf. Zeiten variantenabhängig anpassen
        OperationDef(10, "3D-Druck Füße", "WC-3D", 10, 60, 4),
        OperationDef(20, "3D-Druck Haube", "WC-3D", 10, 240, 1),
        OperationDef(30, "Laserschneiden Grundplatte", "WC-LC", 10, 10, 1),
        OperationDef(40, "Nacharbeit Grundplatte", "WC-NACH", 5, 15, 1),
        OperationDef(50, "Nacharbeit Füße", "WC-NACH", 5, 10, 4),
        OperationDef(60, "Nacharbeit Haube", "WC-NACH", 5, 10, 1),
        OperationDef(70, "WT bestücken", "WC-WTB", 5, 8, 1),
        OperationDef(80, "Löten Elektronik", "WC-LOET", 5, 15, 1),
        OperationDef(90, "Montage Elektronik", "WC-MONT1", 5, 10, 1),
        OperationDef(100, "Flashen Flugcontroller", "WC-FLASH", 5, 8, 1),
        OperationDef(110, "Montage Gehäuse & Rotoren", "WC-MONT2", 5, 12, 1),
        OperationDef(120, "End-Qualitätskontrolle", "WC-QM-END", 2, 5, 1),
    ],
    "lightweight": [
        OperationDef(10, "3D-Druck Füße", "WC-3D", 10, 60, 4),
        OperationDef(20, "3D-Druck Haube", "WC-3D", 10, 240, 1),
        OperationDef(30, "Laserschneiden Grundplatte", "WC-LC", 10, 10, 1),
        OperationDef(40, "Nacharbeit Grundplatte", "WC-NACH", 5, 15, 1),
        OperationDef(50, "Nacharbeit Füße", "WC-NACH", 5, 10, 4),
        OperationDef(60, "Nacharbeit Haube", "WC-NACH", 5, 10, 1),
        OperationDef(70, "WT bestücken", "WC-WTB", 5, 8, 1),
        OperationDef(80, "Löten Elektronik", "WC-LOET", 5, 15, 1),
        OperationDef(90, "Montage Elektronik", "WC-MONT1", 5, 10, 1),
        OperationDef(100, "Flashen Flugcontroller", "WC-FLASH", 5, 8, 1),
        OperationDef(110, "Montage Gehäuse & Rotoren", "WC-MONT2", 5, 12, 1),
        OperationDef(120, "End-Qualitätskontrolle", "WC-QM-END", 2, 5, 1),
    ],
}


def get_routing(variant: VariantName) -> List[OperationDef]:
    """
    Liefert die Routing-Operationen für eine gegebene Variante.

    Rückgabe:
    - Nach seq aufsteigend sortierte Liste von OperationDef.
    """
    ops = ROUTINGS.get(variant)
    if not ops:
        raise ValueError(
            f"Keine Routingdefinition für Variante '{variant}' gefunden."
        )
    # vorsichtshalber nach seq sortieren
    return sorted(ops, key=lambda o: o.seq)

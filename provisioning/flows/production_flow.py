# provisioning/flows/production_flow.py

"""
Produktionssimulation basierend auf dem Python-Routing.

Ziele:
- Für eine Drohnenvariante das definierte Routing durchlaufen.
- Rüst- und Laufzeiten simulieren und aufsummieren.
- UMH-Events für MO-Start, Qualität und MO-Abschluss erzeugen
  und in eine JSON-Datei schreiben.

Nutzt:
- provisioning.flows.production_routing.get_routing
- integration.UMHEventManager / UMHClientSimulator
"""

from __future__ import annotations

from datetime import datetime  # aktuell nur für mögliche Erweiterungen
from typing import Literal

from provisioning.client import OdooClient
from provisioning.utils import log_info, log_success, log_warn
from provisioning.config import UMH_EVENTS_PRODUCTION_FILE

from provisioning.flows.production_routing import get_routing, VariantName
from provisioning.integration.umh_events import UMHEventManager, EventType
from provisioning.integration.umh_client_sim import UMHClientSimulator


class ProductionFlow:
    """Simuliert Produktionsläufe für die verschiedenen Drohnenvarianten."""

    def __init__(self, api: OdooClient) -> None:
        self.api = api
        self.umh_manager = UMHEventManager()
        self.umh_client = UMHClientSimulator(output_file=UMH_EVENTS_PRODUCTION_FILE)

    def run_production_for_variant(
        self, variant: VariantName, quantity: float = 1.0
    ) -> None:
        """
        Simuliert den Produktionsablauf für eine Variante.

        - Durchläuft alle Routing-Operationen.
        - Summiert Rüst- und Laufzeiten.
        - Erzeugt UMH-Events (MO gestartet, Quality-Check, MO fertig).
        """
        log_info(
            f"Starte Produktionssimulation für Variante '{variant}' "
            f"(Menge {quantity})..."
        )

        ops = get_routing(variant)
        total_setup = 0.0
        total_run = 0.0

        # MO-Start-Event (ohne echte MO-ID -> Dummy -1)
        mo_start = self.umh_manager.create_mo_event(
            mo_id=-1, event_type=EventType.MO_STARTED
        )
        self.umh_manager.queue_event(mo_start)

        for op in ops:
            log_info(f"Operation {op.seq}: {op.name} auf {op.workcenter_code}")
            total_setup += op.setup_time_min
            total_run += op.run_time_min * quantity

            # Beispiel: Qualitäts-Event bei End-Qualitätskontrolle
            if op.seq == 120:
                q_evt = self.umh_manager.create_quality_event(
                    product_id=-1,  # Demo: kein echtes Produkt, Fokus auf Routing
                    stage="End-Qualitätskontrolle",
                    result="pass",
                    details=(
                        f"Variant={variant}, "
                        f"Workcenter={op.workcenter_code}, Seq={op.seq}"
                    ),
                )
                self.umh_manager.queue_event(q_evt)

        total = total_setup + total_run
        log_success(
            f"Variante '{variant}': Setup={total_setup:.1f} min, "
            f"Laufzeit={total_run:.1f} min, Gesamt={total:.1f} min."
        )

        # MO-Abschluss-Event
        mo_done = self.umh_manager.create_mo_event(
            mo_id=-1, event_type=EventType.MO_COMPLETED
        )
        self.umh_manager.queue_event(mo_done)

        # Events in Datei schreiben
        events_dicts = [e.to_dict() for e in self.umh_manager.get_pending_events()]
        self.umh_client.send_events_batch(events_dicts)
        if self.umh_client.export_to_file():
            log_success(
                f"UMH-Events für Variante '{variant}' nach "
                f"'{UMH_EVENTS_PRODUCTION_FILE}' exportiert."
            )
        else:
            log_warn(
                f"UMH-Events für Variante '{variant}' konnten nicht nach "
                f"'{UMH_EVENTS_PRODUCTION_FILE}' geschrieben werden."
            )
        self.umh_manager.clear_events()

    def run_demo_all_variants(self) -> None:
        """Führt die Produktionssimulation für alle drei Varianten einmal aus."""
        for v in ("spartan", "balance", "lightweight"):
            self.run_production_for_variant(v, quantity=1.0)
        log_success(
            "Demo-Produktionsläufe abgeschlossen für Varianten: "
            "spartan, balance, lightweight"
        )


def setup_production_flows(api: OdooClient) -> None:
    """
    Einstiegspunkt für den Runner: initialisiert den Produktionsflow
    und kann optional Demo-Simulationen ausführen.
    """
    _flow = ProductionFlow(api)
    log_info("Production-Flow initialisiert.")
    # Optional:
    _flow.run_demo_all_variants()

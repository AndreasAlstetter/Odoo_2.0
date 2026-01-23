# provisioning/integration/umh_events.py

"""
Definition und Verwaltung von UMH-Events für das Drohnenprojekt.

Abbildbare Ereignisse:
- Bestandsänderungen (Stock)
- Fertigungsauftrag-Start/-Ende (MO)
- Versandereignisse (Lieferung gebucht)
- Qualitätsereignisse (Prüfung bestanden/nicht bestanden)
"""

from __future__ import annotations

from typing import Dict, List
from datetime import datetime
from enum import Enum


class EventType(Enum):
    """Unterstützte Eventtypen für UMH."""

    STOCK_CHANGE = "stock_change"
    MO_STARTED = "mo_started"
    MO_COMPLETED = "mo_completed"
    DELIVERY_SHIPPED = "delivery_shipped"
    QUALITY_CHECK = "quality_check"


class UMHEvent:
    """Repräsentiert ein einzelnes Event, das an UMH gesendet werden kann."""

    def __init__(self, event_type: EventType, timestamp: datetime, payload: Dict) -> None:
        self.event_type = event_type
        self.timestamp = timestamp
        self.payload = payload

    def to_dict(self) -> Dict:
        """
        Wandelt das Event in ein Dictionary um, das direkt serialisierbar ist.

        Returns
        -------
        dict
            Struktur: {"type": str, "timestamp": ISO-String, "payload": dict}.
        """
        return {
            "type": self.event_type.value,
            "timestamp": self.timestamp.isoformat(),
            "payload": self.payload,
        }


class UMHEventManager:
    """Verwaltet die Erstellung, Queueing und Bereinigung von UMH-Events."""

    def __init__(self) -> None:
        self.events: List[UMHEvent] = []

    def create_stock_event(
        self,
        product_id: int,
        location_id: int,
        qty_change: float,
    ) -> UMHEvent:
        """
        Erzeugt ein Bestandsänderungs-Event.

        payload:
        - product_id
        - location_id
        - qty_change
        """
        return UMHEvent(
            event_type=EventType.STOCK_CHANGE,
            timestamp=datetime.utcnow(),
            payload={
                "product_id": product_id,
                "location_id": location_id,
                "qty_change": qty_change,
            },
        )

    def create_mo_event(self, mo_id: int, event_type: EventType) -> UMHEvent:
        """
        Erzeugt ein Fertigungsauftrags-Event (Start oder Abschluss).

        payload:
        - mo_id
        """
        return UMHEvent(
            event_type=event_type,
            timestamp=datetime.utcnow(),
            payload={
                "mo_id": mo_id,
            },
        )

    def create_shipping_event(self, delivery_id: int) -> UMHEvent:
        """
        Erzeugt ein Versand-Event (Lieferung gebucht).

        payload:
        - delivery_id
        """
        return UMHEvent(
            event_type=EventType.DELIVERY_SHIPPED,
            timestamp=datetime.utcnow(),
            payload={
                "delivery_id": delivery_id,
            },
        )

    def create_quality_event(
        self,
        product_id: int,
        stage: str,
        result: str,
        details: str | None = None,
    ) -> UMHEvent:
        """
        Erzeugt ein Qualitäts-Event.

        payload:
        - product_id
        - stage (z. B. 'Endtest')
        - result ('pass' oder 'fail')
        - details (optionale Beschreibung)
        """
        payload: Dict[str, object] = {
            "product_id": product_id,
            "stage": stage,
            "result": result,
        }

        if details:
            payload["details"] = details

        return UMHEvent(
            event_type=EventType.QUALITY_CHECK,
            timestamp=datetime.utcnow(),
            payload=payload,
        )

    def queue_event(self, event: UMHEvent) -> None:
        """Fügt ein Event zur Warteschlange hinzu."""
        self.events.append(event)

    def get_pending_events(self) -> List[UMHEvent]:
        """Gibt eine Kopie aller aktuell gequeueten Events zurück."""
        return list(self.events)

    def clear_events(self) -> None:
        """Leert die Warteschlange."""
        self.events.clear()

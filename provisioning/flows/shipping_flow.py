from __future__ import annotations

from typing import List, Dict, Any

from provisioning.client import OdooClient
from provisioning.utils import log_info, log_success, log_warn


class ShippingFlow:
    """Kapselt typische Schritte des Versandprozesses."""

    def __init__(self, api: OdooClient) -> None:
        self.api = api

    def _get_order_name(self, order_id: int) -> str | None:
        """Holt den 'name' eines sale.order (z. B. SO00010)."""
        data = self.api.read("sale.order", [order_id], ["name"])
        if not data:
            return None
        return data[0].get("name")

    def _find_outgoing_pickings(self, order_id: int) -> List[Dict[str, Any]]:
        """
        Sucht Warenausgangs-Pickings (Lieferungen) zu einem Verkaufsauftrag.
        """
        so_name = self._get_order_name(order_id)
        if not so_name:
            log_warn(f"Name für Verkaufsauftrag {order_id} nicht gefunden.")
            return []

        pickings = self.api.search_read(
            "stock.picking",
            [
                ["origin", "=", so_name],
                ["picking_type_id.code", "=", "outgoing"],
            ],
            ["id", "state", "picking_type_id"],
            limit=20,
        )
        return pickings

    def ship_order(self, order_id: int) -> None:
        """
        Bucht die Lieferungen (Warenausgang) zu einem Verkaufsauftrag.
        """
        log_info(f"Buche Lieferungen für Verkaufsauftrag {order_id}...")

        pickings = self._find_outgoing_pickings(order_id)
        if not pickings:
            log_warn(f"Keine Lieferungen für Verkaufsauftrag {order_id} gefunden.")
            return

        for picking in pickings:
            picking_id = picking["id"]
            try:
                self.api.call(
                    "stock.picking",
                    "button_validate",
                    [[picking_id]],
                )
                log_success(f"Lieferung {picking_id} für Auftrag {order_id} gebucht.")
            except Exception:
                log_warn(
                    f"Lieferung {picking_id} für Auftrag {order_id} "
                    f"konnte nicht gebucht werden."
                )

    def run_demo_shipping(self, order_ids: List[int]) -> List[int]:
        """
        Führt eine Versand-Demo für eine Liste von Verkaufsaufträgen aus.

        Rückgabe:
        - Liste der Auftrags-IDs, für die Lieferungen gebucht wurden (oder versucht wurden).
        """
        log_info("Starte Demo: Warenausgang / Versand...")

        processed: List[int] = []
        for oid in order_ids:
            self.ship_order(oid)
            processed.append(oid)

        if not processed:
            log_warn("Keine Aufträge für die Versand-Demo vorhanden.")
        else:
            log_success(f"{len(processed)} Aufträge in der Versand-Demo verarbeitet.")

        return processed


def setup_shipping_flows(api: OdooClient) -> None:
    """
    Einstiegspunkt für den Runner: initialisiert den Versandflow.
    """
    _flow = ShippingFlow(api)
    log_info("Shipping-Flow initialisiert.")
    # Optional:
    _flow.run_demo_shipping([])
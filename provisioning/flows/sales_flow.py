from __future__ import annotations

from typing import List

from provisioning.client import OdooClient
from provisioning.utils import log_info, log_success, log_warn


class SalesFlow:
    """Kapselt Demo-Szenarien für Angebot → Auftrag."""

    def __init__(self, api: OdooClient) -> None:
        self.api = api

    def _create_quotation(
        self,
        customer_name: str,
        product_name: str,
        quantity: float,
        discount: float = 0.0,
    ) -> int:
        """Erstellt ein Angebot (sale.order) mit einer Position."""
        partners = self.api.search_read(
            "res.partner",
            [["name", "=", customer_name], ["customer_rank", ">", 0]],
            ["id"],
            limit=1,
        )
        if not partners:
            raise RuntimeError(f"Kunde '{customer_name}' nicht gefunden.")
        partner_id = partners[0]["id"]

        products = self.api.search_read(
            "product.product",
            [["name", "=", product_name]],
            ["id", "list_price"],
            limit=1,
        )
        if not products:
            raise RuntimeError(f"Produkt '{product_name}' nicht gefunden.")
        product_id = products[0]["id"]

        order_vals = {
            "partner_id": partner_id,
            "order_line": [
                (
                    0,
                    0,
                    {
                        "product_id": product_id,
                        "product_uom_qty": quantity,
                        "discount": discount,
                    },
                )
            ],
        }
        order_id = self.api.create("sale.order", order_vals)
        if isinstance(order_id, (list, tuple)):
            order_id = order_id[0]
        return int(order_id)

    def _confirm_order(self, order_id: int) -> None:
        """Bestätigt ein Angebot zu einem Auftrag (action_confirm)."""
        self.api.call("sale.order", "action_confirm", [[order_id]])

    def scenario_standard_order(self) -> int:
        """Szenario 1: Standardauftrag ohne Rabatt."""
        log_info("Szenario 1: Standardauftrag ohne Rabatt.")
        order_id = self._create_quotation(
            customer_name="Demo Kunde GmbH",
            product_name="EVO2 Spartan Drohne",
            quantity=1.0,
        )
        self._confirm_order(order_id)
        log_success(f"Szenario 1 abgeschlossen: Auftrag {order_id}.")
        return order_id

    def scenario_discount_order(self) -> int:
        """Szenario 2: Auftrag mit Rabatt."""
        log_info("Szenario 2: Auftrag mit Rabatt.")
        order_id = self._create_quotation(
            customer_name="NextLap AG",
            product_name="EVO2 Lightweight Drohne",
            quantity=2.0,
            discount=10.0,
        )
        self._confirm_order(order_id)
        log_success(f"Szenario 2 abgeschlossen: Auftrag {order_id}.")
        return order_id

    def scenario_bulk_order(self) -> int:
        """Szenario 3: Sammelauftrag mit höherer Menge."""
        log_info("Szenario 3: Sammelauftrag mit höherer Menge.")
        order_id = self._create_quotation(
            customer_name="Demo Kunde GmbH",
            product_name="EVO2 Balance Drohne",
            quantity=5.0,
        )
        self._confirm_order(order_id)
        log_success(f"Szenario 3 abgeschlossen: Auftrag {order_id}.")
        return order_id

    def run_demo_quotes_to_orders(self) -> List[int]:
        """
        Führt alle drei Szenarien aus und gibt die Auftrags-IDs zurück.
        """
        orders: List[int] = []

        for scenario in [
            self.scenario_standard_order,
            self.scenario_discount_order,
            self.scenario_bulk_order,
        ]:
            try:
                orders.append(scenario())
            except RuntimeError as exc:
                log_warn(f"Sales-Demo-Szenario übersprungen: {exc}")

        if not orders:
            log_warn("Keine Sales-Demo-Aufträge erzeugt (Kunden/Produkte prüfen).")

        return orders


def setup_sales_flows(api: OdooClient) -> None:
    """
    Einstiegspunkt für den Runner: initialisiert die Sales-Demo-Flows.
    """
    _flow = SalesFlow(api)
    log_info("Sales-Flow initialisiert.")
    # Optional automatisch ausführen:
    _flow.run_demo_quotes_to_orders()

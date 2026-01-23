from __future__ import annotations

from typing import List, Dict, Any

from provisioning.client import OdooClient
from provisioning.utils import log_info, log_success, log_warn


class PurchaseFlow:
    """Kapselt typische Schritte des Einkaufsprozesses."""

    def __init__(self, api: OdooClient) -> None:
        self.api = api

    def _find_vendor(self, name: str) -> int | None:
        """Sucht einen Lieferanten anhand des Namens und liefert dessen ID oder None."""
        res = self.api.search_read(
            "res.partner",
            [["name", "=", name], ["supplier_rank", ">", 0]],
            ["id"],
            limit=1,
        )
        return res[0]["id"] if res else None

    def _find_product(self, name: str) -> int | None:
        """Sucht ein Produkt anhand des Namens und liefert dessen ID oder None."""
        res = self.api.search_read(
            "product.product",
            [["name", "=", name]],
            ["id"],
            limit=1,
        )
        return res[0]["id"] if res else None

    def create_rfq(
        self,
        vendor_name: str,
        product_name: str,
        quantity: float = 10.0,
        price_unit: float = 0.0,
    ) -> int:
        """
        Legt eine Lieferantenanfrage (purchase.order) mit genau einer Zeile an.

        Rückgabe:
        - ID der erzeugten purchase.order.
        """
        log_info(
            f"Erzeuge RFQ für Lieferant '{vendor_name}' "
            f"und Produkt '{product_name}'..."
        )

        vendor_id = self._find_vendor(vendor_name)
        if not vendor_id:
            raise RuntimeError(f"Lieferant '{vendor_name}' nicht gefunden.")

        product_id = self._find_product(product_name)
        if not product_id:
            raise RuntimeError(f"Produkt '{product_name}' nicht gefunden.")

        po_vals: Dict[str, Any] = {
            "partner_id": vendor_id,
            # Optional: weitere Felder wie currency_id, date_order etc.
        }

        po_id = self.api.create("purchase.order", po_vals)
        if isinstance(po_id, (list, tuple)):
            if not po_id:
                raise RuntimeError(
                    "Erstellung von purchase.order hat keine ID geliefert."
                )
            po_id = po_id[0]
        po_id = int(po_id)

        line_vals: Dict[str, Any] = {
            "order_id": po_id,
            "product_id": product_id,
            "product_qty": quantity,
            "price_unit": price_unit,
        }

        self.api.create("purchase.order.line", line_vals)
        log_success(f"RFQ {po_id} erstellt.")
        return po_id

    def confirm_rfq(self, po_id: int) -> None:
        """Bestätigt eine RFQ und konvertiert sie in eine Bestellung."""
        log_info(f"Bestätige RFQ/Bestellung {po_id}...")
        self.api.call("purchase.order", "button_confirm", [[po_id]])
        log_success(f"Bestellung {po_id} bestätigt.")

    def receive_goods(self, po_id: int) -> None:
        """
        Bucht den Wareneingang zu einer Bestellung.

        Vorgehen:
        - Bestellung anhand der ID lesen, Name (PO-Nummer) ermitteln.
        - Eingangs-Pickings finden (origin ~ PO-Name, incoming).
        - button_validate auf den Pickings ausführen.
        """
        log_info(f"Buche Wareneingang für Bestellung {po_id}...")

        po = self.api.search_read(
            "purchase.order",
            [["id", "=", po_id]],
            ["name"],
            limit=1,
        )
        if not po:
            log_warn(f"Bestellung {po_id} nicht gefunden.")
            return

        po_name = po[0]["name"]  # z. B. 'PO00002'

        pickings = self.api.search_read(
            "stock.picking",
            [
                ["origin", "ilike", po_name],
                ["picking_type_id.code", "=", "incoming"],
            ],
            ["id", "state"],
            limit=10,
        )

        if not pickings:
            log_warn(f"Kein Wareneingangspicking für Bestellung {po_id} gefunden.")
            return

        for picking in pickings:
            picking_id = picking["id"]
            self.api.call(
                "stock.picking",
                "button_validate",
                [[picking_id]],
            )

        log_success(f"Wareneingang für Bestellung {po_id} gebucht.")

    def run_demo_purchasing(self) -> List[int]:
        """
        Führt ein Demo-Einkaufsszenario durch.

        Annahmen:
        - Ein Lieferant existiert (z. B. 'Amazon' als Kurzname).
        - Ein Produkt existiert (z. B. 'Akku').

        Rückgabe:
        - Liste der Bestell-IDs.
        """
        log_info("Starte Demo: RFQ → Bestellung → Wareneingang...")

        orders: List[int] = []
        scenarios = [
            ("Amazon", "Akku", 10.0),
        ]

        for vendor, prod, qty in scenarios:
            try:
                po_id = self.create_rfq(vendor, prod, qty)
                self.confirm_rfq(po_id)
                self.receive_goods(po_id)
                orders.append(po_id)
            except RuntimeError as exc:
                log_warn(f"Demo-Einkaufsszenario übersprungen: {exc}")

        if not orders:
            log_warn(
                "Keine Demo-Bestellungen konnten erzeugt werden "
                "(Lieferanten/Produkte prüfen)."
            )
        else:
            log_success(
                f"{len(orders)} Demo-Bestellungen angelegt "
                f"und Wareneingang gebucht."
            )

        return orders


def setup_purchase_flows(api: OdooClient) -> None:
    """
    Einstiegspunkt für den Runner: initialisiert den Einkaufsflow
    und kann optional eine Demo durchlaufen.
    """
    _flow = PurchaseFlow(api)
    log_info("Purchase-Flow initialisiert.")
    # Optional:
    _flow.run_demo_purchasing()

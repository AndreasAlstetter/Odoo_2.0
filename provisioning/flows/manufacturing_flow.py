# provisioning/flows/manufacturing_flow.py

"""
Fertigungsprozess: MO aus Verkaufsauftrag, Materialentnahme, Fertigmeldung.

Ziele:
- Fertigungsaufträge (mrp.production) aus bestätigten Verkaufsaufträgen
  erzeugen (Make To Order, vereinfacht).
- Materialbereitstellung über Reservierung/Transfers (vereinfacht).
- Fertigmeldung mit Ist-Mengen (vereinfacht).

Nutzt:
- mrp.production, stock.move, stock.picking
"""

from __future__ import annotations

from typing import List, Dict, Any

from provisioning.client import OdooClient
from provisioning.utils import log_info, log_success, log_warn


class ManufacturingFlow:
    """Kapselt typische Schritte des Fertigungsprozesses."""

    def __init__(self, api: OdooClient) -> None:
        self.api = api

    def create_mos_from_sales_orders(self, order_ids: List[int]) -> List[int]:
        """
        Erzeugt Fertigungsaufträge (MOs) aus Verkaufsaufträgen.

        Vereinfachung:
        - Liest alle sale.order.line je Auftrag.
        - Erzeugt pro Zeile einen mrp.production mit Produkt und Menge.
        - Nutzt nicht den kompletten Odoo-MTO-Automatikfluss.
        """
        log_info("Erzeuge Fertigungsaufträge aus Verkaufsaufträgen...")

        mo_ids: List[int] = []

        for order_id in order_ids:
            lines = self.api.search_read(
                "sale.order.line",
                [["order_id", "=", order_id]],
                ["id", "product_id", "product_uom_qty"],
                limit=100,
            )

            if not lines:
                log_warn(f"Keine Auftragszeilen für Auftrag {order_id} gefunden.")
                continue

            for line in lines:
                product_id = line["product_id"][0]
                qty = line["product_uom_qty"]

                vals: Dict[str, Any] = {
                    "product_id": product_id,
                    "product_qty": qty,
                    # Optional: "origin": sale.order-Name etc.
                }

                mo_id = self.api.create("mrp.production", vals)
                if isinstance(mo_id, (list, tuple)):
                    if not mo_id:
                        continue
                    mo_id = mo_id[0]
                mo_id = int(mo_id)

                mo_ids.append(mo_id)

        if mo_ids:
            log_success(f"{len(mo_ids)} Fertigungsaufträge erzeugt.")
        else:
            log_warn("Keine Fertigungsaufträge erzeugt (prüfe Routen/Produkte).")

        return mo_ids

    def start_mo(self, mo_id: int) -> None:
        """Startet einen Fertigungsauftrag (vereinfacht)."""
        log_info(f"Starte Fertigungsauftrag {mo_id}...")
        try:
            # confirm -> assign (Material reservieren)
            self.api.call("mrp.production", "action_confirm", [[mo_id]])
            self.api.call("mrp.production", "action_assign", [[mo_id]])
        except Exception as exc:
            # Nicht alle Odoo-Versionen nutzen dieselben Methoden
            log_warn(
                f"Start für MO {mo_id} konnte nicht vollständig durchgeführt werden. "
                f"Details: {exc}"
            )

    def finish_mo(self, mo_id: int, qty_done: float | None = None) -> None:
        """
        Meldet einen Fertigungsauftrag als fertig.

        Logik:
        - Wenn qty_done nicht angegeben: geplante Menge (product_qty) verwenden.
        - Versucht zuerst button_mark_done, dann action_finish.
        """
        log_info(f"Melde Fertigungsauftrag {mo_id} als fertig...")

        mo_data = self.api.search_read(
            "mrp.production",
            [["id", "=", mo_id]],
            ["product_qty", "qty_producing"],
            limit=1,
        )
        planned = mo_data[0].get("product_qty", 0.0) if mo_data else 0.0

        if qty_done is None:
            qty_done = planned or 0.0

        # In modernen Odoo-Versionen über Workorders; hier vereinfachter Aufruf
        try:
            # Setze qty_producing, falls Feld vorhanden
            self.api.write("mrp.production", [mo_id], {"qty_producing": qty_done})
        except Exception:
            pass

        try:
            self.api.call("mrp.production", "button_mark_done", [[mo_id]])
        except Exception:
            try:
                self.api.call("mrp.production", "action_finish", [[mo_id]])
            except Exception as exc:
                log_warn(
                    f"Fertigmeldung für MO {mo_id} konnte nicht automatisch durchgeführt werden. "
                    f"Details: {exc}"
                )

        log_success(f"MO {mo_id} wurde (ggf. vereinfacht) fertiggemeldet.")

    def run_demo_mo_chain(self, order_ids: List[int]) -> List[int]:
        """
        Führt die Demo-Kette „Auftrag → MO → Start → Fertigmeldung“ aus.

        Rückgabe:
        - Liste der verarbeiteten MO-IDs.
        """
        log_info("Starte Demo: Auftragskette inkl. Fertigung...")

        mo_ids = self.create_mos_from_sales_orders(order_ids)

        for mo in mo_ids:
            self.start_mo(mo)
            self.finish_mo(mo)

        if mo_ids:
            log_success(
                f"{len(mo_ids)} Fertigungsaufträge durchlaufen die Demo-Kette."
            )
        else:
            log_warn("Keine MOs für die Demo-Kette verfügbar.")

        return mo_ids


def setup_mrp_flows(api: OdooClient) -> None:
    """
    Einstiegspunkt für den Runner: initialisiert den Fertigungsflow.
    Aktuell nur als Demo (ohne automatische MO-Erzeugung beim Provisioning).
    """
    _flow = ManufacturingFlow(api)
    log_info("Manufacturing-Flow initialisiert.")

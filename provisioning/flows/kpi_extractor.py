from __future__ import annotations

from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

from provisioning.client import OdooClient
from provisioning.utils import log_info, log_warn, log_kpi_summary


class KPIExtractor:
    """Extrahiert KPIs aus Fertigung, QC und Lager (inkl. Detaildaten)."""

    def __init__(self, api: OdooClient, base_data_dir: Optional[str] = None) -> None:
        self.api = api
        self.base_data_dir = base_data_dir

    # -------------------------------------------------------------------------
    # Hilfen für Zeiträume
    # -------------------------------------------------------------------------

    def _default_mo_timerange(self) -> Tuple[datetime, datetime]:
        """Standard: letzte 30 Tage statt nur 'heute'."""
        end = datetime.utcnow()
        start = end - timedelta(days=30)
        return start, end

    # -------------------------------------------------------------------------
    # Fertigung (MOs)
    # -------------------------------------------------------------------------

    def get_mo_performance(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict:
        """
        Liefert Fertigungs-KPIs im Zeitraum.

        summary:
        - mo_count
        - avg_throughput_days

        details:
        - Liste der MOs mit create_date / date_finished
        """
        if start_date is None or end_date is None:
            start_date, end_date = self._default_mo_timerange()

        log_info(f"Berechne MO-Performance von {start_date} bis {end_date}...")

        mos = self.api.search_read(
            "mrp.production",
            [
                ["state", "=", "done"],
                ["create_date", ">=", start_date.strftime("%Y-%m-%d %H:%M:%S")],
                ["create_date", "<=", end_date.strftime("%Y-%m-%d %H:%M:%S")],
            ],
            ["id", "product_id", "product_qty", "create_date", "date_finished"],
            limit=1000,
        )

        durations: List[float] = []
        for mo in mos:
            c = mo.get("create_date")
            f = mo.get("date_finished")
            if not c or not f:
                continue
            try:
                dt_c = datetime.fromisoformat(c)
                dt_f = datetime.fromisoformat(f)
            except Exception:
                continue
            dur_days = (dt_f - dt_c).total_seconds() / 86400.0
            durations.append(dur_days)
            log_info(
                f"[KPI:MO:DETAIL] MO {mo['id']} "
                f"prod={mo.get('product_id')} "
                f"qty={mo.get('product_qty')} "
                f"von {c} bis {f} -> {dur_days:.4f} Tage"
            )

        avg_duration = sum(durations) / len(durations) if durations else 0.0

        if not mos:
            log_warn(
                "[KPI:MO] Keine fertiggestellten MOs im Zeitraum – "
                "MO-Kennzahlen = 0, nicht aussagekräftig."
            )

        return {
            "summary": {
                "mo_count": len(mos),
                "avg_throughput_days": avg_duration,
            },
            "details": mos,
        }

    # -------------------------------------------------------------------------
    # Quality
    # -------------------------------------------------------------------------

    def get_qc_metrics(self, product_id: Optional[int] = None) -> Dict:
        """
        Liefert QC-Kennzahlen.

        Variante ohne Custom-Feld:
        - nutzt ein bestehendes Feld wie 'quality_state' ('passed' / 'failed').

        summary:
        - checks_total, checks_passed, checks_failed, pass_rate, fail_rate

        details:
        - Liste der quality.check-Datensätze
        """
        log_info("Berechne QC-Kennzahlen...")

        domain: List = []
        if product_id:
            domain.append(["product_id", "=", product_id])

        try:
            checks = self.api.search_read(
                "quality.check",
                domain,
                ["id", "product_id", "point_id", "quality_state"],
                limit=1000,
            )
        except Exception:
            log_warn(
                "[KPI:QC] quality.check oder Feld 'quality_state' nicht lesbar – "
                "QC-Kennzahlen = 0."
            )
            return {
                "summary": {
                    "checks_total": 0,
                    "checks_passed": 0,
                    "checks_failed": 0,
                    "pass_rate": 0.0,
                    "fail_rate": 0.0,
                },
                "details": [],
            }

        if not checks or "quality_state" not in checks[0]:
            log_warn(
                "[KPI:QC] Feld 'quality_state' auf quality.check nicht vorhanden – "
                "QC-Kennzahlen = 0."
            )
            return {
                "summary": {
                    "checks_total": 0,
                    "checks_passed": 0,
                    "checks_failed": 0,
                    "pass_rate": 0.0,
                    "fail_rate": 0.0,
                },
                "details": [],
            }

        total = len(checks)
        passed = sum(1 for c in checks if c.get("quality_state") == "passed")
        failed = sum(1 for c in checks if c.get("quality_state") == "failed")

        pass_rate = (passed / total) if total else 0.0
        fail_rate = (failed / total) if total else 0.0

        for c in checks:
            log_info(
                f"[KPI:QC:DETAIL] QC {c['id']} "
                f"product={c.get('product_id')} "
                f"point={c.get('point_id')} "
                f"state={c.get('quality_state')}"
            )

        if total == 0:
            log_warn(
                "[KPI:QC] Keine Quality-Checks gefunden – "
                "QC-Raten = 0, nicht aussagekräftig."
            )

        return {
            "summary": {
                "checks_total": total,
                "checks_passed": passed,
                "checks_failed": failed,
                "pass_rate": pass_rate,
                "fail_rate": fail_rate,
            },
            "details": checks,
        }

    # -------------------------------------------------------------------------
    # Lager
    # -------------------------------------------------------------------------

    def get_inventory_metrics(self) -> Dict:
        """
        Liefert Lagerkennzahlen.

        summary:
        - products_with_stock
        - total_stock_qty

        top_products:
        - Top-N Produkte nach Bestand
        """
        log_info("Berechne Lager-Kennzahlen...")

        products = self.api.search_read(
            "product.product",
            [],
            ["id", "product_tmpl_id", "qty_available"],
            limit=1000,
        )

        stock_values = [p.get("qty_available", 0.0) for p in products]
        positive = [q for q in stock_values if q > 0]
        total_stock = sum(stock_values)

        products_sorted = sorted(
            products, key=lambda p: p.get("qty_available", 0.0), reverse=True
        )

        for p in products_sorted[:10]:
            log_info(
                f"[KPI:INV:DETAIL] Prod {p['id']} tmpl={p.get('product_tmpl_id')} "
                f"qty={p.get('qty_available', 0.0)}"
            )

        if not positive:
            log_warn(
                "[KPI:INV] Keine Produkte mit positivem Bestand – "
                "Kennzahl eher Demo-/Testniveau."
            )

        return {
            "summary": {
                "products_with_stock": len(positive),
                "total_stock_qty": total_stock,
            },
            "top_products": products_sorted[:20],
        }

    # -------------------------------------------------------------------------
    # Lead Time (Sales -> Delivery)
    # -------------------------------------------------------------------------

    def _get_example_lead_time(self) -> float:
        """
        Sucht einen Sale-Order mit abgeschlossener Lieferung und berechnet
        die Lead Time (create_date -> letzte date_done).
        """
        log_info("Berechne Beispiel-Lead-Time für einen SO mit Lieferung...")

        orders = self.api.search_read(
            "sale.order",
            [["state", "in", ["sale", "done"]]],
            ["id", "name", "create_date"],
            limit=20,
        )

        for so in orders:
            so_name = so["name"]
            start_raw = so.get("create_date")
            if not start_raw:
                continue
            try:
                dt_start = datetime.fromisoformat(start_raw)
            except Exception:
                continue

            pickings = self.api.search_read(
                "stock.picking",
                [
                    ["origin", "=", so_name],
                    ["picking_type_id.code", "=", "outgoing"],
                    ["state", "=", "done"],
                ],
                ["id", "date_done"],
                limit=10,
            )
            if not pickings:
                continue

            dates_done: List[datetime] = []
            for p in pickings:
                d = p.get("date_done")
                if not d:
                    continue
                try:
                    dates_done.append(datetime.fromisoformat(d))
                except Exception:
                    continue

            if not dates_done:
                continue

            dt_end = max(dates_done)
            lead_days = (dt_end - dt_start).total_seconds() / 86400.0
            log_info(f"[KPI:LT] Lead Time für SO {so_name}: {lead_days:.2f} Tage.")
            return lead_days

        log_warn("[KPI:LT] Kein SO mit abgeschlossener Lieferung gefunden – Lead-Time = 0.")
        return 0.0

    # -------------------------------------------------------------------------
    # Gesamt-Report
    # -------------------------------------------------------------------------

    def generate_report(self) -> Dict:
        """
        Erzeugt einen KPI-Report über Fertigung, QC, Lager und Lead Time.
        """
        log_info("Erzeuge KPI-Report...")

        mo_perf = self.get_mo_performance()
        qc_metrics = self.get_qc_metrics()
        inv_metrics = self.get_inventory_metrics()
        lead_time = self._get_example_lead_time()

        report = {
            "mo_performance": mo_perf,
            "qc_metrics": qc_metrics,
            "inventory_metrics": inv_metrics,
            "example_lead_time_days": lead_time,
        }

        log_info("KPI-Report erstellt.")
        log_kpi_summary(report)
        return report


def setup_kpi_dashboards(api: OdooClient, base_data_dir: Optional[str] = None) -> None:
    """
    Einstiegspunkt für den Runner: erzeugt einen KPI-Report.
    """
    extractor = KPIExtractor(api=api, base_data_dir=base_data_dir)
    report = extractor.generate_report()
    log_info(f"[KPI] Rohdaten: {report}")

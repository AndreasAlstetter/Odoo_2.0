# scripts/run_provisioning.py
import typer
from provisioning.runner import run
from provisioning.flows.kpi_extractor import KPIExtractor
from provisioning.utils import log_header, log_success, log_error

app = typer.Typer(add_completion=False)

@app.command()
def provision(
    kpi_only: bool = typer.Option(False, help="Nur KPI-Report berechnen"),
):
    """
    Führt die komplette Odoo-Provisionierung aus:
    Produkte, Lieferanten, Stücklisten, Routings, Qualität, KPI-Report.
    """
    log_header("Odoo-Provisioning gestartet")
    try:
        run(kpi_only=kpi_only)
    except Exception as exc:
        log_error(f"Provisioning failed: {exc}")
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()

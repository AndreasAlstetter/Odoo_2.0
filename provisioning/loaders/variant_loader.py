from provisioning.client import OdooClient
from provisioning.variant_logic import run_variant_generation
from provisioning.utils import (
    log_header,
    log_success,
    log_error,
)


class VariantLoader:
    def __init__(self, client: OdooClient, base_data_dir: str) -> None:
        self.client = client
        # aktuell nicht zwingend nötig, bleibt für spätere Erweiterungen
        self.base_data_dir = base_data_dir

    def run(self) -> None:
        log_header("Varianten & Varianten-BOMs generieren")
        try:
            run_variant_generation(self.client)
            log_success("Variantengenerierung abgeschlossen.")
        except Exception as exc:
            log_error(f"Variantengenerierung fehlgeschlagen: {exc}")
            # Optional: Fehler weiterreichen, wenn der Gesamt-Run abbrechen soll
            # raise

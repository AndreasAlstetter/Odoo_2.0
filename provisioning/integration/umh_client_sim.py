# provisioning/integration/umh_client_sim.py

from __future__ import annotations

from typing import Dict, List, Optional
from pathlib import Path
import json

from provisioning.config import UMH_EVENTS_ENDTOEND_FILE


EVENT_FILE = Path(UMH_EVENTS_ENDTOEND_FILE)


class UMHClientSimulator:
    """
    Simuliert einen UMH-Client (MQTT/HTTP) durch pures Dateischreiben.
    Events werden in-memory gesammelt und können als JSON-Datei exportiert werden.
    """

    def __init__(self, use_mqtt: bool = False, output_file: Optional[str] = None) -> None:
        self.use_mqtt = use_mqtt
        self.output_file = Path(output_file) if output_file else EVENT_FILE
        self.events_sent: List[Dict] = []

    def send_event(self, event: Dict) -> bool:
        self.events_sent.append(event)
        return True

    def send_events_batch(self, events: List[Dict]) -> bool:
        for evt in events:
            self.send_event(evt)
        return True

    def get_sent_events(self) -> List[Dict]:
        return list(self.events_sent)

    def export_to_file(self) -> bool:
        try:
            with self.output_file.open("w", encoding="utf-8") as f:
                json.dump(self.events_sent, f, ensure_ascii=False, indent=2)
            return True
        except OSError:
            return False

    def clear_events(self) -> None:
        self.events_sent.clear()

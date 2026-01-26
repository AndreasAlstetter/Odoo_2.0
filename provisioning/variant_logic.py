# provisioning/variant_logic.py - DEPRECATED MODULE

"""
🚨 DEPRECATED: variant_logic.py ist NICHT MEHR ERFORDERLICH!

v4.2 UPGRADE:
─────────────
✅ Varianten werden automatisch von Odoo generiert (ProductsLoader v4.2)
✅ BoMs sind variant-aware (BomLoader v2.0)
✅ Keine manuelle Variantengenerierung mehr nötig!

ALTE FUNKTIONALITÄT (v1.0):
────────────────────────────
- Manuelle Erstellung von 192 Varianten pro Drohne
- Manuelle Zuordnung von Attributen
- Manuelle BoM-Erstellung für jede Variante

NEUE FUNKTIONALITÄT (v4.2):
────────────────────────────
1. ProductsLoaderAdvanced erstellt Templates mit attribute_line_ids
2. Odoo generiert automatisch alle Varianten (8×8×3 = 192)
3. BomLoader erstellt variant-spezifische BoM-Lines basierend auf Namen

MIGRATION:
──────────
Entfernen Sie alle Referenzen zu:
- run_variant_generation()
- load_mengenstueckliste()
- generate_all_configs()
- create_bom_for_config()

Verwenden Sie stattdessen:
- ProductsLoaderAdvanced v4.2 (Phase 2A: Drohnen mit Attributen)
- BomLoader v2.0 (variant-aware BoM Lines)
"""

from provisioning.client import OdooClient
from provisioning.utils import log_warn

def run_variant_generation(api: OdooClient) -> None:
    """
    🚨 DEPRECATED: Diese Funktion wird nicht mehr benötigt!
    
    Varianten werden automatisch von ProductsLoaderAdvanced v4.2 generiert.
    Bitte entfernen Sie Aufrufe zu dieser Funktion.
    """
    log_warn("⚠️ [DEPRECATED] run_variant_generation() ist veraltet!")
    log_warn("⚠️ Varianten werden automatisch von ProductsLoaderAdvanced v4.2 generiert.")
    log_warn("⚠️ Bitte entfernen Sie Aufrufe zu variant_logic.run_variant_generation().")
    
    # Return silently - no action needed
    return

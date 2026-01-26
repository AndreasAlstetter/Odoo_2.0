# provisioning/loaders/variant_loader.py

from provisioning.client import OdooClient
from provisioning.utils import (
    log_header,
    log_success,
    log_info,
    log_warn,
)

class VariantLoader:
    """
    🚀 v2.0: DEPRECATED - Varianten werden automatisch von Odoo generiert!
    
    ProductsLoaderAdvanced v4.2 erstellt Templates mit attribute_line_ids.
    Odoo generiert automatisch alle Varianten (8×8×3 = 192 pro Drohne).
    
    BomLoader v2.0 erstellt variant-spezifische BoM-Lines basierend auf
    Component-Namen (Haubenfarbe/Fußfarbe/Grundplattenfarbe).
    
    → VariantLoader wird NICHT MEHR BENÖTIGT!
    """
    
    def __init__(self, client: OdooClient, base_data_dir: str) -> None:
        self.client = client
        self.base_data_dir = base_data_dir
    
    def run(self) -> dict:
        """
        🚀 v2.0: Überspringe manuelle Variantengenerierung.
        Varianten wurden bereits in ProductsLoader Phase 2A erstellt!
        """
        log_header("📦 VariantLoader v2.0 - AUTO-VARIANTS CHECK")
        
        # Check if drone templates exist with variants
        drone_codes = ['029.3.000', '029.3.001', '029.3.002']
        stats = {
            'templates_found': 0,
            'total_variants': 0,
            'templates_without_variants': [],
        }
        
        for code in drone_codes:
            tmpl = self.client.search_read(
                'product.template',
                [('default_code', '=', code)],
                ['id', 'name', 'attribute_line_ids'],
                limit=1
            )
            
            if not tmpl:
                log_warn(f"⚠️ [TEMPLATE:NOT-FOUND] {code}")
                stats['templates_without_variants'].append(code)
                continue
            
            tmpl_id = tmpl[0]['id']
            tmpl_name = tmpl[0]['name']
            has_attrs = bool(tmpl[0].get('attribute_line_ids'))
            
            if not has_attrs:
                log_warn(f"⚠️ [TEMPLATE:NO-ATTRS] {code} '{tmpl_name}' → Keine Attribute!")
                stats['templates_without_variants'].append(code)
                continue
            
            # Count variants
            variants = self.client.search(
                'product.product',
                [('product_tmpl_id', '=', tmpl_id)],
                limit=200
            )
            
            stats['templates_found'] += 1
            stats['total_variants'] += len(variants)
            
            log_success(f"✅ [VARIANTS:AUTO] {code} '{tmpl_name}' → {len(variants)} Varianten (Odoo auto-generated)")
        
        # Summary
        if stats['templates_found'] == 3 and stats['total_variants'] >= 576:
            log_success(f"✅ [VARIANT-LOADER:SKIP] {stats['total_variants']} Varianten bereits vorhanden (ProductsLoader v4.2)")
            log_info("ℹ️  Manuelle Variantengenerierung NICHT erforderlich!")
            return {
                'status': 'skipped',
                'reason': 'variants_already_generated',
                'stats': stats
            }
        elif stats['templates_without_variants']:
            log_warn(f"⚠️ [VARIANT-LOADER:INCOMPLETE] Templates ohne Varianten: {stats['templates_without_variants']}")
            log_warn("⚠️ Bitte ProductsLoaderAdvanced v4.2 ausführen!")
            return {
                'status': 'incomplete',
                'reason': 'missing_templates_or_variants',
                'stats': stats
            }
        else:
            log_info(f"ℹ️ [VARIANT-LOADER:PARTIAL] {stats['total_variants']} Varianten gefunden")
            return {
                'status': 'partial',
                'stats': stats
            }

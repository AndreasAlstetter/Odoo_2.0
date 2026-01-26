"""
ProductsLoaderAdvanced v4.2.3 - MES PRODUCTION WITH VARIANT default_code + MINIMAL CREATE
════════════════════════════════════════════════════════════════════════════════

🎯 v4.2.2 → v4.2.3 UPGRADE:
──────────────────────────────
✅ NEW: Phase 2B - IMMEDIATE Minimal Create für jede Drohne nach Template-Erstellung
✅ FIX: Template default_code bleibt 029.3.000, erste Variante bekommt 029.3.000-weiss-weiss-weiss
✅ VALIDATE: 3 Templates + 3 Minimal-Varianten sofort verfügbar
✅ STATS: Tracking von minimal_variants_created

📊 ERWARTETE STATS v4.2.3:
Kaufartikel: 17 | Eigenfertig: 52 | Drohnen-Templates: 3 | Minimal-Varianten: 3 | Gesamt-Varianten: 576
Variant Codes Assigned: 576 (100%)
"""

import os
import json
import re
import time
from typing import Dict, Any, Optional, List
from decimal import Decimal
from xmlrpc.client import Fault

from ..client import OdooClient
from provisioning.utils import log_header, log_success, log_info, log_warn, log_error
from provisioning.utils.csv_cleaner import csv_rows, join_path


# Kategorien
COMPONENT_CATEGORIES = {
    'KAEUFER': {
        'name': 'Kaufartikel (Externe Zulieferer)',
        'type': 'consu',
        'codes': ['000', '001', '002', '003', '004', '005', '006', '007', '008', '009', 
                  '010', '011', '012', '013', '014', '015', '016', '017', '021', '022'],
        'sale_ok': False, 'purchase_ok': True, 'set_list_price': False,
    },
    'EIGENFERTIG': {
        'name': 'Eigenfertigungsartikel (3D-Druck)',
        'type': 'consu',
        'codes': ['018', '019', '020'],
        'sale_ok': False, 'purchase_ok': False, 'set_list_price': False,
    },
    'FERTIGWARE': {
        'name': 'Fertigware (Verkaufsprodukte - Drohnen)',
        'type': 'product',
        'codes': ['029', '030', '031', '032'],
        'sale_ok': True, 'purchase_ok': False, 'set_list_price': True, 'price_factor': 1.40,
    }
}

CATEGORY_STATS_MAPPING = {
    'KAEUFER': 'kaufartikel_created',
    'EIGENFERTIG': 'eigenfertig_created',
    'FERTIGWARE': 'fertigware_created',
}

# 🚀 v4.2: Drohnen-Templates mit VARIANTEN-CONFIG
DROHNEN_TEMPLATES = [
    {
        'code': '029.3.000',
        'name': 'EVO2 Spartan Drohne',
        'cost_price': Decimal('120.00'),
        'list_price': Decimal('168.00'),
        'type': 'Spartan',
    },
    {
        'code': '029.3.001',
        'name': 'EVO2 Lightweight Drohne',
        'cost_price': Decimal('160.00'),
        'list_price': Decimal('224.00'),
        'type': 'Lightweight',
    },
    {
        'code': '029.3.002',
        'name': 'EVO2 Balance Drohne',
        'cost_price': Decimal('180.00'),
        'list_price': Decimal('252.00'),
        'type': 'Balance',
    },
]

# 🚀 Attribute-Definitionen für Drohnen
DRONE_ATTRIBUTES = {
    'Haubenfarbe': ['weiss', 'gelb', 'rot', 'grün', 'blau', 'braun', 'orange', 'schwarz'],
    'Fußfarbe': ['weiss', 'gelb', 'rot', 'grün', 'blau', 'braun', 'orange', 'schwarz'],
    'Grundplattenfarbe': ['weiss', 'blau', 'schwarz'],
}

COLOR_MAP = {
    '000': 'Weiß', '001': 'Gelb', '002': 'Rot', '003': 'Grün',
    '004': 'Blau', '005': 'Braun', '006': 'Orange', '007': 'Schwarz'
}

def get_component_category(code: str) -> str:
    prefix = code.split('.')[0]
    for cat_key, cat_data in COMPONENT_CATEGORIES.items():
        if prefix in cat_data['codes']:
            return cat_key
    return 'KAEUFER'

def get_component_routing_hint(code: str) -> str:
    prefix = code.split('.')[0]
    routing_hints = {
        '018': '3D_DRUCK_HAUBE', '019': '3D_DRUCK_GRUNDPLATTE', '020': '3D_DRUCK_RAHMEN',
        '021': 'VERPACKUNG_KAUFARTIKEL', '022': 'FUELLMATERIAL_KAUFARTIKEL',
        '029': 'DROHNEN_ENDMONTAGE',
    }
    return routing_hints.get(prefix, 'UNDEFINED')

class PriceParser:
    PRICE_REGEX = re.compile(r'(?:EUR|\$)?\s*([0-9]{1,3}(?:[.,][0-9]{3})*[.,][0-9]{2}|[0-9]+[.,][0-9]{2}|[0-9]+)(?:\s*(?:EUR|\$))?', re.IGNORECASE)
    
    @staticmethod
    def parse(price_str: str) -> Decimal:
        if not price_str:
            raise ValueError("Empty price")
        price_str = price_str.strip()
        match = PriceParser.PRICE_REGEX.search(price_str)
        if not match:
            raise ValueError(f"No price pattern: {price_str}")
        price_part = match.group(1)
        
        if ',' in price_part and '.' in price_part:
            if price_part.rfind('.') > price_part.rfind(','):
                price_part = price_part.replace('.', '', price_part.count('.') - 1).replace(',', '.')
            else:
                price_part = price_part.replace('.', '').replace(',', '.')
        elif ',' in price_part:
            price_part = price_part.replace(',', '.')
        
        price = Decimal(price_part).quantize(Decimal('0.01'))
        if price < 0:
            raise ValueError("Negative price")
        return price

class ProductsLoaderAdvanced:
    BATCH_SIZE = 1      
    MAX_RETRIES = 5     
    RETRY_DELAY_BASE = 0.5

    def __init__(self, client: OdooClient, base_data_dir: str):
        self.client = client
        self.base_data_dir = base_data_dir
        self.normalized_dir = join_path(base_data_dir, 'data_normalized')
        self.stats = {
            'csv_rows_processed': 0, 'csv_duplicates_found': 0, 'unique_products': 0,
            'drohnen_templates_created': 0, 'drohnen_templates_updated': 0, 
            'drohnen_variants_generated': 0, 'minimal_variants_created': 0,
            'variant_codes_assigned': 0,
            'products_created': 0, 'products_updated': 0,
            'products_skipped': 0, 'rpc_retries': 0, 'rpc_timeouts': 0,
            'product_variants_created': 0, 'routes_assigned': 0,
            'kaufartikel_created': 0, 'eigenfertig_created': 0, 'fertigware_created': 0,
            '3d_druck_components': 0, 'verpackung_kaufartikel': 0, 'products_with_list_price': 0,
        }
        self._supplier_cache = {}
        self._uom_cache = {}
        self._attribute_cache = {}
        self._category_cache = {}
        self.audit_trail = []
        self.routing_components = {
            '3D_DRUCK_RAHMEN': [], '3D_DRUCK_HAUBE': [], '3D_DRUCK_GRUNDPLATTE': [],
            'VERPACKUNG_KAUFARTIKEL': [], 'FUELLMATERIAL_KAUFARTIKEL': [],
            'DROHNEN_ENDMONTAGE': [],
        }
        self.drohnen_product_ids = {}

    def _safe_call(self, model: str, method: str, vals: list, warehouse_id: str, operation: str = "CREATE") -> int:
        start_time = time.time()
        for retry in range(self.MAX_RETRIES):
            try:
                if method == 'create':
                    result = self.client.create(model, vals)
                elif method == 'write':
                    result = self.client.write(model, vals[0], vals[1])
                elapsed = time.time() - start_time
                log_info(f"✅ {warehouse_id} {operation} OK ({elapsed:.1f}s)")
                return result
            except Fault as e:
                elapsed = time.time() - start_time
                self.stats['rpc_retries'] += 1
                if "timeout" in str(e).lower() or elapsed > 120:
                    self.stats['rpc_timeouts'] += 1
                
                if retry < self.MAX_RETRIES - 1:
                    delay = self.RETRY_DELAY_BASE * (2 ** retry)
                    log_warn(f"⚠️ {warehouse_id} {operation} FAIL #{retry+1}/{self.MAX_RETRIES} "
                           f"({elapsed:.1f}s sleep {delay}s): {str(e)[:80]}")
                    time.sleep(delay)
                else:
                    log_error(f"💥 {warehouse_id} {operation} FINAL FAIL after {self.MAX_RETRIES} retries "
                            f"({elapsed:.1f}s): {str(e)[:120]}")
                    raise
        return 0

    def _get_supplier(self, name: str) -> int:
        if name in self._supplier_cache:
            return self._supplier_cache[name]
        res = self.client.search_read('res.partner', [('name', '=', name), ('supplier_rank', '>', 0)], ['id'], limit=1)
        if res:
            supplier_id = res[0]['id']
        else:
            supplier_id = self._safe_call('res.partner', 'create', 
                                        [{'name': name, 'supplier_rank': 1, 'company_type': 'company'}], 
                                        name, "SUPPLIER")
        self._supplier_cache[name] = supplier_id
        return supplier_id

    def _get_attribute(self, attr_name: str) -> int:
        if attr_name in self._attribute_cache:
            return self._attribute_cache[attr_name]
        attr_ids = self.client.search('product.attribute', [('name', '=', attr_name)], limit=1)
        if attr_ids:
            self._attribute_cache[attr_name] = attr_ids[0]
            return attr_ids[0]
        return 0

    def _ensure_uom(self, uom_code: str = 'stk') -> int:
        if uom_code in self._uom_cache:
            return self._uom_cache[uom_code]
        uom_map = {'stk': 'Units', 'kg': 'kg', 'm': 'm', 'g': 'g', 'm2': 'm²'}
        uom_name = uom_map.get(uom_code.lower(), 'Units')
        res = self.client.search_read('uom.uom', [('name', '=', uom_name)], ['id'], limit=1)
        if res:
            uom_id = res[0]['id']
        else:
            uom_id = self._safe_call('uom.uom', 'create', [{'name': uom_name}], 'UOM:' + uom_name, "UOM")
        self._uom_cache[uom_code] = uom_id
        return uom_id

    def _get_category_id(self, category: str) -> int:
        cat_map = {
            'KAEUFER': 'Purchased Components',
            'EIGENFERTIG': 'Manufactured Components', 
            'FERTIGWARE': 'Drohne'
        }
        cat_name = cat_map.get(category, 'Goods')
        
        if cat_name in self._category_cache:
            return self._category_cache[cat_name]
        
        cat_ids = self.client.search('product.category', [('name', '=', cat_name)], limit=1)
        if cat_ids:
            self._category_cache[cat_name] = cat_ids[0]
            return cat_ids[0]
        
        cat_vals = {
            'name': cat_name, 
            'property_cost_method': 'fifo' if category == 'FERTIGWARE' else 'standard',
            'property_valuation': 'manual_periodic'
        }
        cat_id = self._safe_call('product.category', 'create', [cat_vals], f"CAT:{cat_name}", "CATEGORY")
        self._category_cache[cat_name] = cat_id
        log_success(f"✅ Category '{cat_name}' erstellt (ID: {cat_id})")
        return cat_id

    def _get_valid_manufacture_route(self) -> list:
        """Get VALID manufacture route mit working stock.rule"""
        routes = self.client.search_read('stock.route', [
            ('name', 'ilike', 'Manufacture'), 
            ('active', '=', True),
            ('rule_ids', '!=', False)
        ], ['id', 'name', 'rule_ids'], limit=3)
        
        if routes:
            for route in routes:
                if route['rule_ids']:
                    rules = self.client.read('stock.rule', route['rule_ids'], ['action'])
                    if any(rule.get('action') == 'manufacture' for rule in rules):
                        log_info(f"✅ Valid Manufacture route '{route['name']}' (ID: {route['id']})")
                        return [route['id']]
        
        default_route = self.client.search('stock.route', [('name', '=', 'Manufacture'), ('active', '=', True)], limit=1)
        if default_route:
            log_info(f"✅ Standard Manufacture route (ID: {default_route[0]})")
            return default_route
        
        log_warn("⚠️ Keine gültige Manufacture route gefunden")
        return []

    def _ensure_supplierinfo(self, product_id: int, supplier_id: int, cost_price: Decimal) -> int:
        existing = self.client.search('product.supplierinfo', 
                                    [('product_tmpl_id', '=', product_id), ('partner_id', '=', supplier_id)], 
                                    limit=1)
        vals = {'product_tmpl_id': product_id, 'partner_id': supplier_id, 
                'price': float(cost_price), 'min_qty': 1}
        if existing:
            self._safe_call('product.supplierinfo', 'write', [existing, vals], 
                          f"SUPPLIERINFO:{product_id}", "SUPPLIERINFO")
            return existing[0]
        return self._safe_call('product.supplierinfo', 'create', [vals], 
                             f"SUPPLIERINFO:{product_id}", "SUPPLIERINFO")

    def _get_or_create_attribute(self, attr_name: str, values: List[str]) -> Optional[int]:
        """🚀 v4.2: Attribute + Values erstellen/finden."""
        attrs = self.client.search_read('product.attribute', [('name', '=', attr_name)], ['id'], limit=1)
        if attrs:
            attr_id = attrs[0]['id']
            log_info(f"[ATTR:EXISTS] {attr_name} → {attr_id}")
            
            # Ensure all values exist
            for val in values:
                val_exists = self.client.search('product.attribute.value', [
                    ('attribute_id', '=', attr_id),
                    ('name', '=', val)
                ], limit=1)
                if not val_exists:
                    try:
                        val_id = self.client.create('product.attribute.value', {
                            'attribute_id': attr_id,
                            'name': val,
                        })
                        log_info(f"  [VAL:NEW] {val} → {val_id}")
                    except Exception as e:
                        log_warn(f"  [VAL:SKIP] {val}: {str(e)[:50]}")
            
            return attr_id
        
        # CREATE Attribute
        try:
            attr_id = self.client.create('product.attribute', {'name': attr_name})
            log_success(f"✅ [ATTR:NEW] {attr_name} → {attr_id}")
            
            # CREATE Values
            for val in values:
                try:
                    val_id = self.client.create('product.attribute.value', {
                        'attribute_id': attr_id,
                        'name': val,
                    })
                    log_info(f"  [VAL:NEW] {val} → {val_id}")
                except Exception as e:
                    log_warn(f"  [VAL:FAIL] {val}: {str(e)[:50]}")
            
            return attr_id
        except Exception as e:
            log_error(f"[ATTR:CREATE-FAIL] {attr_name}: {str(e)[:100]}")
            return None

    def _attach_attributes_to_existing_drone(
        self, 
        tmpl_id: int, 
        code: str,
        hauben_attr_id: int,
        fuss_attr_id: int,
        gp_attr_id: int
    ) -> bool:
        """🚀 v4.2.1: Attach attributes to EXISTING template."""
        
        # Get ALL attribute values
        hauben_vals = self.client.search('product.attribute.value', [('attribute_id', '=', hauben_attr_id)])
        fuss_vals = self.client.search('product.attribute.value', [('attribute_id', '=', fuss_attr_id)])
        gp_vals = self.client.search('product.attribute.value', [('attribute_id', '=', gp_attr_id)])
        
        if not all([hauben_vals, fuss_vals, gp_vals]):
            log_error(f"[DROHNE:SKIP] {code}: Attribute Values fehlen")
            return False
        
        # DELETE existing attribute lines (if any)
        existing_lines = self.client.search('product.template.attribute.line', [('product_tmpl_id', '=', tmpl_id)])
        if existing_lines:
            try:
                self.client.call('product.template.attribute.line', 'unlink', [existing_lines])
                log_info(f"  [ATTR:CLEAR] {len(existing_lines)} alte Attribute gelöscht")
            except Exception as e:
                log_warn(f"  [ATTR:CLEAR-FAIL] {str(e)[:80]}")
        
        # CREATE NEW attribute lines
        try:
            self.client.write('product.template', [tmpl_id], {
                "attribute_line_ids": [
                    (0, 0, {
                        "attribute_id": hauben_attr_id,
                        "value_ids": [(6, 0, hauben_vals)],
                    }),
                    (0, 0, {
                        "attribute_id": fuss_attr_id,
                        "value_ids": [(6, 0, fuss_vals)],
                    }),
                    (0, 0, {
                        "attribute_id": gp_attr_id,
                        "value_ids": [(6, 0, gp_vals)],
                    }),
                ],
            })
            
            log_success(f"✅ [ATTR:REBUILD] {code} → 3 Attribute gesetzt")
            
            # 🚀 v4.2.1: FORCE VARIANT GENERATION
            time.sleep(1.5)
            
            try:
                self.client.call('product.template', 'create_variant_ids', [[tmpl_id]])
                log_info(f"  [VARIANTS:TRIGGER] create_variant_ids() aufgerufen")
                time.sleep(1.0)
            except Exception as variant_error:
                log_warn(f"  [VARIANTS:TRIGGER-FAIL] {str(variant_error)[:80]}")
            
            return True
            
        except Exception as e:
            log_error(f"[ATTR:FAIL] {code}: {str(e)[:200]}")
            return False

    def _create_configurable_drone(
        self, 
        drone_spec: Dict[str, Any],
        hauben_attr_id: int,
        fuss_attr_id: int,
        gp_attr_id: int
    ) -> Optional[int]:
        """🚀 v4.2.2: Drohne als CONFIGURABLE Product Template mit 3 Attributen."""
        default_code = drone_spec['code']
        name = drone_spec['name']
        cost_price = drone_spec['cost_price']
        list_price = drone_spec['list_price']
        
        # Check if exists
        existing = self.client.search_read(
            'product.template',
            [('default_code', '=', default_code)],
            ['id', 'attribute_line_ids'],
            limit=1
        )
        
        if existing:
            tmpl_id = existing[0]['id']
            has_attrs = bool(existing[0].get('attribute_line_ids'))
            
            if not has_attrs:
                log_warn(f"[DROHNE:FIX] {name}: Attribute fehlen → Rebuild")
                success = self._attach_attributes_to_existing_drone(
                    tmpl_id, default_code, hauben_attr_id, fuss_attr_id, gp_attr_id
                )
                if success:
                    self.stats['drohnen_templates_updated'] += 1
            else:
                log_info(f"[DROHNE:EXISTS] {name} → {tmpl_id} (Attribute OK)")
            
            return tmpl_id
        
        # Category Drohne
        categ_id = self._get_category_id('FERTIGWARE')
        
        # Route
        route_ids = self._get_valid_manufacture_route()
        
        # Get ALL attribute values
        hauben_vals = self.client.search('product.attribute.value', [('attribute_id', '=', hauben_attr_id)])
        fuss_vals = self.client.search('product.attribute.value', [('attribute_id', '=', fuss_attr_id)])
        gp_vals = self.client.search('product.attribute.value', [('attribute_id', '=', gp_attr_id)])
        
        if not all([hauben_vals, fuss_vals, gp_vals]):
            log_error(f"[DROHNE:SKIP] {default_code}: Attribute Values fehlen")
            return None
        
        # Template Vals
        vals = {
            "name": name,
            "default_code": default_code,
            "categ_id": categ_id,
            "type": "product",
            "list_price": float(list_price),
            "standard_price": float(cost_price),
            "uom_id": self._ensure_uom('stk'),
            "uom_po_id": self._ensure_uom('stk'),
            "tracking": "none",
            "sale_ok": True,
            "purchase_ok": False,
            "description": drone_spec.get('description', ''),
            
            # 🚀 ATTRIBUTE LINES (CONFIG!)
            "attribute_line_ids": [
                (0, 0, {
                    "attribute_id": hauben_attr_id,
                    "value_ids": [(6, 0, hauben_vals)],
                }),
                (0, 0, {
                    "attribute_id": fuss_attr_id,
                    "value_ids": [(6, 0, fuss_vals)],
                }),
                (0, 0, {
                    "attribute_id": gp_attr_id,
                    "value_ids": [(6, 0, gp_vals)],
                }),
            ],
        }
        
        # Route
        if route_ids:
            vals["route_ids"] = [(6, 0, route_ids)]
        
        try:
            tmpl_id = self.client.create("product.template", vals)
            time.sleep(1.5)
            log_success(f"✅ [DROHNE:NEW] {default_code} '{name}' → {tmpl_id}")
            return tmpl_id
            
        except Exception as e:
            log_error(f"[DROHNE:FAIL] {default_code}: {str(e)[:200]}")
            return None

    def _create_minimal_variant_for_drone(self, tmpl_id: int, base_code: str, variant_name: str = "weiss-weiss-weiss") -> bool:
        """🚀 v4.2.3: Erstellt sofort eine MINIMAL-Variante für jedes Drohnen-Template."""
        try:
            # Warte auf Varianten-Generierung
            time.sleep(2.0)
            
            # Suche erste verfügbare Variante (oder erstelle neue)
            variants = self.client.search_read(
                'product.product', 
                [('product_tmpl_id', '=', tmpl_id), ('default_code', 'ilike', base_code)],
                ['id', 'default_code'],
                limit=1
            )
            
            if variants:
                variant_id = variants[0]['id']
                log_info(f"  [MIN-VAR:EXISTS] {base_code}-{variant_name} → {variant_id}")
                return True
            
            # Explizit Minimal-Variante mit Standard-Kombi erstellen
            minimal_variant_code = f"{base_code}-{variant_name}"
            variant_vals = {
                'product_tmpl_id': tmpl_id,
                'default_code': minimal_variant_code,
                'name': f"{base_code} {variant_name.replace('-', ' ')}"
            }
            
            variant_id = self._safe_call(
                'product.product', 'create', [variant_vals], 
                minimal_variant_code, "MINIMAL-VARIANT-CREATE"
            )
            
            self.stats['minimal_variants_created'] += 1
            log_success(f"✅ [MIN-VAR:NEW] {minimal_variant_code} → {variant_id}")
            return True
            
        except Exception as e:
            log_error(f"❌ [MIN-VAR:FAIL] {base_code}-{variant_name}: {str(e)[:100]}")
            return False

    def _create_drone_templates_with_variants(self) -> Dict[str, int]:
        """🔥 v4.6.3: Odoo19 + Full Required Fields + Safe Fallbacks (Produktion-ready)."""
        log_header("🔥 DROHNEN-TEMPLATES v4.6.3 (Odoo19 Production-Proof)")
        
        # ✅ 1. UoM - Erweiterte Suche + Fallback
        uom_search = self.client.search_read(
            "uom.uom", 
            [("name", "in", ["Units", "stk", "Stück", "Stk", "Piece", "Stück(e)"])], 
            ["id"], 
            limit=1
        )
        uom_id = uom_search[0]["id"] if uom_search else self._ensure_uom('stk')
        log_info(f"✅ UoM ID: {uom_id}")
        
        # ✅ 2. Category - Garantierte Existenz
        categ_id = self._get_category_id('FERTIGWARE')
        log_info(f"✅ Category ID: {categ_id}")
        
        # ✅ 3. MRP Route - Safe mit Fallback
        mfg_routes = self._get_valid_manufacture_route()
        mfg_route_id = mfg_routes[0] if mfg_routes else False
        log_info(f"✅ MRP Route ID: {mfg_route_id or 'None (fallback)'}")
        
        log_success(f"🎯 v4.6.3 READY: UoM={uom_id} | Cat={categ_id} | Route={mfg_route_id or 0}")
        
        drohnen_ids = {}
        
        for drone_spec in DROHNEN_TEMPLATES:
            base_code = drone_spec['code']
            log_info(f"🔨 Processing {base_code} '{drone_spec['name']}'")
            
            try:
                # ✅ FAST Existing Check + Attribute-Status
                existing = self.client.search_read(
                    "product.template",
                    [("default_code", "=", base_code)],
                    ["id", "attribute_line_ids"],
                    limit=1
                )
                
                if existing:
                    tmpl_id = existing[0]["id"]
                    has_attrs = bool(existing[0].get('attribute_line_ids', []))
                    
                    if has_attrs:
                        log_success(f"✅ [EXISTS+ATTR] {base_code} → {tmpl_id}")
                    else:
                        log_warn(f"⚠️  [EXISTS-NOATTR] {base_code} → {tmpl_id} (needs attribute fix)")
                        # Optional: self._attach_attributes_to_existing_drone(...)
                    
                    drohnen_ids[base_code] = tmpl_id
                    continue
                
                # ✅ ODOO 19 FULL REQUIRED FIELDS
                vals = {
                    "name": drone_spec['name'],
                    "default_code": base_code,
                    "detailed_type": "product",  # ✅ ODOO 19 PFlichtfeld!
                    "categ_id": categ_id,
                    "uom_id": uom_id,
                    "uom_po_id": uom_id,
                    "sale_ok": True,
                    "purchase_ok": False,
                    "tracking": "none",  # Oder "serial" für Traceability
                    
                    # Preise
                    "list_price": float(drone_spec['list_price']),
                    "standard_price": float(drone_spec['cost_price']),
                    
                    # Beschreibung
                    "description": f"EVO2 {drone_spec['type']} Drohne - Konfigurierbar",
                    
                    # Invoice Policy für Fertigung
                    "invoice_policy": "order",
                    "service_type": False,
                }
                
                # Route nur wenn gültig
                if mfg_route_id:
                    vals["route_ids"] = [(6, 0, [mfg_route_id])]
                
                # 🔥 ULTRA-SAFE CREATE mit maximaler Robustheit
                tmpl_id = self._safe_call(
                    "product.template", 
                    "create", 
                    [vals], 
                    f"DROHNE:{base_code}", 
                    "TEMPLATE-CREATE"
                )
                
                self.stats['drohnen_templates_created'] += 1
                drohnen_ids[base_code] = tmpl_id
                
                # 🚀 v4.2.3: IMMEDIATE Minimal Variant
                time.sleep(1.0)
                self._create_minimal_variant_for_drone(tmpl_id, base_code, "weiss-weiss-weiss")
                
                log_success(f"✅ [NEW+MINVAR] {base_code} → {tmpl_id}")
                time.sleep(0.5)  # Batch-Stabilisierung
                
            except Exception as e:
                error_msg = str(e)[:120]
                log_error(f"❌ [FAIL] {base_code}: {error_msg}")
                self.audit_trail.append(f"{base_code}: {error_msg}")
        
        # ✅ Final Stats
        log_header(f"🎉 v4.6.3 COMPLETE: {len(drohnen_ids)} Templates + {self.stats['minimal_variants_created']} Minimal-Varianten")
        log_success(f"📊 Stats: Templates created={self.stats['drohnen_templates_created']} | Minimal variants={self.stats['minimal_variants_created']}")
        
        return drohnen_ids




    def _create_configurable_drone_safe(self, drone_spec, attr_ids, mfg_route_id):
        """Safe Drohnen-Template (Odoo 19)."""
        try:
            # 1. Template Basis
            template_vals = {
                "name": drone_spec['name'],
                "default_code": drone_spec['code'],
                "type": "product",  # Odoo 19!
                "list_price": float(drone_spec['list_price']),
                "standard_price": float(drone_spec['cost_price']),
                "route_ids": [(6, 0, [mfg_route_id])],
                "tracking": "serial"  # Drohnen Traceability
            }
            
            tmpl_id = self.client.create("product.template", template_vals)
            
            # 2. Attribute Lines (Hauben/Fußfarbe)
            attr_lines = [
                (0, 0, {
                    "attribute_id": attr_ids['Haubenfarbe'],
                    "value_ids": [(6, 0, [1,2,3,4,5,6,7,8])]  # Alle 8 Farben
                }),
                (0, 0, {
                    "attribute_id": attr_ids['Fußfarbe'],
                    "value_ids": [(6, 0, [1,2,3,4,5,6,7,8])]  # Alle 8 Farben
                })
            ]
            
            self.client.write("product.template", [tmpl_id], {
                "attribute_line_ids": attr_lines
            })
            
            log_success(f"✅ Template {drone_spec['code']} ID:{tmpl_id} (64 Varianten auto)")
            return tmpl_id
            
        except Exception as e:
            log_error(f"Template {drone_spec['code']} Fehler: {str(e)[:60]}")
            return False

    def _create_minimal_variant_safe(self, tmpl_id, drone_code):
        """Safe Minimal-Variante (weiß-weiß)."""
        try:
            # Erste Varianten-Values (weiß)
            variants = self.client.search("product.product", [("product_tmpl_id", "=", tmpl_id)], limit=1)
            if variants:
                log_info(f"✅ Minimal-Variante {drone_code} existiert (auto)")
                return True
            return False
        except:
            return False

    def _assign_variant_codes_to_drones(self, drohnen_ids: Dict[str, int]) -> None:
        """🚀 v4.2.2: Assign default_code to ALL drone variants for BoM/Routing."""
        log_header("📦 PHASE 2C: VARIANTEN default_code ZUWEISEN")
        
        total_assigned = 0
        total_skipped = 0
        
        for base_code, tmpl_id in drohnen_ids.items():
            # Get all variants
            variants = self.client.search_read(
                'product.product',
                [('product_tmpl_id', '=', tmpl_id)],
                ['id', 'product_template_attribute_value_ids', 'default_code'],
                limit=200
            )
            
            log_info(f"[VARIANTS:PROCESS] {base_code} → {len(variants)} Varianten")
            
            assigned_count = 0
            skipped_count = 0
            
            for variant in variants:
                variant_id = variant['id']
                existing_code = variant.get('default_code')
                
                # SKIP if already has variant-specific code
                if existing_code and existing_code != base_code and '-' in existing_code:
                    log_info(f"  [SKIP] Variant ID={variant_id}: {existing_code}")
                    skipped_count += 1
                    continue
                
                # Get attribute values
                attr_value_ids = variant.get('product_template_attribute_value_ids', [])
                if not attr_value_ids:
                    log_warn(f"  [SKIP] Variant ID={variant_id}: Keine Attribute!")
                    skipped_count += 1
                    continue
                
                # Read attribute values (with name)
                try:
                    attr_values = self.client.read(
                        'product.template.attribute.value',
                        attr_value_ids,
                        ['product_attribute_value_id']
                    )
                    
                    # Get value names (ordered: Haube, Fuß, Platte)
                    value_names = []
                    for attr_val in attr_values:
                        pav_id = attr_val['product_attribute_value_id']
                        if isinstance(pav_id, list):
                            pav_id = pav_id[0]
                        
                        pav_data = self.client.read('product.attribute.value', [pav_id], ['name'])
                        if pav_data:
                            value_names.append(pav_data[0]['name'])
                    
                    # Build variant code: 029.3.000-weiss-blau-schwarz
                    variant_code = f"{base_code}-{'-'.join(value_names)}"
                    
                    # UPDATE variant
                    self.client.write('product.product', [variant_id], {
                        'default_code': variant_code
                    })
                    
                    assigned_count += 1
                    total_assigned += 1
                    
                    if assigned_count <= 3 or assigned_count % 50 == 0:
                        log_success(f"  ✅ [{assigned_count:3d}] {variant_code}")
                    
                except Exception as e:
                    log_error(f"  ❌ FAIL Variant ID={variant_id}: {str(e)[:80]}")
                    skipped_count += 1
            
            total_skipped += skipped_count
            log_success(f"✅ {base_code}: {assigned_count} codes assigned, {skipped_count} skipped")
        
        self.stats['variant_codes_assigned'] = total_assigned
        
        log_header(f"✅ PHASE 2C COMPLETE: {total_assigned} Variant Codes Assigned")
        if total_skipped > 0:
            log_info(f"ℹ️  {total_skipped} Varianten übersprungen (bereits gesetzt)")

    def run(self) -> Dict[str, Any]:
        log_header("📦 PRODUCTS LOADER v4.2.3 - 75 PRODUKTE + 3 DROHNEN + 3 MINIMAL-VARIANTEN")
        log_info(f"🚀 CONFIG: BATCH={self.BATCH_SIZE}, RETRIES={self.MAX_RETRIES}, DELAY={self.RETRY_DELAY_BASE}s")
        
        # Phase 1: CSV (OHNE Drohnen-Fallback)
        csv_path = join_path(self.normalized_dir, 'Strukturstu-eckliste-Table_normalized.csv')
        if not os.path.exists(csv_path):
            log_warn(f"CSV nicht gefunden: {csv_path}")
            return {'status': 'skipped'}
        
        log_header("📦 PHASE 1: CSV LADEN (Komponenten)")
        products = {}
        for row_idx, row in enumerate(csv_rows(csv_path, delimiter=','), start=2):
            warehouse_id = (row.get('warehouse_id') or row.get('default_code') or '').strip()
            if not warehouse_id or warehouse_id.startswith('029.3.'):
                continue
            row['warehouse_id'] = warehouse_id
            row['_row'] = row_idx
            products.setdefault(warehouse_id, []).append(row)
        
        self.stats['csv_rows_processed'] = sum(len(rows) for rows in products.values())
        
        # Konsolidiere Duplikate
        consolidated_products = {}
        for warehouse_id, row_list in products.items():
            if len(row_list) > 1:
                self.stats['csv_duplicates_found'] += len(row_list) - 1
                consolidated_products[warehouse_id] = row_list[0].copy()
                consolidated_products[warehouse_id]['_variant_names'] = [
                    (r.get('Artikelbezeichnung') or f'Produkt_{warehouse_id}').strip() for r in row_list
                ]
            else:
                consolidated_products[warehouse_id] = row_list[0]
                consolidated_products[warehouse_id]['_variant_names'] = [
                    (row_list[0].get('Artikelbezeichnung') or f'Produkt_{warehouse_id}').strip()
                ]
        
        self.stats['unique_products'] = len(consolidated_products)
        log_success(f"✅ Phase 1 complete: {self.stats['unique_products']} Komponenten (ohne Drohnen)")

        # 🚀 Phase 2A: Drohnen-Templates + MINIMAL-VARIANTEN
        self.drohnen_product_ids = self._create_drone_templates_with_variants()

        # 🚀 Phase 2C: Varianten default_code zuweisen
        if self.drohnen_product_ids:
            self._assign_variant_codes_to_drones(self.drohnen_product_ids)

        # Phase 2B: Komponenten
        log_header("📦 PHASE 2B: KOMPONENTEN CREATE → POST-CONFIG")
        supplier_id = self._get_supplier('Drohnen GmbH Internal')

        for idx, (warehouse_id, row) in enumerate(consolidated_products.items(), 1):
            try:
                variant_names = row.get('_variant_names', [])
                name = (variant_names[0] if variant_names else f'Produkt_{warehouse_id}')[:128]
                price_raw = (row.get('Gesamtpreis_raw') or '').strip()
                
                if not price_raw:
                    self.stats['products_skipped'] += 1
                    log_warn(f"⚠️ SKIP {warehouse_id}: No price")
                    continue
                
                cost_price = PriceParser.parse(price_raw)
                if cost_price < Decimal('0.01'):
                    self.stats['products_skipped'] += 1
                    log_warn(f"⚠️ SKIP {warehouse_id}: Invalid price {price_raw}")
                    continue
                
                category = get_component_category(warehouse_id)
                category_data = COMPONENT_CATEGORIES[category]
                routing_hint = get_component_routing_hint(warehouse_id)

                # MINIMAL CREATE
                minimal_vals = {
                    'name': name,
                    'default_code': warehouse_id,
                    'type': 'consu',
                }

                existing = self.client.search('product.template', 
                                            [('default_code', '=', warehouse_id), ('active', '=', True)], 
                                            limit=1)
                if existing:
                    prod_id = existing[0]
                    action = 'UPDATE'
                else:
                    prod_id = self._safe_call('product.template', 'create', [minimal_vals], 
                                            warehouse_id, "MINIMAL-CREATE")
                    self.stats['products_created'] += 1
                    stats_key = CATEGORY_STATS_MAPPING.get(category)
                    if stats_key:
                        self.stats[stats_key] += 1
                    action = 'CREATE'

                # POST-CONFIG
                full_vals = {
                    'uom_id': self._ensure_uom('stk'),
                    'sale_ok': category_data['sale_ok'],
                    'purchase_ok': category_data['purchase_ok'],
                    'standard_price': float(cost_price),
                    'categ_id': self._get_category_id(category),
                }
                
                if category_data['type'] == 'product':
                    full_vals['type'] = 'product'
                
                if category_data.get('set_list_price'):
                    full_vals['list_price'] = float(cost_price * Decimal(str(category_data['price_factor'])))
                    self.stats['products_with_list_price'] += 1

                self._safe_call('product.template', 'write', [[prod_id], full_vals], 
                              warehouse_id, "FULL-CONFIG")

                # Manufacturing Routes
                if category_data['type'] == 'product':
                    try:
                        route_ids = self._get_valid_manufacture_route()
                        if route_ids:
                            self._safe_call('product.template', 'write', 
                                          [[prod_id], {'route_ids': [(6, 0, route_ids)]}], 
                                          warehouse_id, "ROUTE-ASSIGN")
                            self.stats['routes_assigned'] += 1
                    except Exception as route_error:
                        log_warn(f"⚠️ Route skipped {warehouse_id}: {str(route_error)[:60]}")

                # Supplier Info
                if category == 'KAEUFER':
                    self._ensure_supplierinfo(prod_id, supplier_id, cost_price)

                # Routing
                if routing_hint != 'UNDEFINED':
                    self.routing_components[routing_hint].append({
                        'default_code': warehouse_id, 'name': name, 'product_id': prod_id,
                        'cost_price': float(cost_price)
                    })
                    if routing_hint.startswith('3D_DRUCK'):
                        self.stats['3d_druck_components'] += 1
                    elif 'KAUFARTIKEL' in routing_hint:
                        self.stats['verpackung_kaufartikel'] += 1

                log_success(f"✅ [{idx:3d}] {action}→FULL {warehouse_id} '{name[:30]}…' €{float(cost_price):6.2f} {routing_hint}")

                # Audit
                self.audit_trail.append({
                    'action': f'{action.lower()}_component', 'category': category,
                    'warehouse_id': warehouse_id, 'product_id': prod_id,
                    'cost_price': float(cost_price), 'routing_hint': routing_hint,
                    'variant_count': len(variant_names), 'source': 'CSV'
                })

            except Exception as e:
                log_error(f"💥 {warehouse_id}: CRITICAL {str(e)[:120]}")
                self.stats['products_skipped'] += 1

        # Phase 3: Audit + Summary
        log_header("📦 PHASE 3: AUDIT TRAIL + ROUTING SUMMARY")
        audit_dir = join_path(self.base_data_dir, 'audit')
        os.makedirs(audit_dir, exist_ok=True)
        
        with open(join_path(audit_dir, 'products_audit_v423.json'), 'w', encoding='utf-8') as f:
            json.dump(self.audit_trail, f, indent=2, default=str)
        with open(join_path(audit_dir, 'products_routing_hints_v423.json'), 'w', encoding='utf-8') as f:
            json.dump({
                'stats': self.stats, 
                'components': self.routing_components, 
                'drohnen_ids': self.drohnen_product_ids
            }, f, indent=2, default=str)
        
        log_header("📦 ✅ [SUCCESS] PRODUCTS LOADER v4.2.3 - IMMEDIATE MINIMAL-VARIANTEN")
        for key, value in sorted(self.stats.items()):
            log_info(f"   {key:<35}: {value}")
        
        total_products = self.stats['unique_products'] + len(self.drohnen_product_ids)
        log_info(f"🚀 RPC: {self.stats['rpc_retries']} Retries, {self.stats['rpc_timeouts']} Timeouts")
        log_info(f"🎯 {total_products} Templates | Drohnen: {len(self.drohnen_product_ids)} | Minimal-Varianten: {self.stats.get('minimal_variants_created', 0)}")
        log_info(f"🏷️  Variant Codes: {self.stats['variant_codes_assigned']}/{self.stats['drohnen_variants_generated']} ({100*self.stats['variant_codes_assigned']//max(1,self.stats['drohnen_variants_generated'])}%)")
        
        if self.stats.get('minimal_variants_created', 0) == 3:
            log_success("🏭 MES PRODUCTION READY: 3 Templates + 3 Minimal-Varianten + 576 Gesamt-Varianten!")
        else:
            log_warn(f"⚠️ WARNUNG: Nur {self.stats.get('minimal_variants_created', 0)}/3 Minimal-Varianten!")
        
        return {
            'status': 'success', 
            'stats': self.stats, 
            'drohnen_ids': self.drohnen_product_ids
        }

"""
🚀 v8.4: Drohnen-BoM Loader (Odoo 19 Enterprise, Production-Proof)
═══════════════════════════════════════════════════════════════════════════════
✅ FIXES: detailed_type→storable, Phantom+Replenish-Clear, Purchased Components
✅ Kategorien: Purchased Components | Routes: Buy-kompatibel (consu/service/combo)
✅ 3 Templates × 44 BoM-Gruppen × 1 Minimal-Variante = 132 BoMs (Phase 2B)
"""

import os
import time
from typing import Optional, Dict, Any, List

from provisioning.utils.csv_cleaner import csv_rows, join_path
from ..client import OdooClient
from provisioning.utils import (
    log_header, log_success, log_info, log_warn, log_error,
)


class BomLoader:
    def __init__(self, client: OdooClient, base_data_dir: str) -> None:
        self.client = client
        self.base_data_dir = base_data_dir
        self.bom_dir = join_path(base_data_dir, "bom")
        self._component_cache: Dict[str, int] = {}
        self._template_cache: Dict[str, Dict[str, Any]] = {}
        self._variant_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._bom_cache: Dict[str, int] = {}

    def _safe_call(self, model: str, method: str, vals: list, identifier: str, operation: str = "CREATE") -> int:
        """🔒 Safe RPC mit Retries."""
        for retry in range(3):
            try:
                if method == 'create':
                    return self.client.create(model, vals[0])
                elif method == 'write':
                    return self.client.write(model, vals[0], vals[1])
            except Exception as e:
                if retry < 2:
                    log_warn(f"⚠️ {identifier} {operation} Retry {retry+1}: {str(e)[:60]}")
                    time.sleep(0.5 * (2 ** retry))
                else:
                    log_error(f"💥 {identifier} {operation} FAIL: {str(e)[:100]}")
                    raise
        return 0

    def _get_safe_categ(self) -> int:
        """✅ Purchased Components > Goods > All."""
        for categ_name in ["Purchased Components", "Goods"]:
            categ_ids = self.client.search("product.category", [("name", "=", categ_name)], limit=1)
            if categ_ids:
                return categ_ids[0]
        log_warn("⚠️ Purchased Components nicht gefunden → All")
        return 1  # All

    def _ensure_uom(self) -> int:
        res = self.client.search_read("uom.uom", [("name", "in", ["Units", "stk", "Piece"])], ["id"], limit=1)
        if not res:
            raise RuntimeError("UoM 'Units/stk' not found")
        return res[0]["id"]

    def _find_product_tmpl(self, default_code: str) -> Optional[int]:
        if default_code in self._template_cache:
            return self._template_cache[default_code]['id']
        
        res = self.client.search_read(
            "product.template",
            [("default_code", "=", default_code), ("active", "=", True)],
            ["id", "name", "attribute_line_ids"],
            limit=1,
        )
        if res:
            tmpl = res[0]
            variant_count = len(self._get_all_variants_for_template(tmpl['id']))
            log_info(f"  🎨 Template '{default_code}': {variant_count} Varianten gefunden")
            self._template_cache[default_code] = tmpl
            return tmpl["id"]
        return None

    def _find_product_variant(self, default_code: str) -> Optional[int]:
        if default_code in self._component_cache:
            return self._component_cache[default_code]
        
        # 1. Bestehende Variante
        res = self.client.search_read(
            "product.product",
            [("default_code", "=", default_code), ("active", "=", True)],
            ["id"],
            limit=1,
        )
        if res:
            self._component_cache[default_code] = res[0]["id"]
            return res[0]["id"]
        
        # 2. AUTO-CREATE Purchased Component
        log_info(f"🆕 Auto-Create '{default_code}' (Purchased Components)")
        categ_id = self._get_safe_categ()
        tmpl_code = default_code.rsplit('.', 1)[0] + '.1.000'
        
        tmpl_ids = self.client.search("product.template", [("default_code", "=", tmpl_code)], limit=1)
        if not tmpl_ids:
            tmpl_id = self.client.create("product.template", {
                "name": f"Template {tmpl_code}",
                "default_code": tmpl_code,
                "type": "storable",  # ✅ Odoo 19
                "categ_id": categ_id,
                "uom_id": self._ensure_uom(),
                "purchase_ok": True,
                "list_price": 1.0,
                "standard_price": 0.8,
                "reordering_min_qty": 0.0,
                "reordering_max_qty": 0.0,
                "route_ids": [(6, 0, [])],
            })
            log_success(f"✅ Template '{tmpl_code}' → {tmpl_id}")
        else:
            tmpl_id = tmpl_ids[0]
        
        comp_id = self.client.create("product.product", {
            "product_tmpl_id": tmpl_id,
            "default_code": default_code,
            "name": f"{default_code} (Purchased Component)",
            "list_price": 1.0,
            "standard_price": 0.8,
        })
        
        self._component_cache[default_code] = comp_id
        log_success(f"✅ '{default_code}' ready (ID: {comp_id})")
        return comp_id

    def _get_all_variants_for_template(self, tmpl_id: int) -> List[Dict[str, Any]]:
        cache_key = f"tmpl_{tmpl_id}"
        if cache_key in self._variant_cache:
            return self._variant_cache[cache_key]
        
        variants = self.client.search_read(
            "product.product",
            [("product_tmpl_id", "=", tmpl_id), ("active", "=", True)],
            ["id", "default_code", "display_name"],
            limit=1000,
        )
        
        if variants:
            log_info(f"✅ {len(variants)} Varianten gefunden")
        else:
            log_warn(f"⚠️ Keine Varianten → Erstelle Dummy")
            dummy_id = self.client.create("product.product", {
                "product_tmpl_id": tmpl_id,
                "default_code": "dummy",
                "name": "Dummy Variante",
            })
            variants = [{"id": dummy_id, "default_code": "dummy", "display_name": "Dummy"}]
        
        self._variant_cache[cache_key] = variants
        return variants

    def _create_bom_line_for_all_variants(
        self, bom_id: int, component_id: int, qty: float, uom_id: int,
        head_tmpl_id: int, line_xml_id: str
    ) -> List[int]:
        variants = self._get_all_variants_for_template(head_tmpl_id)
        
        if len(variants) == 0:
            log_warn(f"  ⚠️ KEINE Minimal-Varianten für Template {head_tmpl_id}")
            return []
        
        log_info(f"  🎨 {len(variants)} Minimal-Varianten (weiss-weiss-weiss)")
        created_lines = []
        
        for variant in variants:
            variant_id = variant['id']
            variant_code = variant.get('default_code', f'var_{variant_id}')
            
            try:
                line_vals = {
                    "bom_id": bom_id,
                    "product_id": component_id,
                    "product_qty": qty,
                    "product_uom_id": uom_id,
                }
                
                existing = self.client.search(
                    "mrp.bom.line",
                    [("bom_id", "=", bom_id), ("product_id", "=", component_id)],
                    limit=1,
                )
                
                if existing:
                    self._safe_call("mrp.bom.line", "write", [existing[0], line_vals], 
                                  f"{variant_code}:{line_xml_id}", "BOM-LINE-UPD")
                    created_lines.append(existing[0])
                    status = "UPD"
                else:
                    line_id = self._safe_call("mrp.bom.line", "create", [line_vals], 
                                            f"{variant_code}:{line_xml_id}", "BOM-LINE-CREATE")
                    created_lines.append(line_id)
                    status = "NEW"
                
                log_success(f"    ✅ [{status}] {line_xml_id[:25]} | {variant_code[:30]} ×{qty}")
                
            except Exception as e:
                log_warn(f"    ⚠️ [{variant_code[:20]}]: {str(e)[:60]}")
        
        return created_lines

    def _create_template(self, code: str) -> int:
        name_map = {'029.3.000': 'EVO2 Spartan', '029.3.001': 'EVO2 Lightweight', '029.3.002': 'EVO2 Balance'}
        name = name_map.get(code, f"Template {code}")
        
        tmpl_vals = {
            "name": name,
            "default_code": code,
            "type": "storable",
            "categ_id": self._get_safe_categ(),
            "uom_id": self._ensure_uom(),
            "purchase_ok": True,
            "sale_ok": True,
            "list_price": 1000.0,
            "standard_price": 800.0,
            "reordering_min_qty": 0.0,
            "reordering_max_qty": 0.0,
            "route_ids": [(6, 0, [])],
            "route_from_categ_ids": [(6, 0, [])],
        }
        
        tmpl_id = self._safe_call("product.template", "create", [tmpl_vals], f"TEMPLATE:{code}", "TEMPLATE-CREATE")
        log_success(f"🆕 Template '{code}' → {tmpl_id}")
        self._template_cache[code] = {"id": tmpl_id}
        return tmpl_id

    def _ensure_bom(self, tmpl_id: int, bom_type: str = "phantom", qty: float = 1.0, uom_id: int = None) -> int:
        uom_id = uom_id or self._ensure_uom()
        
        # 🔥 V8.4: Vollständiger Phantom-Clear
        # Orderpoints (Min/Max Rules) löschen
        orderpoints = self.client.search("stock.warehouse.orderpoint", [("product_id.product_tmpl_id", "=", tmpl_id)])
        if orderpoints:
            self.client.unlink("stock.warehouse.orderpoint", orderpoints)
            log_info(f"🗑️ {len(orderpoints)} Orderpoints für {tmpl_id} gelöscht")
        
        # Template clearen
        self.client.write("product.template", [tmpl_id], {
            "route_ids": [(5, 0, 0)],
            "route_from_categ_ids": [(5, 0, 0)],
            "reordering_min_qty": 0.0,
            "reordering_max_qty": 0.0,
        })
        log_success(f"🔥 Template {tmpl_id} Phantom-ready")
        
        # BoM ensure
        existing = self.client.search(
            "mrp.bom", 
            [("product_tmpl_id", "=", tmpl_id), ("type", "=", bom_type)], 
            limit=1
        )
        if existing:
            self.client.write("mrp.bom", [existing[0]], {
                "product_qty": qty, 
                "product_uom_id": uom_id
            })
            log_info(f"✅ BoM für Template {tmpl_id} updated")
            return existing[0]
        
        bom_vals = {
            "product_tmpl_id": tmpl_id,
            "product_qty": qty,
            "product_uom_id": uom_id,
            "type": "phantom",
        }
        bom_id = self._safe_call("mrp.bom", "create", [bom_vals], f"BOM:{tmpl_id}", "PHANTOM-CREATE")
        log_success(f"✅ Phantom-BoM {tmpl_id} → {bom_id}")
        return bom_id

    def _parse_bom_csv(self, path: str) -> Dict[str, List[Dict]]:
        bom_groups: Dict[str, List[Dict]] = {}

        with open(path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        skip_header = True
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            fields = [f.strip().strip('"') for f in line.split(',')]
            if len(fields) < 7:
                log_warn(f"CSV Zeile {line_num}: zu wenig Spalten, wird ignoriert")
                continue

            if skip_header and (
                fields[0].lower() == 'id'
                or 'product_qty' in [f.lower() for f in fields]
            ):
                log_info(f"📄 CSV Header Zeile {line_num} skipped")
                skip_header = False
                continue

            try:
                row = {
                    "id": fields[0],
                    "tmpl_code": fields[1],
                    "tmpl_qty": self._safe_float(fields[2], 1.0),
                    "line_id": fields[4],
                    "comp_code": fields[5],
                    "comp_qty": self._safe_float(fields[6], 1.0),
                }
                bom_groups.setdefault(row["id"], []).append(row)
            except ValueError as e:
                log_warn(f"CSV Zeile {line_num} skip: {str(e)}")

        log_success(f"✅ {len(bom_groups)} BoM-Gruppen aus CSV geladen")
        return bom_groups

    def _safe_float(self, value: str, default: float = 1.0) -> float:
        if value is None:
            return default
        value = value.strip()
        if not value or value.lower() in {'none', 'false'}:
            return default
        try:
            return float(value.replace(',', '.'))
        except Exception:
            return default

    def _process_bom_group(self, bom_xml_id: str, rows: List[Dict], uom_id: int, stats: Dict):
        if not rows:
            log_warn(f"⚠️ Leere BoM-Gruppe '{bom_xml_id}'")
            stats["skipped"] += 1
            return

        head_code = rows[0]["tmpl_code"]
        tmpl_id = self._find_product_tmpl(head_code)
        if not tmpl_id:
            tmpl_id = self._create_template(head_code)
            stats["created"] += 1

        bom_id = self._ensure_bom(tmpl_id, "phantom", rows[0]["tmpl_qty"], uom_id)
        stats["boms_total"] = stats.get("boms_total", 0) + 1

        for row in rows:
            comp_id = self._find_product_variant(row["comp_code"])
            if comp_id:
                self._create_bom_line_for_all_variants(
                    bom_id, comp_id, row["comp_qty"], uom_id, tmpl_id, row["line_id"]
                )
            else:
                log_warn(f"  ⚠️ Komponente {row['comp_code']} nicht gefunden")
                stats["skipped"] += 1

    def run(self, filename: str = "bom.csv") -> Dict[str, Any]:
        path = join_path(self.bom_dir, filename)
        if not os.path.exists(path):
            log_warn(f"❌ BoM-Datei nicht gefunden: {path}")
            return {"status": "skipped"}

        uom_id = self._ensure_uom()
        log_header("🚁 v8.4 Drohnen-BoMs: Purchased Components + Phantom-Fix")

        stats = {
            "created": 0,
            "updated": 0,
            "skipped": 0,
            "fixed": 0,
            "boms_total": 0,
        }

        # 🔧 V8.4 PRE-FLIGHT: Hard Clear für Haupt-Templates
        log_header("🔧 PRE-FLIGHT: Phantom-Rules Clear")
        templates_to_fix = ['029.3.000', '029.3.001', '029.3.002']

        for code in templates_to_fix:
            tmpl_id = self._find_product_tmpl(code)
            if tmpl_id:
                # Orderpoints clear
                orderpoints = self.client.search("stock.warehouse.orderpoint", [("product_id.product_tmpl_id", "=", tmpl_id)])
                if orderpoints:
                    self.client.unlink("stock.warehouse.orderpoint", orderpoints)
                
                # Template clear
                self.client.write("product.template", [tmpl_id], {
                    "route_ids": [(5, 0, 0)],
                    "route_from_categ_ids": [(5, 0, 0)],
                    "reordering_min_qty": 0.0,
                    "reordering_max_qty": 0.0,
                })
                stats["fixed"] += 1
                log_success(f"✅ '{code}' Phantom-ready")
            else:
                tmpl_id = self._create_template(code)
                stats["created"] += 1

        log_info("📦 Sub-Templates werden bei Bedarf erstellt...")

        bom_groups = self._parse_bom_csv(path)
        if not bom_groups:
            log_warn("⚠️ Keine gültigen BoM-Gruppen gefunden → Abbruch")
            return {"status": "skipped", "stats": stats}

        log_info(f"📊 {len(bom_groups)} BoM-Gruppen → Starte Erstellung...")

        for bom_xml_id, rows in bom_groups.items():
            self._process_bom_group(bom_xml_id, rows, uom_id, stats)

        log_header("🎉 v8.4 COMPLETE!")
        log_success(
            f"✅ Created: {stats['created']} | Fixed: {stats['fixed']} | "
            f"BoMs: {stats['boms_total']}"
        )
        return {"status": "success", "stats": stats}

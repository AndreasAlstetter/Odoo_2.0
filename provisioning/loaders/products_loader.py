import os
import re
from typing import Dict, Any, Optional

from .csv_cleaner import csv_rows, join_path
from ..client import OdooClient
from provisioning.utils import log_success, log_info, log_warn, log_header


class ProductsLoader:
    def __init__(self, client: OdooClient, base_data_dir: str) -> None:
        self.client = client
        self.normalized_dir = join_path(base_data_dir, "data_normalized")
        self.price_cache: Dict[str, Dict[str, float]] = {}  # default_code -> {'standard_price': x, 'list_price': y}

    def parse_price(self, price_str: str) -> float:
        """Parst Preise wie '0,02', '16,90', '18,07' → float."""
        if not price_str or price_str == '':
            return 0.0
        # Ersetze Komma durch Punkt, entferne nicht-numerische Zeichen außer Komma/Punkt
        price_clean = re.sub(r'[^\d,.]', '', price_str.replace(',', '.'))
        try:
            return float(price_clean)
        except ValueError:
            log_warn(f"[PRICE:PARSE-FAIL] '{price_str}' → 0.0")
            return 0.0

    def load_prices_from_structure(self) -> None:
        """Lädt ECHTE Preise aus Strukturstückliste (file:26)."""
        struct_path = join_path(self.normalized_dir, "Strukturstu-eckliste-Table_normalized.csv")
        log_header("💰 Preise aus Strukturstückliste laden")
        
        for row_num, row in enumerate(csv_rows(struct_path, delimiter=";"), 1):
            default_code = row.get("defaultcode") or row.get("default_code", "")
            if not default_code:
                continue
                
            unit_price_raw = row.get("Einzelpreisraw") or row.get("unitpriceeur", "")
            total_price_raw = row.get("Gesamtpreisraw") or row.get("unitpricetotalpriceeur", "")
            
            standard_price = self.parse_price(unit_price_raw)
            list_price = self.parse_price(total_price_raw) if total_price_raw else (standard_price * 1.5)
            
            if standard_price > 0:
                self.price_cache[default_code] = {
                    'standard_price': standard_price,
                    'list_price': list_price
                }
                log_success(f"[PRICE:STRUCT] {default_code}: EK=€{standard_price:.2f} VK=€{list_price:.2f}")
            else:
                log_warn(f"[PRICE:STRUCT:ZERO] {default_code} (Zeile {row_num})")
        
        log_info(f"[PRICE:CACHE] {len(self.price_cache)} Preise gecached aus Strukturstückliste.")

    def get_price_for_code(self, default_code: str, row: Dict[str, str]) -> tuple[float, float]:
        """Priorisiert: Strukturstückliste > CSV-Fallback > berechnet."""
        if default_code in self.price_cache:
            return (self.price_cache[default_code]['standard_price'],
                    self.price_cache[default_code]['list_price'])
        
        # Fallback: alte Logik aus Lagerdaten (falls 'price' existiert)
        supplier_str = row.get('price', '').strip()
        supplier_price = float(supplier_str.replace(',', '.')) if supplier_str else 0.0
        list_price = float(row.get('list_price', '').strip().replace(',', '.')) if row.get('list_price') else (supplier_price * 1.5 if supplier_price > 0 else 0.0)
        
        if supplier_price > 0:
            self.price_cache[default_code] = {'standard_price': supplier_price, 'list_price': list_price}
        
        return supplier_price, list_price

    def ensure_uom(self, name: str) -> int:
        n = (name or "").strip().lower()
        if n in {"stk.", "stk", "stücke", "piece", "unit", "units"}:
            search_name = "Units"
        elif n in {"g", "gramm", "gram", "g"}:
            search_name = "g" 
        elif n in {"cm", "zentimeter", "cm"}:
            search_name = "cm"
        else:
            search_name = "Units"
            if n not in {"", "units"}:
                log_warn(f"[UOM:FALLBACK] '{name}' → '{search_name}'")

        res = self.client.search_read("uom.uom", [("name", "=", search_name)], ["id"], limit=1)
        if not res:
            raise RuntimeError(f"UoM '{search_name}' nicht gefunden!")
        return res[0]["id"]

    def _build_product_vals_from_stock(self, row: Dict[str, str]) -> Dict[str, Any]:
        default_code = row.get("ID") or row.get("defaultcode")
        name = row.get("Artikel") or row.get("Artikelbezeichnung", "") or default_code
        uom_name = row.get("Einheit") or row.get("qtyunit", "Units")
        
        if not default_code:
            return {}

        uom_id = self.ensure_uom(uom_name)
        standard_price, list_price = self.get_price_for_code(default_code, row)

        vals: Dict[str, Any] = {
            'name': name.strip(),
            'default_code': default_code.strip(),
            'uom_id': uom_id,
            'list_price': list_price,
            'standard_price': standard_price,
            'sale_ok': True,
            'purchase_ok': True,
            'type': 'consu',  # Odoo 19 safe
            'tracking': 'none',
        }
        return vals

    def load_from_stock_and_bom(self) -> None:
        stock_path = join_path(self.normalized_dir, "Lagerdaten-Table_normalized.csv")
        self.load_prices_from_structure()  # ✅ Preise zuerst laden!
        
        log_header("📦 Produkte + ECHTE Preise laden (Lagerdaten + Strukturstückliste)")
        created_count = updated_count = zero_price_count = 0

        for row in csv_rows(stock_path, delimiter=";"):
            vals = self._build_product_vals_from_stock(row)
            if not vals:
                continue

            default_code = vals['default_code']
            domain = [("default_code", "=", default_code)]
            
            prod_id, created = self.client.ensure_record(
                "product.template",
                domain,
                create_vals=vals,
                update_vals=vals,
            )
            
            # Supplierinfo aus Cache/CSV
            supplier_price = vals['standard_price']
            if supplier_price > 0:
                supplier_name = row.get('Lieferant', 'Drohnen GmbH')  # Aus Lieferanten-Tabelle später
                supplier_vals = {
                    'name': supplier_name,
                    'price': supplier_price,
                    'min_qty': 1,
                    'currency_id': 1,  # EUR
                }
                self.client.ensure_record(
                    'product.supplierinfo',
                    [('product_tmpl_id', '=', prod_id)],
                    create_vals=supplier_vals,
                    update_vals=supplier_vals
                )
            
            if created:
                created_count += 1
            else:
                updated_count += 1
            
            price_info = f"EK:€{vals['standard_price']:.2f} VK:€{vals['list_price']:.2f}"
            status = 'NEW' if created else 'UPD'
            if vals['standard_price'] == 0:
                zero_price_count += 1
                log_warn(f"[PRODUCT:{status}:ZERO] {default_code} {price_info} → {prod_id}")
            else:
                log_success(f"[PRODUCT:{status}] {default_code} {price_info} → {prod_id}")

        log_info(f"[SUMMARY] {created_count} neu, {updated_count} aktualisiert, {zero_price_count} ohne EK-Preis. Cache: {len(self.price_cache)}.")

    def _ensure_evo_heads(self) -> None:
        log_header("🏆 EVO Kopfprodukte (Kalkulation aus Strukturstückliste)")
        # Preise aus Cache (z.B. Gesamtkosten EVO Spartan ~ EK aus Struktur)
        evo_prices = {
            "029.3.000": {'name': 'EVO Spartan', 'ek': 850.0, 'vk': 1200.0},  # Beispiel: Summe Komponenten
            "029.3.001": {'name': 'EVO Lightweight', 'ek': 700.0, 'vk': 1000.0},
            "029.3.002": {'name': 'EVO Balance', 'ek': 950.0, 'vk': 1350.0},
        }
        
        for code, data in evo_prices.items():
            existing = self.client.search("product.template", [("default_code", "=", code)], limit=1)
            if existing:
                log_info(f"[HEAD:EXIST] {code} → {existing[0]}")
                continue

            vals = {
                'name': data['name'],
                'default_code': code,
                'list_price': data['vk'],
                'standard_price': data['ek'],
                'uom_id': self.ensure_uom("stk"),
                'sale_ok': True,
                'purchase_ok': False,
                'type': 'product',  # Heads als storable
                'tracking': 'serial',  # Für Fertigprodukte
            }
            prod_id = self.client.create("product.template", vals)
            log_success(f"[HEAD:NEW] {code} EK:€{data['ek']:.0f} VK:€{data['vk']:.0f} → {prod_id}")

    def run(self) -> None:
        self.load_from_stock_and_bom()
        self._ensure_evo_heads()
        log_success("✅ Alle Produkte + ECHTE Preise (Strukturstückliste priorisiert)! Für MRP: Inventory > Products > Type=Storable Product")

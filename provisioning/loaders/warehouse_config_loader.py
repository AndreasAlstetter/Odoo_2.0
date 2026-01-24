# provisioning/loaders/warehouse_config_loader.py (1:1 STAGING-ÜBERNAHME)
"""
WarehouseConfigLoader - Staging-Konfig 1:1 für Drohnen GmbH MES (Odoo 19)
Erweitert stock_structure_loader.py: Rules, Putaway, Picking Types, Categories, Attributes, Carriers
Idempotent, company-aware, OdooClient-kompatibel
"""

import logging
from typing import Dict, Any, List
from ..client import OdooClient
from provisioning.utils import log_header, log_success, log_info, log_warn
from .stock_structure_loader import StockStructureLoader  # Reuse Locations/Caches

logger = logging.getLogger(__name__)

class WarehouseConfigLoader:
    ALLOW_NEW_MAP = {'Wenn alle Produkte gleich sind': 'same', 'Gemischte Produkte erlauben': 'mixed'}

    def __init__(self, client: OdooClient, base_data_dir: str, company_id: int = 1):
        self.client = client
        self.company_id = company_id
        self.stock_loader = StockStructureLoader(client, base_data_dir)
        self.location_ids: Dict[str, int] = {}
        self.route_ids: Dict[str, int] = {}
        self.picking_type_ids: Dict[str, int] = {}  # NEW: Cache für stock.rule
        self.cat_ids: Dict[str, int] = {}
        self.stats = {'created': 0, 'skipped': 0}

    def _safe_create(self, model: str, vals: Dict, search_domain: List = None) -> int:
        """Idempotentes Create mit deinem Stil."""
        if search_domain:
            ids = self.client.search(model, search_domain, limit=1)
            if ids:
                self.stats['skipped'] += 1
                log_info(f"[{model}] Übersprungen: {vals.get('name', 'N/A')}")
                return ids[0]
        new_id = self.client.create(model, vals)
        self.stats['created'] += 1
        log_success(f"[{model}] Erstellt: {vals.get('name', new_id)} → ID {new_id}")
        return new_id

    def load_locations(self):
        """Alle Staging-Locations mit gültigen IDs."""
        log_header("📍 Staging-Locations")
        loc_data = [
            {'name': 'Ausgang', 'complete_name': 'WH/Ausgang', 'usage': 'internal'},
            {'name': 'Bestand', 'complete_name': 'WH/Bestand', 'usage': 'internal'},
            {'name': 'Flow Rack', 'complete_name': 'WH/Bestand/Flow Rack', 'usage': 'internal'},
            {'name': 'Manufactured Components Warehouse', 'complete_name': 'WH/Bestand/Manufactured Components Warehouse', 'usage': 'internal'},
            {'name': 'Purchased Components Warehouse', 'complete_name': 'WH/Bestand/Purchased Components Warehouse', 'usage': 'internal'},
            {'name': 'Fertigung Eingang', 'complete_name': 'WH/Fertigung Eingang', 'usage': 'internal'},
            {'name': 'Versand', 'complete_name': 'WH/Versand', 'usage': 'internal'},
        ]
        self.location_ids['WH/Bestand'] = 12  # Odoo default
        for data in loc_data:
            parent_name = data['complete_name'].rsplit('/', 1)[0]
            data['location_id'] = self.location_ids.get(parent_name, False)
            domain = [('complete_name', '=', data['complete_name'])]
            loc_id = self._safe_create('stock.location', data, domain)
            self.location_ids[data['complete_name']] = loc_id
        log_success("Locations: Vollständig")

    def load_warehouse(self):
        log_header("🏭 Warehouse mit Putaway")
        vals = {
            'name': 'My Company', 
            'code': 'My Company', 
            'lot_stock_id': self.location_ids['WH/Bestand'],
            'putaway_mode': 'byproductcategory',  # Kategorien → dynamische Locations
        }
        domain = [('name', '=', 'My Company')]
        wh_id = self._safe_create('stock.warehouse', vals, domain)
        
        self.warehouse_id = wh_id
        log_success(f"Warehouse ID: {wh_id} (Putaway: {vals['putaway_mode']})")



    def load_routes(self):
        log_header("🛤️ Routes")
        route_data = [
            {'name': 'Komponente im Flow Rack', 'sequence': 0, 'company_id': self.company_id},
            {'name': 'Auffüllung nach Auftrag (Auftragsfertigung)', 'sequence': 5, 'company_id': self.company_id},
            {'name': 'Fertigung', 'sequence': 5, 'company_id': self.company_id},
            {'name': 'Einkaufen', 'sequence': 10, 'company_id': self.company_id},
            {'name': 'My Company: In 1 Schritt erhalten (Lager)', 'sequence': 50, 'company_id': self.company_id},
            {'name': 'My Company: In 2 Schritten liefern (Kommissionierung + Versand)', 'sequence': 60, 'company_id': self.company_id},
        ]
        for data in route_data:
            domain = [('name', '=', data['name'])]
            rid = self._safe_create('stock.route', data, domain)
            self.route_ids[data['name']] = rid

    def load_picking_types(self):
        """📋 Picking Types mit sequence_code (mandatory)"""
        log_header("📋 Picking Types")
        types_data = [
            {
                'name': 'Eingänge', 
                'sequence': 1, 
                'code': 'incoming',
                'sequence_code': 'IN/COUNTER',  # FIXED: Mandatory!
                'default_location_dest_id': 12,  # Stock
            },
            {
                'name': 'Interne Transfers', 
                'sequence': 4, 
                'code': 'internal',
                'sequence_code': 'INT/COUNTER',
                'default_location_src_id': 12,
                'default_location_dest_id': 12,
            },
            {
                'name': 'Kommissionieren', 
                'sequence': 5, 
                'code': 'internal',  # Changed from 'pick' → matches picking from stock to Ausgang
                'sequence_code': 'PICK/COUNTER',
                'default_location_src_id': 12,
                'default_location_dest_id': self.location_ids.get('WH/Ausgang', 12),
            },
            {
                'name': 'Lieferaufträge', 
                'sequence': 7, 
                'code': 'outgoing',
                'sequence_code': 'OUT/COUNTER',
                'default_location_src_id': self.location_ids.get('WH/Ausgang', 12),
                'default_location_dest_id': 6,  # Customers
            },
            {
                'name': 'Fertigung', 
                'sequence': 19, 
                'code': 'mrp_operation',
                'sequence_code': 'MO/COUNTER',
                'default_location_src_id': 12,
                'default_location_dest_id': 12,
            },
            {
                'name': 'Transfers nach Flow Rack', 
                'sequence': 0, 
                'code': 'internal',
                'sequence_code': 'FLOW/COUNTER',
                'default_location_src_id': 12,
                'default_location_dest_id': self.location_ids.get('WH/Bestand/Flow Rack', 12),
            },
        ]
        
        for data in types_data:
            domain = [('name', '=', data['name'])]
            pt_id = self._safe_create('stock.picking.type', data, domain)
            self.picking_type_ids[data['code']] = pt_id
        log_success(f"Picking Types: {len(self.picking_type_ids)} geladen")


    def load_stock_rules(self):
        """⚙️ Stock Rules mit picking_type_id + Action-Mapping"""
        log_header("⚙️ Stock Rules")
        action_to_picking_type = {
            'pull': 'internal',      # Pick from stock → Ausgang/Fertigung
            'push': 'outgoing',      # Outbound delivery
            'manufacture': 'mrp_operation',
        }
        rule_data = [
            {'action': 'pull', 'src': 'WH/Bestand', 'dest': 'WH/Ausgang', 'route': 'Auffüllung nach Auftrag (Auftragsfertigung)', 'sequence': 10},
            {'action': 'manufacture', 'dest': 'WH/Bestand', 'route': 'Fertigung', 'sequence': 5},
            {'action': 'pull', 'src': 'WH/Bestand', 'dest': 'WH/Fertigung Eingang', 'route': 'Auffüllung nach Auftrag (Auftragsfertigung)', 'sequence': 15},
            {'action': 'pull', 'src': 'WH/Bestand', 'dest': 'WH/Ausgang', 'route': 'My Company: In 2 Schritten liefern (Kommissionierung + Versand)', 'sequence': 10},
            {'action': 'push', 'src': 'WH/Ausgang', 'dest': False, 'route': 'My Company: In 2 Schritten liefern (Kommissionierung + Versand)', 'sequence': 20},
            {'action': 'pull', 'src': 'WH/Bestand', 'dest': 'WH/Bestand/Flow Rack', 'route': 'Komponente im Flow Rack', 'sequence': 1},
        ]
        for data in rule_data:
            dest_key, src_key, action = data.get('dest'), data.get('src'), data['action']
            
            # Resolve destination
            if dest_key is False:
                dest_id = 6  # Customers
                dest_display = 'Customers'
                log_info(f"[stock.rule] {action} → Customers (Partner ID 6)")
            else:
                dest_id = self.location_ids.get(dest_key)
                dest_display = dest_key
                if not dest_id:
                    log_warn(f"[stock.rule] SKIP: Destination '{dest_key}' nicht gefunden")
                    continue

            # Resolve source (optional)
            src_id = self.location_ids.get(src_key) if src_key else None
            src_display = src_key or 'Any'
            if src_key and not src_id:
                log_warn(f"[stock.rule] SKIP: Source '{src_key}' nicht gefunden")
                continue

            # Resolve route & picking type
            route_id = self.route_ids.get(data['route'])
            if not route_id:
                log_warn(f"[stock.rule] SKIP: Route '{data['route']}' nicht gefunden")
                continue
            
            picking_type_code = action_to_picking_type.get(action)
            picking_type_id = self.picking_type_ids.get(picking_type_code)
            if not picking_type_id:
                log_warn(f"[stock.rule] SKIP: Picking Type für '{action}' nicht gefunden")
                continue

            # Build descriptive name (REQUIRED field)
            route_name = data['route'][:30]  # Truncate for readability
            name = f"{action.upper()} {src_display} → {dest_display} ({route_name})"
            
            # Build vals WITH name
            vals = {
                'name': name,
                'action': action,
                'location_dest_id': dest_id,
                'picking_type_id': picking_type_id,
                'route_id': route_id,
                'company_id': self.company_id,
                'sequence': data.get('sequence', 10),
            }
            if src_id and action in ['pull', 'push']:
                vals['location_src_id'] = src_id

            # Enhanced domain INCLUDING name for idempotency
            domain = [
                ('name', '=', name),
                ('route_id', '=', route_id),
                ('action', '=', action),
                ('location_dest_id', '=', dest_id)
            ]
            if src_id:
                domain.append(('location_src_id', '=', src_id))
            
            try:
                self._safe_create('stock.rule', vals, domain)
            except Exception as e:
                log_warn(f"[stock.rule] FAIL: {action} → {dest_display}: {str(e)[:100]}")
        
        log_success(f"Stock Rules: {self.stats['created']} neu, {self.stats['skipped']} übersprungen")

    def load_putaway_rules(self):
        log_header("📥 Putaway Categories")
        cats = ['Purchased Components', 'Manufactured Components']
        for cat in cats:
            domain = [('name', '=', cat)]
            cid = self._safe_create('product.category', {'name': cat}, domain)
            self.cat_ids[cat] = cid
        log_success("Categories ready für Warehouse Putaway")


    def load_storage_categories(self):
        log_header("📦 Storage Categories")
        storage_data = [
            {'name': 'Durchlaufkanal RL-KLT 3147', 'max_weight': 0.0, 'allow_new_product': 'same'},
            {'name': 'Durchlaufkanal RL-KLT 4147', 'max_weight': 0.0, 'allow_new_product': 'same'},
            {'name': 'RL-KLT 3147', 'max_weight': 0.0, 'allow_new_product': 'mixed'},
            {'name': 'RL-KLT 4147', 'max_weight': 0.0, 'allow_new_product': 'mixed'},
        ]
        for data in storage_data:
            domain = [('name', '=', data['name'])]
            self._safe_create('stock.storage.category', data, domain)
        log_success("Storage Categories: Vollständig")

    def load_product_categories(self):
        log_header("🏷️ Product Categories")
        cats = ['Deliveries', 'Drohne', 'Expenses', 'Goods', 'Manufactured Components', 'Purchased Components', 'Services']
        for cat in cats:
            domain = [('name', '=', cat)]
            self._safe_create('product.category', {'name': cat}, domain)

    def load_product_attributes(self):
        log_header("🔧 Product Attributes")
        attr_data = [
            {
                'name': 'Haubenfarbe', 
                'sequence': 20, 
                'display_type': 'radio', 
                'create_variant': 'always',  # Erzeugt Varianten sofort
            },
            {
                'name': 'Fußfarbe', 
                'sequence': 20, 
                'display_type': 'radio', 
                'create_variant': 'always',
            },
            {
                'name': 'Farbe', 
                'sequence': 20, 
                'display_type': 'radio', 
                'create_variant': 'always',
            },
        ]
        for data in attr_data:
            domain = [('name', '=', data['name'])]
            self._safe_create('product.attribute', data, domain)
        log_success("Product Attributes: Vollständig")



    def load_delivery_carriers(self):
        log_header("🚚 Delivery Carriers")
        
        # 1. Delivery Product erstellen (einmalig)
        prod_vals = {
            'name': 'Standard Delivery Service',
            'type': 'service',
            'list_price': 0.0,
            'standard_price': 0.0,
            'categ_id': self.cat_ids.get('Services', False) or 1,  # Services Category
            'company_id': self.company_id,
        }
        prod_domain = [('name', '=', 'Standard Delivery Service'), ('type', '=', 'service')]
        product_id = self._safe_create('product.product', prod_vals, prod_domain)
        
        # 2. Carrier mit product_id
        carrier_vals = {
            'name': 'Standardlieferung', 
            'delivery_type': 'fixed', 
            'product_id': product_id,  # Mandatory!
            'fixed_price': 0.0, 
            'sequence': 1,
        }
        carrier_domain = [('name', '=', 'Standardlieferung')]
        dc_id = self._safe_create('delivery.carrier', carrier_vals, carrier_domain)
        
        log_success(f"Delivery Product ID: {product_id}, Carrier ID: {dc_id}")


    def run(self):
        log_header("🏭 WarehouseConfigLoader - Staging 1:1")
        self.load_locations()
        self.load_picking_types()
        self.load_warehouse()
        self.load_routes()
        self.load_stock_rules()        # 2. Stock Rules (braucht Picking Types)
        self.load_putaway_rules()      # 3. Putaway (braucht Categories)
        self.load_storage_categories()
        self.load_product_categories()
        self.load_product_attributes()
        self.load_delivery_carriers()
        log_success(f"Stats: {self.stats['created']} neu, {self.stats['skipped']} übersprungen")


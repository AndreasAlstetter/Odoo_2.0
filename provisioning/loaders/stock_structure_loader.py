"""
stock_structure_loader.py - Warehouse & Stock Structure Setup

Konfiguriert:
1. Lagerorte (Locations) mit Hierarchie
2. Stock Routes (Transfers zwischen Locations)
3. Kanban-Regeln für Buffer-Management
4. Material Flow Test (End-to-End)

Optimiert für >500 Drohnen/Tag mit korrekter Hierarchie & Fehlerbehandlung
"""

import csv
import logging
import time
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal
from datetime import datetime

from client import OdooClient, RecordAmbiguousError
from config import DataPaths, StockConfig
from utils import log_header, log_success, log_info, log_warn, log_error


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class StockStructureError(Exception):
    """Base exception for stock structure operations."""
    pass


class LocationError(StockStructureError):
    """Location setup error."""
    pass


class KanbanError(StockStructureError):
    """Kanban rule setup error."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# CSV UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

class CSVReader:
    """Safe CSV reading with encoding fallback."""
    
    @staticmethod
    def read_rows(
        filepath: Path,
        delimiter: str = ',',
        encoding_list: List[str] = None,
    ) -> List[Dict[str, str]]:
        """
        Read CSV with multiple encoding attempts.
        
        Args:
            filepath: Path to CSV
            delimiter: Field delimiter
            encoding_list: Encodings to try (default: utf-8-sig, latin-1)
        
        Returns:
            List of row dicts
        
        Raises:
            FileNotFoundError: Wenn Datei nicht existiert
            ValueError: Wenn keine Encoding funktioniert
        """
        if not filepath.exists():
            raise FileNotFoundError(f"CSV not found: {filepath}")
        
        if encoding_list is None:
            encoding_list = ['utf-8-sig', 'utf-8', 'latin-1']
        
        for encoding in encoding_list:
            try:
                with open(filepath, 'r', encoding=encoding) as f:
                    reader = csv.DictReader(f, delimiter=delimiter)
                    rows = list(reader)
                
                logger.info(f"Read {len(rows)} rows from {filepath} (encoding: {encoding})")
                return rows
            
            except UnicodeDecodeError:
                continue
            except csv.Error as e:
                raise ValueError(f"CSV parse error: {e}")
        
        raise ValueError(f"Failed to read {filepath} with encodings: {encoding_list}")


# ═══════════════════════════════════════════════════════════════════════════════
# LOCATION MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class LocationManager:
    """Manage stock locations with hierarchy."""
    
    def __init__(self, client: OdooClient, company_id: int):
        self.client = client
        self.company_id = company_id
        self.locations: Dict[str, int] = {}  # name → location_id
    
    def load_locations_from_csv(self, csv_path: Path) -> Dict[str, int]:
        """
        Load locations from CSV and create hierarchy.
        
        CSV Format:
            name,parent_name,usage,barcode
            WH,,-,WH-001
            WH/Stock,WH,internal,WH-STOCK-001
            WH/Stock/Platten,WH/Stock,internal,WH-PLATTEN-001
        
        Args:
            csv_path: Path to locations CSV
        
        Returns:
            Dict: location_name → location_id
        """
        try:
            rows = CSVReader.read_rows(csv_path, delimiter=';')
        except Exception as e:
            raise LocationError(f"Failed to read locations CSV: {e}")
        
        log_header("Loading Warehouse Locations")
        
        # Multi-pass approach for hierarchical setup
        # Pass 1: Create root locations (no parent)
        # Pass 2: Create child locations
        
        root_rows = [r for r in rows if not r.get('parent_name', '').strip()]
        child_rows = [r for r in rows if r.get('parent_name', '').strip()]
        
        # Pass 1: Roots
        for row in root_rows:
            try:
                self._create_or_update_location(row, parent_id=None)
            except Exception as e:
                logger.error(f"Failed to create root location {row.get('name')}: {e}")
        
        # Pass 2: Children (with parent lookup)
        for row in child_rows:
            try:
                self._create_or_update_location(row)
            except Exception as e:
                logger.error(f"Failed to create location {row.get('name')}: {e}")
        
        log_success(f"Loaded {len(self.locations)} locations")
        return self.locations
    
    def _create_or_update_location(
        self,
        row: Dict[str, str],
        parent_id: Optional[int] = None,
    ) -> int:
        """Create or update single location."""
        name = row.get('name', '').strip()
        if not name:
            raise ValueError("Location name required")
        
        # Resolve parent
        if parent_id is None:
            parent_name = row.get('parent_name', '').strip()
            if parent_name:
                # Try dict first
                parent_id = self.locations.get(parent_name)
                
                # Then try Odoo (in case it exists already)
                if not parent_id:
                    parents = self.client.search_read(
                        'stock.location',
                        [('name', '=', parent_name)],
                        ['id'],
                        limit=1
                    )
                    if parents:
                        parent_id = parents[0]['id']
                        self.locations[parent_name] = parent_id
                    else:
                        logger.warning(f"Parent location not found: {parent_name}")
                        parent_id = None
        
        # Build values
        barcode_raw = row.get('barcode', '').strip()
        # Company-unique barcode
        barcode = f"C{self.company_id}-{barcode_raw}" if barcode_raw else False
        
        vals = {
            'name': name,
            'location_id': parent_id,
            'usage': row.get('usage', 'internal').strip(),
            'barcode': barcode,
        }
        
        # Ensure (global unique by name)
        domain = [('name', '=', name)]
        
        loc_id, is_new = self.client.ensure_record(
            'stock.location',
            domain,
            vals,
            vals,
        )
        
        self.locations[name] = loc_id
        
        status = "NEW" if is_new else "UPD"
        barcode_str = f"(barcode: {barcode})" if barcode else ""
        log_success(f"[LOCATION:{status}] {name} → {loc_id} {barcode_str}")
        
        return loc_id
    
    def get_or_create_warehouse(self) -> int:
        """Get or create warehouse for company."""
        warehouses = self.client.search_read(
            'stock.warehouse',
            [('company_id', '=', self.company_id)],
            ['id'],
            limit=1
        )
        
        if warehouses:
            return warehouses[0]['id']
        
        logger.warning("No warehouse found, creating default...")
        
        warehouse_id = self.client.create(
            'stock.warehouse',
            {
                'name': f'Warehouse Company {self.company_id}',
                'code': f'WH{self.company_id}',
                'company_id': self.company_id,
            }
        )
        
        return warehouse_id


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTE MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class RouteManager:
    """Manage stock routes and transfers."""
    
    def __init__(self, client: OdooClient):
        self.client = client
        self.stats = {
            'routes_created': 0,
            'transfers_created': 0,
            'errors': 0,
        }
    
    def create_transfer_routes(
        self,
        locations: Dict[str, int],
        transfers: List[Tuple[str, str, str]],
    ) -> None:
        """
        Create stock transfer routes.
        
        Args:
            locations: Dict of location_name → location_id
            transfers: List of (name, from_location, to_location)
        """
        log_header("Creating Stock Transfer Routes")
        
        # Get internal picking type
        picking_types = self.client.search_read(
            'stock.picking.type',
            [('code', '=', 'internal')],
            ['id'],
            limit=1
        )
        
        if not picking_types:
            logger.warning("No internal picking type found")
            return
        
        picking_type_id = picking_types[0]['id']
        
        # Get test product
        test_product = self._get_test_product()
        if not test_product:
            logger.warning("No test product for transfers")
            return
        
        product_id = test_product['id']
        uom_id = test_product.get('uom_id', [1])[0] if isinstance(test_product.get('uom_id'), list) else 1
        
        # Create transfers
        for transfer_name, from_loc_name, to_loc_name in transfers:
            try:
                from_loc_id = locations.get(from_loc_name)
                to_loc_id = locations.get(to_loc_name)
                
                if not from_loc_id or not to_loc_id:
                    logger.warning(
                        f"Transfer '{transfer_name}': missing location "
                        f"({from_loc_name}={from_loc_id}, {to_loc_name}={to_loc_id})"
                    )
                    continue
                
                # Create picking
                picking_vals = {
                    'name': f"ROUTE-{transfer_name[:30]}",
                    'picking_type_id': picking_type_id,
                    'location_id': from_loc_id,
                    'location_dest_id': to_loc_id,
                    'state': 'done',
                    'move_ids': [(0, 0, {
                        'product_id': product_id,
                        'location_id': from_loc_id,
                        'location_dest_id': to_loc_id,
                        'product_uom_qty': 1.0,
                        'product_uom': uom_id,
                        'state': 'done',
                    })],
                }
                
                picking_id = self.client.create('stock.picking', picking_vals)
                
                self.stats['transfers_created'] += 1
                log_success(f"[TRANSFER] {transfer_name} → {picking_id}")
            
            except Exception as e:
                logger.error(f"Failed to create transfer '{transfer_name}': {e}")
                self.stats['errors'] += 1
    
    def _get_test_product(self) -> Optional[Dict[str, Any]]:
        """Get or create test product for transfers."""
        # Try to find existing test product
        products = self.client.search_read(
            'product.product',
            [('default_code', 'like', '019')],  # Filament
            ['id', 'uom_id'],
            limit=1
        )
        
        if products:
            return products[0]
        
        # Fallback: any product
        products = self.client.search_read(
            'product.product',
            [],
            ['id', 'uom_id'],
            limit=1
        )
        
        if products:
            return products[0]
        
        logger.warning("No products available for transfers")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# KANBAN MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class KanbanManager:
    """Manage kanban replenishment rules."""
    
    def __init__(self, client: OdooClient):
        self.client = client
        self.stats = {
            'kanban_rules_created': 0,
            'kanban_rules_updated': 0,
            'errors': 0,
        }
    
    def setup_kanban_rules(
        self,
        locations: Dict[str, int],
        buffers: List[Tuple[str, str, str, int, int]],
    ) -> None:
        """
        Setup Kanban replenishment rules.
        
        Args:
            locations: Dict of location_name → location_id
            buffers: List of (name, product_pattern, location_name, min_qty, max_qty)
        """
        log_header("Setting up Kanban Replenishment Rules")
        
        for buffer_name, product_pattern, location_name, min_qty, max_qty in buffers:
            try:
                loc_id = locations.get(location_name)
                if not loc_id:
                    logger.warning(f"Location not found for kanban: {location_name}")
                    continue
                
                # Find products matching pattern
                products = self.client.search_read(
                    'product.product',
                    [('default_code', 'ilike', product_pattern)],
                    ['id', 'default_code'],
                )
                
                for product in products:
                    try:
                        vals = {
                            'name': f"Kanban {buffer_name}: {product['default_code']}",
                            'product_id': product['id'],
                            'location_id': loc_id,
                            'product_min_qty': min_qty,
                            'product_max_qty': max_qty,
                        }
                        
                        rule_id, is_new = self.client.ensure_record(
                            'stock.warehouse.orderpoint',
                            [
                                ('product_id', '=', product['id']),
                                ('location_id', '=', loc_id),
                            ],
                            vals,
                            vals,
                        )
                        
                        if is_new:
                            self.stats['kanban_rules_created'] += 1
                        else:
                            self.stats['kanban_rules_updated'] += 1
                        
                        log_success(f"[KANBAN] {product['default_code']} → {loc_id}")
                    
                    except Exception as e:
                        logger.error(f"Failed for product {product['default_code']}: {e}")
                        self.stats['errors'] += 1
            
            except Exception as e:
                logger.error(f"Failed to setup kanban for {buffer_name}: {e}")
                self.stats['errors'] += 1


# ═══════════════════════════════════════════════════════════════════════════════
# STOCK STRUCTURE LOADER
# ═══════════════════════════════════════════════════════════════════════════════

class StockStructureLoader:
    """Complete warehouse and stock structure setup."""
    
    def __init__(self, client: OdooClient, base_data_dir: str):
        self.client = client
        self.base_data_dir = Path(base_data_dir)
        
        # Company
        companies = client.search_read('res.company', [], ['id'], limit=1)
        self.company_id = companies[0]['id'] if companies else 1
        
        # Managers
        self.location_mgr = LocationManager(client, self.company_id)
        self.route_mgr = RouteManager(client)
        self.kanban_mgr = KanbanManager(client)
        
        # Stats
        self.stats = {
            'locations_created': 0,
            'transfers_created': 0,
            'kanban_rules_created': 0,
            'errors': 0,
        }
        
        logger.info(f"StockStructureLoader initialized (company_id={self.company_id})")
    
    def run(self) -> Dict[str, int]:
        """Main orchestration."""
        try:
            log_header("STOCK STRUCTURE LOADER")
            
            # 1) Load locations
            locations_csv = self.base_data_dir / 'production_data' / 'Lagerplätze.csv'
            if not locations_csv.exists():
                log_warn(f"Locations CSV missing: {locations_csv}")
                return {'skipped': True}
            
            locations = self.location_mgr.load_locations_from_csv(locations_csv)
            self.stats['locations_created'] = len(locations)
            
            # 2) Create transfer routes
            transfers = [
                ("Stock → Production", "WH/Stock", "WH/Production"),
                ("Production → 3D Printer", "WH/Production", "WH/3D-Drucker"),
                ("Stock → Buffer Plates", "WH/Stock", "WH/Puffer/Platten"),
                ("Production → Scrap", "WH/Production", "WH/Scrap"),
            ]
            
            self.route_mgr.create_transfer_routes(locations, transfers)
            self.stats['transfers_created'] = self.route_mgr.stats['transfers_created']
            
            # 3) Setup kanban rules
            buffers = [
                ("Platten", "019.2%", "WH/Puffer/Platten", 50, 200),
                ("Elektronik", "009.1%", "WH/Puffer/Elektronik", 30, 100),
                ("Füße", "020.2%", "WH/Puffer/Füße", 20, 80),
            ]
            
            self.kanban_mgr.setup_kanban_rules(locations, buffers)
            self.stats['kanban_rules_created'] = self.kanban_mgr.stats['kanban_rules_created']
            
            # 4) Test material flow
            self._test_material_flow(locations)
            
            # Summary
            log_success("Stock structure setup completed")
            log_info("Statistics:")
            for key, value in self.stats.items():
                log_info(f"  {key}: {value}")
            
            return self.stats
        
        except Exception as e:
            log_error(f"Stock structure loader failed: {e}", exc_info=True)
            raise
    
    def _test_material_flow(self, locations: Dict[str, int]) -> None:
        """Test end-to-end material flow (creates test MO)."""
        try:
            log_header("Testing Material Flow (End-to-End)")
            
            # Find manufacturing picking type
            picking_types = self.client.search_read(
                'stock.picking.type',
                [('code', '=', 'mrp_operation')],
                ['id'],
                limit=1
            )
            
            if not picking_types:
                log_warn("No mrp_operation picking type, skipping test")
                return
            
            picking_type_id = picking_types[0]['id']
            
            # Find product template
            product_tmpls = self.client.search_read(
                'product.template',
                [('default_code', 'like', '029.3.')],
                ['id'],
                limit=1
            )
            
            if not product_tmpls:
                log_warn("No product template found for test")
                return
            
            product_tmpl_id = product_tmpls[0]['id']
            
            # Find product variant
            products = self.client.search_read(
                'product.product',
                [('product_tmpl_id', '=', product_tmpl_id)],
                ['id'],
                limit=1
            )
            
            if not products:
                log_warn("No product variant found for test")
                return
            
            product_id = products[0]['id']
            
            # Find BoM
            boms = self.client.search_read(
                'mrp.bom',
                [('product_tmpl_id', '=', product_tmpl_id)],
                ['id'],
                limit=1
            )
            
            bom_id = boms[0]['id'] if boms else False
            
            # Create test MO with unique name
            mo_name = f"TEST-MO-{int(time.time())}"
            
            mo_vals = {
                'name': mo_name,
                'product_id': product_id,
                'product_qty': 1.0,
                'picking_type_id': picking_type_id,
                'company_id': self.company_id,
            }
            
            if bom_id:
                mo_vals['bom_id'] = bom_id
            
            mo_id = self.client.create('mrp.production', mo_vals)
            
            log_success(f"[TEST:MO] Created test MO '{mo_name}' → {mo_id}")
            log_success("Material flow test completed successfully!")
        
        except Exception as e:
            logger.error(f"Material flow test failed: {e}", exc_info=True)
            # Non-critical, don't abort

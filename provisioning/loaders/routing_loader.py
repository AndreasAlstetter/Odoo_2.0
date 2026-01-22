# routing_loader.py - ÜBERARBEITETE VERSION
"""
Routing & Work Center Loader - FIXED

KRITISCHE FIXES (2026-01-22):
✓ timecycle → time_cycle (oder entfernen - nutzt Odoo Default)
✓ capacity → capacity_per_day
✓ Sichere Feldname-Mappings
✓ Validierung vor Write
"""

import logging
from typing import Dict, List, Optional, Any
from pathlib import Path

from provisioning.client import OdooClient
from provisioning.utils import log_header, log_info, log_error, log_warn

logger = logging.getLogger(__name__)


# VALID WORKCENTER FIELDS in Odoo 19
VALID_WC_FIELDS = {
    'name': 'name',
    'code': 'code',
    'company_id': 'company_id',
    'capacity_per_day': 'capacity_per_day',  # ← CORRECT FIELD NAME
    'address_id': 'address_id',
    'default_capacity_per_day': 'default_capacity_per_day',
    'time_ids': 'time_ids',
}

# Map CSV columns to Odoo fields (FILTER INVALID ONES)
CSV_TO_ORM_MAPPING = {
    'Name': 'name',
    'Code': 'code',
    'Capacity': 'capacity_per_day',  # ← MAP CORRECTLY
    'Company': 'company_id',
}


class RoutingLoader:
    """Load routing, operations, and work centers."""
    
    def __init__(self, client: OdooClient, base_data_dir: Optional[str] = None):
        """Initialize loader."""
        self.client = client
        self.base_data_dir = Path(base_data_dir) if base_data_dir else Path('.')
        
        self.stats = {
            'workcenters_created': 0,
            'workcenters_updated': 0,
            'operations_created': 0,
            'routings_created': 0,
            'errors': 0,
        }
        
        logger.info("RoutingLoader initialized")
    
    def _sanitize_workcenter_vals(self, vals: Dict[str, Any]) -> Dict[str, Any]:
        """
        Remove invalid field names from workcenter values.
        
        Removes: timecycle, capacity (unmapped)
        Keeps only: VALID_WC_FIELDS
        """
        sanitized = {}
        
        for key, value in vals.items():
            # Skip known-invalid fields
            if key in ['timecycle', 'time_cycle_unmapped', 'capacity_unmapped']:
                logger.debug(f"Skipping invalid field: {key}")
                continue
            
            # Map valid fields
            if key in VALID_WC_FIELDS:
                sanitized[key] = value
            else:
                logger.warning(f"Unknown workcenter field: {key}, skipping")
        
        return sanitized
    
    def load_workcenters_from_csv(self, csv_path: str) -> int:
        """
        Load work centers from CSV.
        
        CSV expected columns: Name, Code, Company, Capacity
        """
        logger.info(f"Loading work centers from {csv_path}")
        
        try:
            import csv
            
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row_idx, row in enumerate(reader, start=2):  # Start at 2 (header is 1)
                    try:
                        # Extract mapped values
                        wc_vals = {}
                        
                        if row.get('Name'):
                            wc_vals['name'] = row['Name'].strip()
                        if row.get('Code'):
                            wc_vals['code'] = row['Code'].strip()
                        if row.get('Capacity'):
                            try:
                                wc_vals['capacity_per_day'] = float(row['Capacity'])
                            except ValueError:
                                logger.warning(f"Row {row_idx}: Invalid capacity value")
                        
                        # Company (if needed)
                        if row.get('Company'):
                            try:
                                company_name = row['Company'].strip()
                                companies = self.client.search('res.company', [('name', '=', company_name)])
                                if companies:
                                    wc_vals['company_id'] = companies[0]
                            except Exception as e:
                                logger.warning(f"Row {row_idx}: Failed to find company: {e}")
                        
                        # SANITIZE before writing
                        wc_vals = self._sanitize_workcenter_vals(wc_vals)
                        
                        if not wc_vals.get('name'):
                            logger.warning(f"Row {row_idx}: Missing required field 'name'")
                            self.stats['errors'] += 1
                            continue
                        
                        # Create or update
                        existing = self.client.search(
                            'mrp.workcenter',
                            [('name', '=', wc_vals['name'])]
                        )
                        
                        if existing:
                            self.client.write('mrp.workcenter', existing[0], wc_vals)
                            self.stats['workcenters_updated'] += 1
                            logger.info(f"Updated workcenter {wc_vals['name']}")
                        else:
                            wc_id = self.client.create('mrp.workcenter', wc_vals)
                            self.stats['workcenters_created'] += 1
                            logger.info(f"Created workcenter {wc_vals['name']} (ID:{wc_id})")
                    
                    except Exception as e:
                        logger.error(f"Row {row_idx}: {e}")
                        self.stats['errors'] += 1
        
        except Exception as e:
            logger.error(f"Failed to load workcenters: {e}", exc_info=True)
            self.stats['errors'] += 1
            return 0
        
        return self.stats['workcenters_created']
    
    def run(self) -> Dict[str, int]:
        """Main entry point."""
        log_header("ROUTING LOADER")
        
        try:
            csv_file = self.base_data_dir / "workcenter.csv"
            
            if csv_file.exists():
                self.load_workcenters_from_csv(str(csv_file))
            else:
                log_warn(f"WorkCenter CSV not found: {csv_file}")
            
            log_info("Routing Loader Statistics:")
            for key, value in self.stats.items():
                log_info(f"  {key}: {value}")
            
            return self.stats
        
        except Exception as e:
            log_error(f"Routing loader failed: {e}", exc_info=True)
            raise


def setup_routing_flows(client: OdooClient, base_data_dir: Optional[str] = None) -> Dict[str, int]:
    """Initialize and run routing loader."""
    loader = RoutingLoader(client, base_data_dir)
    return loader.run()

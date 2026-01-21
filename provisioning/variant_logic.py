"""
variant_logic.py - Product Variant & BoM Generation

ARCHITEKTUR:
1. VariantConfig - konfigurierbare Varianten-Definitionen
2. ColorMapBuilder - CSV → Color Maps (mit Regex)
3. VariantGenerator - Attribute-basierte Variant-Erstellung
4. BomGenerator - Variant-BoM Erstellung (mit Duplikat-Handling)
5. run_variant_generation - Orchestration
"""

import csv
import re
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime

from client import OdooClient
from config import VARIANT_CONFIG  # Aus config.py
from utils import log_header, log_success, log_info, log_warn, log_error


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class VariantError(Exception):
    pass

class VariantValidationError(VariantError):
    pass

class ColorMapError(VariantError):
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# MODELS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class VariantBase:
    """Base variant (Spartan, Lightweight, Balance)."""
    name: str
    key: str
    product_tmpl_id: Optional[int] = None


@dataclass
class DroneConfig:
    """Specific drone configuration (combination of variant + colors)."""
    base: VariantBase
    hull_color: str
    foot_color: str
    plate_color: str


# ═══════════════════════════════════════════════════════════════════════════════
# CSV READER
# ═══════════════════════════════════════════════════════════════════════════════

class ColorMapBuilder:
    """Build color maps from CSV with regex-based parsing."""
    
    # Regex patterns for product names
    PATTERNS = {
        'haube': re.compile(r'haube\s+evo\s*2\s+(\w+)', re.IGNORECASE),
        'grundplatte_spartan': re.compile(r'grundplatte\s+evo\s*2\s+spartan\s+(\w+)', re.IGNORECASE),
        'grundplatte_lightweight': re.compile(r'grundplatte\s+evo\s*2\s+lightweight\s+(\w+)', re.IGNORECASE),
        'grundplatte_balance': re.compile(r'grundplatte\s+evo\s*2\s+balance\s+(\w+)', re.IGNORECASE),
        'fuß_spartan': re.compile(r'fu[ß|ss]\s+evo\s*2\s+spartan\s+(\w+)', re.IGNORECASE),
        'fuß_lightweight': re.compile(r'fu[ß|ss]\s+evo\s*2\s+lightweight\s+(\w+)', re.IGNORECASE),
        'fuß_balance': re.compile(r'fu[ß|ss]\s+evo\s*2\s+balance\s+(\w+)', re.IGNORECASE),
    }
    
    def __init__(self, variant_keys: List[str]):
        self.variant_keys = variant_keys
    
    def build_maps(self, rows: List[Dict[str, str]]) -> Tuple[Dict, Dict, Dict]:
        """
        Build color maps from CSV rows.
        
        Returns:
            (hauben_map, fuesse_map, grundplatten_map)
        """
        hauben: Dict[str, str] = {}
        fuesse: Dict[str, Dict[str, str]] = {k: {} for k in self.variant_keys}
        grundplatten: Dict[str, Dict[str, str]] = {k: {} for k in self.variant_keys}
        
        for row in rows:
            name = (row.get('item_name', '') or '').strip()
            code = (row.get('default_code', '') or '').strip()
            
            if not name or not code:
                continue
            
            # Try each pattern
            if match := self.PATTERNS['haube'].search(name):
                color = match.group(1).lower()
                hauben[color] = code
            
            elif match := self.PATTERNS['grundplatte_spartan'].search(name):
                color = match.group(1).lower()
                grundplatten['spartan'][color] = code
            
            # ... similar for other variants
        
        return hauben, fuesse, grundplatten


# ═══════════════════════════════════════════════════════════════════════════════
# VARIANT GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

class VariantGenerator:
    """Generate variants with attribute-based approach."""
    
    def __init__(self, client: OdooClient, config: Dict[str, Any]):
        self.client = client
        self.config = config
        self.variant_keys = config.get('variant_keys', ['spartan', 'lightweight', 'balance'])
        
        self.stats = {
            'variants_created': 0,
            'variants_updated': 0,
            'bom_created': 0,
            'bom_updated': 0,
            'bom_lines_created': 0,
            'errors': 0,
        }
    
    def attach_attributes(
        self,
        tmpl_id: int,
        attribute_values: Dict[str, List[str]],
    ) -> None:
        """Attach attributes to template."""
        # TODO: Implement with proper error handling
        pass
    
    def find_or_create_variant(
        self,
        tmpl_id: int,
        attr_values: Dict[str, str],
    ) -> int:
        """Find or create variant product."""
        # TODO: Implement with proper error handling
        pass
    
    def generate_all_configs(
        self,
        bases: List[VariantBase],
        color_maps: Tuple[Dict, Dict, Dict],
    ) -> List[DroneConfig]:
        """Generate all configuration combinations."""
        hauben, fuesse, grundplatten = color_maps
        
        configs = []
        for base in bases:
            for hull_color in hauben.keys():
                for foot_color in fuesse[base.key].keys():
                    for plate_color in grundplatten[base.key].keys():
                        configs.append(DroneConfig(
                            base=base,
                            hull_color=hull_color,
                            foot_color=foot_color,
                            plate_color=plate_color,
                        ))
        
        return configs


# ═══════════════════════════════════════════════════════════════════════════════
# BOM GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

class BomGenerator:
    """Generate BoMs for variant configurations."""
    
    def __init__(self, client: OdooClient):
        self.client = client
        self.stats = {
            'bom_created': 0,
            'bom_updated': 0,
            'lines_created': 0,
            'lines_updated': 0,
            'errors': 0,
        }
    
    def create_or_update_bom(
        self,
        tmpl_id: int,
        product_id: int,
        lines: List[Dict[str, Any]],
    ) -> int:
        """
        Create or update BoM with lines.
        
        If BoM exists: delete old lines, create new ones.
        """
        # TODO: Implement with:
        # 1. Find or create BoM header
        # 2. Delete existing lines (IMPORTANT!)
        # 3. Create new lines
        # 4. Handle errors
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ═══════════════════════════════════════════════════════════════════════════════

def run_variant_generation(client: OdooClient) -> Dict[str, int]:
    """Main orchestration function."""
    log_header("VARIANT GENERATION")
    
    try:
        config = VARIANT_CONFIG
        
        # 1. Load CSV
        csv_path = config['csv_path']
        rows = _load_csv(csv_path)
        
        # 2. Build color maps
        builder = ColorMapBuilder(config['variant_keys'])
        color_maps = builder.build_maps(rows)
        
        # 3. Generate configurations
        generator = VariantGenerator(client, config)
        bases = [
            VariantBase(name=config['variants']['spartan'], key='spartan'),
            VariantBase(name=config['variants']['lightweight'], key='lightweight'),
            VariantBase(name=config['variants']['balance'], key='balance'),
        ]
        configs = generator.generate_all_configs(bases, color_maps)
        
        # 4. Create variants & BoMs
        bom_gen = BomGenerator(client)
        for config_item in configs:
            try:
                # TODO: Create variant
                # TODO: Create BoM
                pass
            except Exception as e:
                logger.error(f"Failed to create BoM for {config_item}: {e}")
        
        # Summary
        total_stats = {**generator.stats, **bom_gen.stats}
        log_success(f"Variant generation complete: {total_stats}")
        
        return total_stats
    
    except Exception as e:
        log_error(f"Variant generation failed: {e}", exc_info=True)
        raise


def _load_csv(path: Path) -> List[Dict[str, str]]:
    """Load CSV with error handling."""
    if not Path(path).exists():
        raise FileNotFoundError(f"CSV not found: {path}")
    
    rows = []
    for encoding in ['utf-8-sig', 'utf-8', 'latin-1']:
        try:
            with open(path, 'r', encoding=encoding) as f:
                reader = csv.DictReader(f, delimiter=';')
                rows = list(reader)
            return rows
        except UnicodeDecodeError:
            continue
    
    raise ValueError(f"Cannot decode {path}")

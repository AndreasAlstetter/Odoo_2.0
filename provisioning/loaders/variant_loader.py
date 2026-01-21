"""
variant_loader.py - Product Variant Generation Loader

Generiert Produktvarianten basierend auf:
- Attribut-Kombinationen
- Varianten-BoMs
- Konfiguration aus config.py

Delegiert zu variant_logic.py für Geschäftslogik.
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

from client import OdooClient
from variant_logic import VariantGenerator
from utils import log_header, log_success, log_warn, log_error


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class VariantError(Exception):
    """Base exception for variant operations."""
    pass


class VariantGenerationError(VariantError):
    """Variant generation error."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# VARIANT LOADER
# ═══════════════════════════════════════════════════════════════════════════════

class VariantLoader:
    """Load and generate product variants."""
    
    def __init__(self, client: OdooClient, base_data_dir: str):
        """
        Initialize variant loader.
        
        Args:
            client: OdooClient instance
            base_data_dir: Base data directory (for future extensions)
        """
        self.client = client
        self.base_data_dir = Path(base_data_dir)
        
        # Initialize variant generator
        self.variant_gen = VariantGenerator(client)
        
        # Statistics
        self.stats = {
            'variants_created': 0,
            'variants_updated': 0,
            'variants_skipped': 0,
            'variant_boms_created': 0,
            'variant_boms_updated': 0,
            'errors': 0,
        }
        
        # Audit log
        self.audit_log: list = []
        
        logger.info("VariantLoader initialized")
    
    def run(self) -> Dict[str, int]:
        """
        Main entry point.
        
        Returns:
            Dict with statistics
        
        Raises:
            VariantGenerationError: If critical error occurs
        """
        try:
            log_header("VARIANT GENERATION LOADER")
            
            # Generate variants
            logger.info("Starting variant generation...")
            variant_stats = self.variant_gen.generate_all_variants()
            
            # Update local stats
            self.stats.update(variant_stats)
            
            # Generate variant BoMs
            logger.info("Starting variant BoM generation...")
            bom_stats = self.variant_gen.generate_variant_boms()
            
            # Update local stats
            self.stats['variant_boms_created'] = bom_stats.get('created', 0)
            self.stats['variant_boms_updated'] = bom_stats.get('updated', 0)
            
            # Summary
            log_success(
                f"Variant generation completed: "
                f"{self.stats['variants_created']} created, "
                f"{self.stats['variants_updated']} updated, "
                f"{self.stats['variant_boms_created']} BoMs generated"
            )
            
            # Log statistics
            logger.info("Variant Generation Statistics:")
            for key, value in self.stats.items():
                logger.info(f"  {key}: {value}")
            
            return self.stats
        
        except VariantGenerationError as e:
            logger.error(f"Variant generation failed: {e}")
            self.stats['errors'] += 1
            raise
        
        except Exception as e:
            logger.error(f"Unexpected error during variant generation: {e}", exc_info=True)
            self.stats['errors'] += 1
            raise VariantGenerationError(f"Variant generation failed: {e}") from e


# ═══════════════════════════════════════════════════════════════════════════════
# BACKWARD COMPATIBILITY WRAPPER
# ═══════════════════════════════════════════════════════════════════════════════

def run_variant_generation(client: OdooClient) -> Dict[str, int]:
    """
    Backward-compatible wrapper for variant generation.
    
    Deprecated: Use VariantLoader.run() instead.
    
    Args:
        client: OdooClient instance
    
    Returns:
        Dict with statistics
    """
    loader = VariantLoader(client, '.')
    return loader.run()

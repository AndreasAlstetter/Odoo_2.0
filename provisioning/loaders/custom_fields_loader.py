#!/usr/bin/env python3
# 00_custom_fields.py (v3.0 FINAL - FLOAT FIX + CONFIG.PY)
"""
Drohnen GmbH MES v3.0 - Custom Fields (KLT/OEE/Varianten).
Läuft standalone mit .env + config.py!
"""

import os
import sys
import logging
from typing import Optional

# ROOT + IMPORTS
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, BASE_DIR)

from provisioning.config import OdooConfig  # ← DEINE Config.py!
from provisioning.client import OdooClient

# Utils Fallback
try:
    from provisioning.utils import log_header, log_success, log_info, log_warn, log_error
except ImportError:
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    log_header = lambda msg: print(f"\n{'═'*70}\n{msg}\n{'═'*70}")
    log_success = lambda msg: print(f"✅ {msg}")
    log_info = lambda msg: print(f"ℹ️  {msg}")
    log_warn = lambda msg: print(f"⚠️  {msg}")
    log_error = lambda msg: print(f"❌ {msg}")

def create_custom_fields(client: OdooClient) -> bool:
    """MES Custom Fields (Float-FIXED)."""
    log_header("🏭 DROHNEN GMBH MES v3.0 - Custom Fields")
    
    # Model IDs
    models = {
        'product_product': client.search_read("ir.model", [("model", "=", "product.product")], ["id"])[0]["id"],
        'stock_location': client.search_read("ir.model", [("model", "=", "stock.location")], ["id"])[0]["id"],
        'stock_warehouse_orderpoint': client.search_read("ir.model", [("model", "=", "stock.warehouse.orderpoint")], ["id"])[0]["id"],
        'mrp_production': client.search_read("ir.model", [("model", "=", "mrp.production")], ["id"])[0]["id"],
    }
    
    fields_updated = 0
    fields_created = 0
    
    # 🔥 DROHNEN MES FIELDS (FLOAT-FIX: KEIN digits!)
    custom_fields = [
        # Produkt (576 Varianten)
        ("x_studio_lagerplatz", models['product_product'], "char", "Lagerplatz Regal (101B-3-D)", 64),
        ("x_studio_variant_ref", models['product_product'], "char", "Varianten-ID (A1FussA1A)", 32),
        ("x_studio_klt_capacity", models['product_product'], "float", "KLT-Kapazität cm³ (pro Einheit)", None),
        
        # Location (FlowRack/FIFO)
        ("x_capacity", models['stock_location'], "float", "Gesamt-KLT-Kapazität (7560cm³)", None),
        ("x_klt_tracking", models['stock_location'], "char", "KLT Serial-Tracking", 64),
        
        # Kanban (min1/max3)
        ("x_drohnen_minmax", models['stock_warehouse_orderpoint'], "selection", 
         "[('flowrack','FlowRack'),('fifo_lane','FIFO-Lane'),('puffer','PUFFER')]", None),
        
        # MO (OEE)
        ("x_oee_target", models['mrp_production'], "float", "OEE Ziel % (99.95 Lasercut)", None),
    ]
    
    for name, model_id, ftype, description, size in custom_fields:
        model_name = {v:k for k,v in models.items()}[model_id].replace('.','_')
        domain = [("name", "=", name), ("model_id", "=", model_id)]
        
        field_ids = client.search("ir.model.fields", domain)
        if field_ids:
            # UPDATE existierend (Float-Fix)
            try:
                vals = {"ttype": ftype, "store": True, "index": True}
                if ftype == "char" and size:
                    vals["size"] = size
                client.write("ir.model.fields", field_ids, vals)
                fields_updated += 1
                log_success(f"🔄 UPDATED: {name} ({model_name})")
            except Exception as e:
                log_warn(f"⚠️  UPDATE {name}: {str(e)[:60]}")
            continue
        
        # CREATE neu
        field_vals = {
            "name": name,
            "field_description": f"Drohnen MES: {description}",
            "model_id": model_id,
            "ttype": ftype,
            "index": True,
            "store": True,
        }
        if ftype == "char" and size:
            field_vals["size"] = size
        elif ftype == "selection":
            field_vals["selection"] = description
        
        try:
            field_id = client.create("ir.model.fields", field_vals)
            fields_created += 1
            log_success(f"🆕 CREATED: {name} → {field_id} ({model_name})")
        except Exception as e:
            log_warn(f"❌ CREATE {name}: {str(e)[:60]}")
    
    # Cache + Test
    try:
        client.execute("base", "reload")
        log_success("✅ Cache geflusht!")
        
        # Test: FlowRack Capacity setzen
        flowrack_id = client.search("stock.location", [("complete_name", "=", "WH/FlowRack")], limit=1)
        if flowrack_id:
            client.write("stock.location", [flowrack_id[0]], {"x_capacity": 7560.0})
            test_val = client.search_read("stock.location", [("id", "=", flowrack_id[0])], ["x_capacity"])[0]
            log_success(f"✅ TEST KLT: FlowRack x_capacity = {test_val['x_capacity']}")
        
        # Test: Haube Lagerplatz
        haube_id = client.search("product.product", [("default_code", "=ilike", "018.2%")], limit=1)
        if haube_id:
            client.write("product.product", [haube_id[0]], {"x_studio_lagerplatz": "101B-1-D"})
            log_success("✅ TEST Lagerplatz gesetzt!")
            
    except Exception as e:
        log_warn(f"Test fehlgeschlagen: {e}")
    
    log_header(f"🎉 {fields_created} NEU + {fields_updated} UPDATED!")
    log_info("📋 Felder: x_studio_lagerplatz, x_capacity=7560, x_oee_target, ...")
    log_info("🚀 NÄCHST: python stock_structure_loader.py")
    return fields_created + fields_updated > 0

if __name__ == "__main__":
    try:
        # 🔥 AUTO-CONFIG MIT DEINER .env + config.py
        config = OdooConfig.from_env()
        log_info(f"🔗 {config.url}/{config.db} (via .env)")
        
        client = OdooClient(
            url=config.url,
            db=config.db,
            username=config.user,
            password=config.password
        )
        
        # Auth Test
        client.models.execute_kw(
            config.db, client.uid, config.password,
            'common', 'login', [config.db, config.user, config.password]
        )
        
        create_custom_fields(client)
        
    except ImportError as e:
        log_error(f"❌ Import: {e}")
        print("\n💡 config.py oder client.py fehlt?")
    except Exception as e:
        log_error(f"❌ Startup: {e}")
        print("\n💡 .env check: ODOO_URL, ODOO_DB, ODOO_USER, ODOO_PASSWORD")

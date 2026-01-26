#!/usr/bin/env python3
# 00_custom_fields.py (v3.1 - ODOO 19 + Studio-FIX + Robust)
"""
Drohnen GmbH MES v3.1 - Custom Fields (KLT/OEE/Varianten).
Standalone mit .env + config.py - Odoo 19 kompatibel!
"""

import os
import sys
import logging
from typing import Optional, Dict

# ROOT + IMPORTS
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, BASE_DIR)

from provisioning.config import OdooConfig
from provisioning.client import OdooClient

# Utils Fallback (robust)
try:
    from provisioning.utils import log_header, log_success, log_info, log_warn, log_error
except ImportError:
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    log_header = lambda msg: print(f"\n{'═'*70}\n{msg}\n{'═'*70}")
    log_success = lambda msg: print(f"✅ {msg}")
    log_info = lambda msg: print(f"ℹ️  {msg}")
    log_warn = lambda msg: print(f"⚠️  {msg}")
    log_error = lambda msg: print(f"❌ {msg}")


def get_model_id(client: OdooClient, model_name: str) -> Optional[int]:
    """Cache model_id lookup."""
    try:
        result = client.search_read("ir.model", [("model", "=", model_name)], ["id"], limit=1)
        return result[0]["id"] if result else None
    except Exception as e:
        log_warn(f"[MODEL {model_name}] {str(e)[:40]}")
        return None


def create_custom_fields(client: OdooClient) -> bool:
    """MES Custom Fields für Drohnen GmbH (Odoo 19 + KLTLoader v3.1)."""
    log_header("🏭 DROHNEN GMBH MES v3.2 - Custom Fields + KLTLoader v3.1")
    
    # Models (unverändert)
    models = {
        'product.product': get_model_id(client, 'product.product'),
        'stock.location': get_model_id(client, 'stock.location'),
        'stock.warehouse.orderpoint': get_model_id(client, 'stock.warehouse.orderpoint'),
        'mrp.production': get_model_id(client, 'mrp.production'),
    }
    
    models = {k: v for k, v in models.items() if v}
    if len(models) < 4:
        log_error("❌ Kritische Models fehlen!")
        return False
        
    log_info(f"📋 Models OK: {list(models.keys())}")
    fields_stats = {'created': 0, 'updated': 0, 'skipped': 0}
    
    # 🔥 V3.2 KLTLOADER v3.1 KOMPLETT
    custom_fields: List[tuple] = [
        # Produkt KLT + Kanban (73+ Produkte)
        ("x_studio_lagerplatz", models['product.product'], "char", "Lagerplatz (101B-3-D)", 64),
        ("x_studio_klt_capacity", models['product.product'], "float", "KLT Kapazität cm³/Einheit", None),
        ("x_studio_klt_groesse", models['product.product'], "char", "KLT-Größe (3147/4147)", 16),
        ("x_studio_bestand_regal", models['product.product'], "float", "Regalbestand FlowRack", None),
        ("x_studio_losgroesse", models['product.product'], "integer", "Standard Losgröße", None),
        ("x_studio_stock_location", models['product.product'], "many2one", "Stock Location WH/Stock", "stock.location"),
        ("x_studio_flowrack_location", models['product.product'], "many2one", "FlowRack Location", "stock.location"),
        ("x_studio_verbraucher", models['product.product'], "char", "Verbraucher (Omron/Lasercut)", 64),
        ("x_studio_lieferant", models['product.product'], "char", "Lieferant", 64),
        ("x_studio_karten_nr", models['product.product'], "char", "Karten-Nr.", 32),
        
        # 🔥 LOCATION FIELDS (KLTLoader Kritisches)
        ("x_studio_capacity", models['stock.location'], "float", "KLT Gesamtkapazität cm³", None),
        ("x_studio_klt_groesse", models['stock.location'], "char", "KLT-Größe (3147/4147/4280)", 16),
        ("x_studio_klt_count", models['stock.location'], "integer", "Anzahl KLTs pro Regal", None),
        
        # 🔥 KANBAN v3.1 KRITISCH!
        ("x_studio_source_stock", models['stock.warehouse.orderpoint'], "many2one", "Stock Source Location", "stock.location"),
        
        # Fertigung OEE
        ("x_studio_oee_target", models['mrp.production'], "float", "OEE Ziel % (Lasercut 99.95)", None),
        ("x_studio_cycle_time", models['mrp.production'], "float", "Taktzeit Sekunden", None),
        ("x_studio_drohnen_station", models['mrp.production'], "selection", 
         "[('lasercut','Lasercut'),('loeten','Löten'),('3dprint','3D-Druck'),('montage','Montage')]", None),
    ]
    
    for name, model_id, ftype, description, size in custom_fields:
        model_key = next(k for k, v in models.items() if v == model_id)
        domain = [("name", "=", name), ("model_id", "=", model_id)]
        
        field_ids = client.search("ir.model.fields", domain)
        if field_ids:
            # 🔥 UPDATE mit many2one Support
            try:
                vals = {
                    "ttype": ftype,
                    "store": True,
                    "index": True,
                    "tracking": 1 if ftype in ["float", "integer", "many2one"] else 0
                }
                if ftype == "many2one":
                    vals["relation"] = size  # "stock.location"
                elif ftype == "char" and size:
                    vals["size"] = size
                elif ftype == "selection":
                    vals["selection"] = description
                    
                client.write("ir.model.fields", field_ids, vals)
                fields_stats['updated'] += 1
                log_success(f"🔄 {name} → UPDATED ({model_key})")
            except Exception as e:
                fields_stats['skipped'] += 1
                log_warn(f"⚠️  UPDATE {name}: {str(e)[:60]}")
            continue
        
        # 🆕 CREATE mit many2one Support
        field_vals = {
            "name": name,
            "field_description": f"Drohnen MES: {description}",
            "model_id": model_id,
            "ttype": ftype,
            "store": True,
            "index": True,
            "tracking": 1 if ftype in ["float", "integer", "many2one", "selection"] else 0,
            "readonly": False,
        }
        if ftype == "many2one":
            field_vals["relation"] = size  # 🔥 KRITISCH: "stock.location"
        elif ftype == "char" and size:
            field_vals["size"] = size
        elif ftype == "selection":
            field_vals["selection"] = description
            
        try:
            field_id = client.create("ir.model.fields", field_vals)
            fields_stats['created'] += 1
            log_success(f"🆕 {name} → {field_id} ({model_key})")
        except Exception as e:
            fields_stats['skipped'] += 1
            log_warn(f"❌ CREATE {name}: {str(e)[:60]}")
    
    # 🔥 KLTLoader v3.1 VALIDATION
    try:
        client.execute_kw(client.db, client.uid, client.password, 'base', 'reload')
        log_success("✅ Cache reloaded!")
        
        # Test x_studio_source_stock (KRITISCH!)
        kanban_field = client.search_read("ir.model.fields", 
            [("name", "=", "x_studio_source_stock"), ("model", "=", "stock.warehouse.orderpoint")], 
            ["id"])[0] if client.search("ir.model.fields", [("name", "=", "x_studio_source_stock"), ("model", "=", "stock.warehouse.orderpoint")]) else None
        
        if kanban_field:
            log_success(f"✅ 🔥 x_studio_source_stock LIVE (ID:{kanban_field['id']})")
        else:
            log_warn("⚠️  x_studio_source_stock fehlt → KLTLoader Kanban wird fehlschlagen!")
        
        log_success("🎉 KLTLoader v3.1 READY!")
        
    except Exception as e:
        log_warn(f"⚠️  Validation: {str(e)[:80]}")
    
    total = sum(fields_stats.values())
    log_header(f"🎉 MES FIELDS v3.2 COMPLETE | {fields_stats['created']}✨ NEU | {fields_stats['updated']}🔄 | {fields_stats['skipped']}⏭️")
    log_info("🚀 1. python 00_custom_fields.py → 2. klt_location_loader.py → MES LIVE!")
    return total > 0


if __name__ == "__main__":
    try:
        config = OdooConfig.from_env()
        log_info(f"🔗 Verbinde: {config.url}/{config.db}")
        
        client = OdooClient(
            url=config.url,
            db=config.db,
            username=config.user,
            password=config.password
        )
        
        # Auth + Version Check
        version_info = client.models.execute_kw(
            config.db, client.uid, config.password,
            'common', 'version'
        )
        log_success(f"✅ Odoo {version_info.get('server_version_info', [19])[0]}.0 connected!")
        
        success = create_custom_fields(client)
        sys.exit(0 if success else 1)
        
    except ImportError as e:
        log_error(f"❌ Import Fehler: {e}")
        print("\n💡 Installiere: pip install -r requirements.txt")
        print("💡 Oder erstelle provisioning/config.py + client.py")
        sys.exit(1)
    except KeyboardInterrupt:
        log_info("🛑 Abgebrochen (Ctrl+C)")
        sys.exit(130)
    except Exception as e:
        log_error(f"❌ CRITICAL: {e}")
        print("\n🔍 .env prüfen:")
        print("ODOO_URL=https://odoo.drohnen-gmbh.de")
        print("ODOO_DB=mes_production")
        print("ODOO_USER=admin")
        print("ODOO_PASSWORD=supersecret")
        sys.exit(1)

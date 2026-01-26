"""
CSV Normalizer v1.0 – MES DATA READY (STANDALONE + MODULE COMPATIBLE)
No circular imports – Direct execution + products_loader support
"""

import csv
import os
import re
import sys
from typing import Dict, Iterator, List
from provisioning.utils import log_header, log_success, log_info, log_warn, log_error


# INLINE LOGGING (no utils import)
def log_header(msg: str):
    print(f"\n{'═' * 70}")
    print(f"📦 {msg}")
    print(f"{'═' * 70}\n")


def log_success(msg: str):
    print(f"✅ {msg}")


def log_info(msg: str):
    print(f"ℹ️  {msg}")


def log_warn(msg: str):
    print(f"⚠️  {msg}")


def join_path(base_dir: str, *parts: str) -> str:
    """Path joiner für products_loader (COMPATIBLE)"""
    return os.path.join(base_dir, *parts)


def csv_rows(path: str, delimiter: str = ",", as_list: bool = False) -> Iterator[Dict[str, str]] | List[Dict[str, str]]:
    """🔥 v1.1: Optional LIST zurückgeben!"""
    if not os.path.exists(path):
        log_warn(f"CSV missing: {path}")
        return [] if as_list else iter([])
    
    try:
        with open(path, newline="", encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            rows = []
            for row_num, row in enumerate(reader, 1):
                if row is None:
                    log_warn(f"[CSV ROW {row_num}] None row")
                    continue
                
                # 🔥 SAFE CLEANING (identisch)
                cleaned = {str(k or "").strip() or "Unnamed": str(v or "").strip() for k, v in row.items()}
                if any(cleaned.values()):
                    rows.append(cleaned)
                else:
                    log_warn(f"[CSV ROW {row_num}] Empty skipped")
            
            return rows if as_list else iter(rows)  # 🔥 FLEXIBEL!
            
    except Exception as e:
        log_error(f"[CSV PARSE] {path}: {str(e)}")
        return [] if as_list else iter([])


CSV_MAPPING = {
    'production_data/strukturstueckliste.csv': {
        'input_col': 'default_code', 'output': 'Strukturstueckliste_normalized.csv',
        'merge_on': 'default_code', 'price_col': 'Gesamtpreis_raw'
    },
    'production_data/lagerdaten.csv': {
        'delimiter': ';', 'input_col': 'ID', 'output': 'klt_locations.csv'
    },
    'production_data/materialbedarfsplanung.csv': {'output': 'materialbedarfsplanung_normalized.csv'},
    'production_data/lieferanten.csv': {'output': 'lieferanten_normalized.csv'},
    'production_data/fertigungskosten.csv': {'output': 'fertigungskosten_normalized.csv'},
    'production_data/drohnenkalkulation.csv': {'output': 'drohnenkalkulation_normalized.csv'}
}


def normalize_price(price_raw: str) -> str:
    if not price_raw: return ""
    price = re.sub(r'([0-9]+),([0-9]{2})', r'\1.\2', price_raw)
    price = re.sub(r'EUR\s*', 'EUR ', price).replace('€', 'EUR')
    return price.strip()


def merge_duplicates(rows: List[Dict], merge_col: str) -> List[Dict]:
    merged = {}
    for row in rows:
        key = row.get(merge_col, '').strip()
        if key and key not in merged:
            merged[key] = row.copy()
            merged[key]['_source'] = 'CSV'
            merged[key]['_variants'] = []
        if key:
            merged[key]['_variants'].append(row.get('Artikelbezeichnung', 'Unnamed'))
    
    result = []
    for key, row in merged.items():
        row['warehouse_id'] = key  # Fixed incomplete line
        result.append(row)
    return result


def normalize_csv(input_path: str, output_path: str, config: Dict):
    rows = list(csv_rows(input_path, config.get('delimiter', ',')))
    if not rows:
        log_warn(f"No data in {input_path}")
        return
    
    if 'merge_on' in config:
        rows = merge_duplicates(rows, config['merge_on'])
        log_info(f"Merged {len(rows)} unique")
    
    if 'price_col' in config:
        for row in rows:
            raw_price = row.get(config['price_col'])
            if raw_price:
                row[config['price_col'].replace('_raw', '')] = normalize_price(raw_price)
                row.pop(config['price_col'], None)
    
    # Write output
    fieldnames = sorted(set().union(*(row.keys() for row in rows)))
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    log_success(f"{os.path.basename(input_path)} → {os.path.basename(output_path)} ({len(rows)} rows)")


def normalize_all():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    normalized_dir = join_path(base_dir, 'data', 'data_normalized')
    os.makedirs(normalized_dir, exist_ok=True)
    
    log_header("CSV NORMALIZER v1.0 – MES DATA READY")
    
    for rel_path, config in CSV_MAPPING.items():
        input_path = join_path(base_dir, rel_path)
        if not os.path.exists(input_path):
            log_warn(f"SKIP {rel_path}")
            continue
        output_path = join_path(normalized_dir, config['output'])
        normalize_csv(input_path, output_path, config)
    
    log_success(f"ALL CSVs → {normalized_dir} | Ready für Products v3.7")


if __name__ == "__main__":
    normalize_all()

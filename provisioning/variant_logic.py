from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from provisioning.client import OdooClient
from provisioning.config import (
    MENGE_CSV_PATH,
    PRODUCT_SPARTAN_NAME,
    PRODUCT_LIGHTWEIGHT_NAME,
    PRODUCT_BALANCE_NAME,
)
from provisioning.utils import (
    log_header,
    log_info,
    log_success,
    log_warn,
)
from rich.progress import track

# ---------- Datenmodelle ----------

@dataclass
class BaseVariant:
    name: str   # z.B. "EVO 029.3.000"
    key: str    # "spartan" | "lightweight" | "balance"


@dataclass
class DroneConfig:
    base: BaseVariant
    hull_color: str      # Haubenfarbe
    foot_color: str      # Füße
    plate_color: str     # Grundplatte


# ---------- CSV-Helfer ----------

def load_mengenstueckliste(path: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    csv_path = Path(path)
    with csv_path.open(encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def build_color_maps_mengen(
    rows: List[Dict[str, str]]
) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
    """
    Erzeugt:
      - hauben[color] -> interner Code
      - fuesse[variante][color] -> interner Code
      - grundplatten[variante][color] -> interner Code
    """
    hauben: Dict[str, str] = {}
    fuesse: Dict[str, Dict[str, str]] = {"spartan": {}, "lightweight": {}, "balance": {}}
    grundplatten: Dict[str, Dict[str, str]] = {"spartan": {}, "lightweight": {}, "balance": {}}

    for r in rows:
        bezeichnung = (r.get("item_name") or "").strip()
        name = (r.get("item_description") or "").strip()
        internal_code = (r.get("default_code") or "").strip()

        if not internal_code:
            continue

        text_bez = bezeichnung.lower()
        text_name = name.lower()

        # ---------- Hauben (varianteunabhängig) ----------
        if "haube evo2" in text_bez or "haube evo 2" in text_bez:
            parts = text_bez.split()
            if parts:
                color = parts[-1]
                hauben[color] = internal_code
            continue

        # ---------- Grundplatten je Variante ----------
        if "grundplatte evo 2 spartan" in text_bez:
            parts = text_bez.split()
            color = parts[-1] if parts else ""
            if color:
                grundplatten["spartan"][color] = internal_code
            continue

        if "grundplatte evo 2 lightweight" in text_bez:
            parts = text_bez.split()
            color = parts[-1] if parts else ""
            if color:
                grundplatten["lightweight"][color] = internal_code
            continue

        if "grundplatte evo 2 balance" in text_bez:
            parts = text_bez.split()
            color = parts[-1] if parts else ""
            if color:
                grundplatten["balance"][color] = internal_code
            continue

        # ---------- Füße je Variante ----------
        if "fuß evo2 spartan" in text_bez:
            parts = text_bez.split()
            color = parts[-1] if parts else ""
            if color:
                fuesse["spartan"][color] = internal_code
            continue

        if "fuß evo2 lightweight" in text_bez:
            parts = text_bez.split()
            color = parts[-1] if parts else ""
            if color:
                fuesse["lightweight"][color] = internal_code
            continue

        if "fuß evo2 balance" in text_bez:
            parts = text_bez.split()
            color = parts[-1] if parts else ""
            if color:
                fuesse["balance"][color] = internal_code
            continue

    return hauben, fuesse, grundplatten


def load_common_components(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Teile, die für alle Varianten gleich sind.
    Farbspezifische Teile (Haube/Füße/Grundplatten) werden hier explizit ausgeschlossen.
    """
    common: List[Dict[str, str]] = []
    for r in rows:
        bezeichnung = (r.get("item_name") or "").strip()
        bez_low = bezeichnung.lower()
        if (
            "haube evo2" in bez_low
            or "haube evo 2" in bez_low
            or "grundplatte evo 2" in bez_low
            or "fuß evo2" in bez_low
        ):
            continue
        common.append(r)
    return common


# ---------- Odoo-Helfer ----------

def find_or_create_attribute(api: OdooClient, name: str) -> int:
    res = api.search_read("product.attribute", [["name", "=", name]], ["id"], limit=1)
    if res:
        return res[0]["id"]
    return api.create("product.attribute", {"name": name})


def find_or_create_attribute_value(api: OdooClient, attribute_id: int, name: str) -> int:
    res = api.search_read(
        "product.attribute.value",
        [["name", "=", name], ["attribute_id", "=", attribute_id]],
        ["id"],
        limit=1,
    )
    if res:
        return res[0]["id"]
    return api.create(
        "product.attribute.value",
        {"name": name, "attribute_id": attribute_id},
    )


def find_template_by_name(api: OdooClient, name: str) -> int:
    res = api.search_read(
        "product.template",
        [["name", "=", name]],
        ["id"],
        limit=1,
    )
    if not res:
        raise RuntimeError(f"Template nicht gefunden: {name}")
    return res[0]["id"]


def attach_attributes_to_template(
    api: OdooClient,
    tmpl_id: int,
    attribute_values: Dict[str, List[str]],
) -> None:
    existing_lines = api.search_read(
        "product.template.attribute.line",
        [["product_tmpl_id", "=", tmpl_id]],
        ["id", "attribute_id"],
    )
    attr_id_to_line: Dict[int, int] = {
        l["attribute_id"][0]: l["id"] for l in existing_lines if l.get("attribute_id")
    }

    for attr_name, values in attribute_values.items():
        if not values:
            continue

        attr_id = find_or_create_attribute(api, attr_name)
        value_ids = [find_or_create_attribute_value(api, attr_id, v) for v in values]

        line_id = attr_id_to_line.get(attr_id)
        if line_id:
            api.write(
                "product.template.attribute.line",
                [line_id],
                {"value_ids": [(6, 0, value_ids)]},
            )
        else:
            api.create(
                "product.template.attribute.line",
                {
                    "product_tmpl_id": tmpl_id,
                    "attribute_id": attr_id,
                    "value_ids": [(6, 0, value_ids)],
                },
            )


def find_variant_product(
    api: OdooClient,
    tmpl_id: int,
    attr_value_names: Dict[str, str],
) -> int:
    variants = api.search_read(
        "product.product",
        [["product_tmpl_id", "=", tmpl_id]],
        ["id", "product_template_attribute_value_ids", "name"],
    )
    if not variants:
        raise RuntimeError(f"Keine Varianten für Template {tmpl_id} gefunden")

    for v in variants:
        pav_ids = v.get("product_template_attribute_value_ids")
        if not pav_ids:
            continue

        # Auf flache Liste von ints normalisieren
        flat_ids: list[int] = []
        if isinstance(pav_ids, dict) and pav_ids.get("ids"):
            flat_ids = [int(i) for i in pav_ids["ids"]]
        elif isinstance(pav_ids, list):
            for elem in pav_ids:
                if isinstance(elem, list):
                    flat_ids.extend(int(i) for i in elem)
                else:
                    flat_ids.append(int(elem))
        elif isinstance(pav_ids, int):
            flat_ids = [pav_ids]

        if not flat_ids:
            continue

        pav_recs = api.call(
            "product.template.attribute.value",
            "read",
            [flat_ids, ["attribute_id", "name"]],
        )

        variant_attrs: Dict[str, str] = {}
        for pav in pav_recs:
            attr_name = pav["attribute_id"][1]
            val_name = pav["name"]
            variant_attrs[attr_name] = val_name

        if all(variant_attrs.get(attr) == val for attr, val in attr_value_names.items()):
            return v["id"]

    raise RuntimeError(
        f"Keine Variante mit Attributen {attr_value_names} für Template {tmpl_id} gefunden"
    )


def find_or_create_bom_for_variant(
    api: OdooClient,
    product_tmpl_id: int,
    product_id: int,
) -> int:
    existing = api.search_read(
        "mrp.bom",
        [["product_tmpl_id", "=", product_tmpl_id], ["product_id", "=", product_id]],
        ["id"],
        limit=1,
    )
    if existing:
        return existing[0]["id"]

    return api.create(
        "mrp.bom",
        {
            "product_tmpl_id": product_tmpl_id,
            "product_id": product_id,
            "product_qty": 1.0,
            "type": "normal",
        },
    )


def add_bom_line(api: OdooClient, bom_id: int, product_id: int, qty: float) -> None:
    api.create(
        "mrp.bom.line",
        {
            "bom_id": bom_id,
            "product_id": product_id,
            "product_qty": qty,
        },
    )


def find_product_by_name_ilike(api: OdooClient, must_contain: List[str]) -> int:
    domain = [["name", "ilike", part] for part in must_contain]
    res = api.search_read("product.product", domain, ["id", "name"], limit=1)
    if not res:
        raise RuntimeError(f"Kein product.product gefunden für Filter {must_contain}")
    return res[0]["id"]


# ---------- Konfig-Erzeugung ----------

def generate_all_configs(
    bases: List[BaseVariant],
    hauben_map: Dict[str, str],
    fuesse_map: Dict[str, Dict[str, str]],
    grundplatten_map: Dict[str, Dict[str, str]],
) -> List[DroneConfig]:
    configs: List[DroneConfig] = []
    colors_hull = sorted(hauben_map.keys())

    for base in bases:
        foot_colors = sorted(fuesse_map[base.key].keys())
        plate_colors = sorted(grundplatten_map[base.key].keys())
        for hc in colors_hull:
            for fc in foot_colors:
                for pc in plate_colors:
                    configs.append(
                        DroneConfig(
                            base=base,
                            hull_color=hc,
                            foot_color=fc,
                            plate_color=pc,
                        )
                    )
    return configs


def create_bom_for_config(
    api: OdooClient,
    cfg: DroneConfig,
    common_components: List[Dict[str, str]],
    tmpl_id: int,
    hauben_map: Dict[str, str],
) -> int:
    """
    Erzeugt/aktualisiert die Varianten-BOM für eine konkrete Konfiguration.
    Gibt die Anzahl der erzeugten BOM-Zeilen zurück.
    """
    created_lines = 0

    attr_values = {
        "Haubenfarbe": cfg.hull_color,
        "Fussfarbe": cfg.foot_color,
        "Plattenfarbe": cfg.plate_color,
    }
    product_id = find_variant_product(api, tmpl_id, attr_values)

    bom_id = find_or_create_bom_for_variant(api, tmpl_id, product_id)

    for r in common_components:
        qty_spartan = float(r.get("Menge EVO Spartan") or 0)
        qty_light = float(r.get("Menge EVO Lightweight") or 0)
        qty_balance = float(r.get("Menge EVO Balance") or 0)

        if cfg.base.key == "spartan":
            qty = qty_spartan
        elif cfg.base.key == "lightweight":
            qty = qty_light
        else:
            qty = qty_balance

        if qty <= 0:
            continue

        comp_name = (r.get("item_name") or "").strip()
        if not comp_name:
            continue

        comp_product_id = find_product_by_name_ilike(api, [comp_name])
        add_bom_line(api, bom_id, comp_product_id, qty)
        created_lines += 1

    hull_product_id = find_product_by_name_ilike(
        api, ["haube", "evo2", cfg.hull_color]
    )
    add_bom_line(api, bom_id, hull_product_id, 1.0)
    created_lines += 1

    variant_label = cfg.base.key  # "spartan" | "lightweight" | "balance"
    plate_product_id = find_product_by_name_ilike(
        api,
        ["grundplatte", "evo 2", variant_label, cfg.plate_color],
    )
    add_bom_line(api, bom_id, plate_product_id, 1.0)
    created_lines += 1

    foot_product_id = find_product_by_name_ilike(
        api,
        ["fuß", "evo2", variant_label, cfg.foot_color],
    )
    add_bom_line(api, bom_id, foot_product_id, 4.0)
    created_lines += 1

    return created_lines


def run_variant_generation(api: OdooClient) -> None:
    log_header("Varianten & Varianten-BOMs generieren")

    mengen_rows = load_mengenstueckliste(MENGE_CSV_PATH)
    hauben_map, fuesse_map, grundplatten_map = build_color_maps_mengen(mengen_rows)
    common_components = load_common_components(mengen_rows)

    bases = [
        BaseVariant(name=PRODUCT_SPARTAN_NAME, key="spartan"),
        BaseVariant(name=PRODUCT_LIGHTWEIGHT_NAME, key="lightweight"),
        BaseVariant(name=PRODUCT_BALANCE_NAME, key="balance"),
    ]

    attr_values_all = {
        "Haubenfarbe": sorted(hauben_map.keys()),
        "Fussfarbe": sorted({c for v in fuesse_map.values() for c in v.keys()}),
        "Plattenfarbe": sorted({c for v in grundplatten_map.values() for c in v.keys()}),
    }

    tmpl_ids: Dict[str, int] = {}
    for base in bases:
        tmpl_id = find_template_by_name(api, base.name)
        tmpl_ids[base.key] = tmpl_id
        attach_attributes_to_template(api, tmpl_id, attr_values_all)
        log_info(f"[VARIANT:ATTR] Template '{base.name}' (id={tmpl_id}) mit Attributen versehen.")

    configs = generate_all_configs(bases, hauben_map, fuesse_map, grundplatten_map)
    log_info(f"{len(configs)} Drohnenkonfigurationen ermittelt.")

    total_lines = 0
    errors = 0

    for cfg in track(configs, description="BOMs für Varianten erzeugen..."):
        tmpl_id = tmpl_ids[cfg.base.key]
        try:
            created = create_bom_for_config(
                api,
                cfg,
                common_components,
                tmpl_id,
                hauben_map,
            )
            total_lines += created
            log_success(
                f"[VARIANT:BOM] {cfg.base.name} – Haube={cfg.hull_color}, "
                f"Füße={cfg.foot_color}, Platte={cfg.plate_color} "
                f"(Lines={created})"
            )
        except Exception as exc:
            errors += 1
            log_warn(
                f"[VARIANT:BOM:FAIL] {cfg.base.name} – Haube={cfg.hull_color}, "
                f"Füße={cfg.foot_color}, Platte={cfg.plate_color}: {exc}"
            )

    log_info(
        f"[VARIANT:SUMMARY] {len(configs) - errors} erfolgreiche Konfigurationen, "
        f"{errors} mit Fehlern, insgesamt {total_lines} BOM-Zeilen erzeugt."
    )

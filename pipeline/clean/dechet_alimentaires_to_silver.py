#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import pandas as pd  # type: ignore
import re
import json
import argparse

# ---------- Utils lecture & parsing ----------
def guess_read_csv(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, sep=";", dtype=str, low_memory=False)
        if df.shape[1] < 2:
            df = pd.read_csv(path, sep=",", dtype=str, low_memory=False)
        return df
    except Exception as e:
        raise RuntimeError(f"Lecture CSV échouée pour {path}: {e}")

def parse_lon_lat_from_point(s: str):
    if not isinstance(s, str):
        return (None, None)
    # format "lat, lon"
    m = re.match(r"\s*([\-0-9.]+)\s*,\s*([\-0-9.]+)\s*$", s)
    if m:
        lat = pd.to_numeric(m.group(1), errors="coerce")
        lon = pd.to_numeric(m.group(2), errors="coerce")
        return (lon, lat)
    return (None, None)

def parse_lon_lat_from_geojson(s: str):
    if not isinstance(s, str):
        return (None, None)
    try:
        obj = json.loads(s.replace('""','"'))
        coords = obj.get("coordinates") if isinstance(obj, dict) else None
        if coords and len(coords) >= 2:
            lon = pd.to_numeric(coords[0], errors="coerce")
            lat = pd.to_numeric(coords[1], errors="coerce")
            return (lon, lat)
    except Exception:
        pass
    # fallback regex
    m = re.search(r"\[([\-0-9.]+)\s*,\s*([\-0-9.]+)\]", s)
    if m:
        lon = pd.to_numeric(m.group(1), errors="coerce")
        lat = pd.to_numeric(m.group(2), errors="coerce")
        return (lon, lat)
    return (None, None)

# ---------- Fonction principale ----------
def clean_dechets_silver(
    src: str | Path = "data/bronze/abribac_dechets_alimentaires.csv",
    dst: str | Path = "data/silver/abribac_dechets_alimentaires.csv",
) -> Path:
    """Nettoie le CSV PAVDA et écrit une version silver.
    Compatible avec ton main() (pas d'argparse quand appelée en lib).
    """
    src, dst = Path(src), Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)

    print(f"[ABRIBAC] Lecture: {src}")
    df = guess_read_csv(src).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]  # trims entêtes

    # --- RENOMMAGE ROBUSTE ---
    rename = {
        # anciens jeux "éducation" (si jamais)
        "Type d'établissement - Année scolaire": "type_annee_scolaire",
        "Libellé établissement": "etablissement",
        #"Adresse": "adresse",
        "Arrondissement": "arrondissement_txt",
        "Code INSEE": "code_insee",
        #"Année scolaire": "annee_scolaire",
        #"Type établissement": "type_etablissement",

        # jeu PAVDA (ton échantillon)
        #"adr": "adresse",
        "arrdt": "arrondissement_txt",   # ex: 75005 -> on en tire 05
        "pavda_idt": "pavda_id",
        #"pavda_etat": "etat",
        #"cquartier": "quartier",
        #"point_x": "point_x",
        #"point_y": "point_y",

        # géo (communs)
        #"geo_shape": "geo_shape",
        #"geo_point_2d": "geo_point_2d",
    }
    df.rename(columns={k: v for k, v in rename.items() if k in df.columns}, inplace=True)

    # --- ARRONDISSEMENT ---
    # 1) depuis code_insee (751xx)
    arr_num = None
    if "code_insee" in df.columns:
        arr_num = df["code_insee"].astype(str).str.extract(r"^751(\d{2})$", expand=False)

    # 2) sinon depuis arrondissement_txt
    if arr_num is None or arr_num.isna().all():
        if "arrondissement_txt" in df.columns:
            txt = df["arrondissement_txt"].astype(str).str.strip()
            # CP de type 75005
            arr_num = txt.str.extract(r"^750(\d{2})$", expand=False)
            # sinon formats "5", "5e", "5ème", "05", "5eme", etc.
            arr_num = arr_num.fillna(txt.str.extract(r"(\d{1,2})", expand=False))

    if arr_num is not None:
        df["arrondissement"] = pd.to_numeric(arr_num, errors="coerce")
        df["arrondissement_str"] = df["arrondissement"].apply(
            lambda x: f"{int(x):02d}" if pd.notna(x) and 1 <= int(x) <= 20 else None
        )

    # --- LON/LAT (priorité geo_point_2d, fallback geo_shape) ---
    if "geo_point_2d" in df.columns:
        lon_lat_point = df["geo_point_2d"].apply(parse_lon_lat_from_point).tolist()
    else:
        lon_lat_point = [(None, None)] * len(df)

    if "geo_shape" in df.columns:
        lon_lat_shape = df["geo_shape"].apply(parse_lon_lat_from_geojson).tolist()
        lon_lat = [(lon or lon2, lat or lat2) for (lon, lat), (lon2, lat2) in zip(lon_lat_point, lon_lat_shape)]
    else:
        lon_lat = lon_lat_point

    df["longitude"] = [t[0] for t in lon_lat]
    df["latitude"]  = [t[1] for t in lon_lat]

    # --- STABILISER LE SCHÉMA (cols vides si absentes) ---
    for col in ["arrondissement", "arrondissement_str"]:
        if col not in df.columns:
            df[col] = pd.NA

    # --- COLONNES FINALES (compat + PAVDA utiles) ---
    all_cols = [
        # standard "éducation" si présents
        #"type_annee_scolaire","etablissement","annee_scolaire",
        "pavda_id",
        "type_etablissement",
        # communs
        #"adresse",
        "arrondissement_txt",
        "code_insee",
        #"quartier",
        #"etat",
        #"point_x","point_y",
        #"geo_shape","geo_point_2d",
        "longitude","latitude",
        "arrondissement",
        #"arrondissement_str",
    ]
    keep = [c for c in all_cols if c in df.columns]

    df[keep].to_csv(dst, index=False)
    out = dst.resolve()
    print(f"[ABRIBAC] OK: {len(df):,} lignes → {out}")
    return out

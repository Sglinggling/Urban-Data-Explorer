#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import pandas as pd

def guess_read_csv(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, sep=";", dtype=str, low_memory=False)
        if "valeur_fonciere" not in df.columns:
            df = pd.read_csv(path, sep=",", dtype=str, low_memory=False)
        return df
    except Exception as e:
        raise RuntimeError(f"Lecture CSV échouée pour {path}: {e}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="src", default="../../data/bronze/dvf.csv")
    ap.add_argument("--out", dest="dst", default="../../data/silver/transactions_residentiel.csv")
    args = ap.parse_args()

    src = Path(args.src)
    dst = Path(args.dst)
    dst.parent.mkdir(parents=True, exist_ok=True)

    print(f"[DVF] Lecture: {src}")
    df = guess_read_csv(src).dropna(how="all")

    # En-tête dupliquée éventuelle
    if "valeur_fonciere" in df.columns:
        df = df[df["valeur_fonciere"] != "valeur_fonciere"]

    keep = [
        "id_mutation","date_mutation","nature_mutation","valeur_fonciere",
        "code_postal","type_local","surface_reelle_bati",
        "nombre_pieces_principales","longitude","latitude"
    ]
    df = df[[c for c in keep if c in df.columns]].copy()

    # Casts légers (sans KPI)
    for c in ["valeur_fonciere","surface_reelle_bati","nombre_pieces_principales"]:
        if c in df.columns:
            df[c] = (df[c].str.replace(",", ".", regex=False)
                           .str.replace(" ", "", regex=False))
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Standardisation minimale
    if "date_mutation" in df.columns:
        df["date_mutation"] = pd.to_datetime(df["date_mutation"], errors="coerce")
        df["annee"] = df["date_mutation"].dt.year

    if "code_postal" in df.columns:
        mask_paris = df["code_postal"].astype(str).str.match(r"750(0[1-9]|1[0-9]|20)", na=False)
        df = df[mask_paris]
        df["arrondissement"] = df["code_postal"].astype(str).str[-2:]

    # Filtres basiques (sans agrégats)
    if "nature_mutation" in df.columns:
        df = df[df["nature_mutation"].isin(["Vente","Vente en l'état futur d'achèvement"])]
    if "type_local" in df.columns:
        df = df[df["type_local"].isin(["Appartement","Maison"])]

    cols_out = [c for c in [
        "id_mutation","date_mutation","annee","arrondissement","code_postal",
        "nature_mutation","type_local","surface_reelle_bati",
        "nombre_pieces_principales","valeur_fonciere","longitude","latitude"
    ] if c in df.columns]

    df.to_csv(dst, index=False)
    print(f"[DVF] OK: {len(df):,} lignes → {dst.resolve()}")

if __name__ == "__main__":
    main()

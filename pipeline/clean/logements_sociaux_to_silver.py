#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import pandas as pd

def guess_read_csv(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, sep=";", dtype=str, low_memory=False)
        # Si les colonnes FR ne sont pas là, retente avec ','
        if not any(c in df.columns for c in ["Année du financement - agrément", "Code postal"]):
            df = pd.read_csv(path, sep=",", dtype=str, low_memory=False)
        return df
    except Exception as e:
        raise RuntimeError(f"Lecture CSV échouée pour {path}: {e}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="src", default="../../data/bronze/logements-sociaux-finances-a-paris.csv")
    ap.add_argument("--out", dest="dst", default="../../data/silver/logements_sociaux_programmes.csv")
    args = ap.parse_args()

    src = Path(args.src)
    dst = Path(args.dst)
    dst.parent.mkdir(parents=True, exist_ok=True)

    print(f"[LS] Lecture: {src}")
    df = guess_read_csv(src).dropna(how="all")

    # Normalisation noms (sans agrégats/KPI)
    rename = {
        "Année du financement - agrément": "annee",
        "Arrondissement": "arrondissement",
        "Nombre total de logements financés": "nb_total",
        "Dont nombre de logements PLA I": "nb_plai",
        "Dont nombre de logements PLUS": "nb_plus",
        "Dont nombre de logements PLUS CD": "nb_plus_cd",
        "Dont nombre de logements PLS": "nb_pls",
        "Bailleur social": "bailleur",
        "Code postal": "code_postal",
        "Adresse du programme": "adresse",
        "Mode de réalisation": "mode_realisation",
        "Ville": "ville",
        "Identifiant livraison": "id_programme",
    }
    df.rename(columns={k: v for k, v in rename.items() if k in df.columns}, inplace=True)

    # Paris uniquement (si dispo)
    if "code_postal" in df.columns:
        mask_paris = df["code_postal"].astype(str).str.match(r"750(0[1-9]|1[0-9]|20)", na=False)
        df = df[mask_paris]
        df["arrondissement"] = df["code_postal"].astype(str).str[-2:]

    # Casts légers
    for c in ["annee","arrondissement","nb_total","nb_plai","nb_plus","nb_plus_cd","nb_pls"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    keep = [c for c in [
        "id_programme","annee","arrondissement","code_postal",
        "adresse","ville","bailleur","mode_realisation",
        "nb_total","nb_plai","nb_plus","nb_plus_cd","nb_pls"
    ] if c in df.columns]

    df[keep].to_csv(dst, index=False)
    print(f"[LS] OK: {len(df):,} lignes → {dst.resolve()}")

if __name__ == "__main__":
    main()

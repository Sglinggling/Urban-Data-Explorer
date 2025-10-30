#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Nettoyage → SILVER (version CSV)
--------------------------------
Ce script nettoie et prépare les données pour la zone SILVER du projet.

Il génère :
- data/silver/transactions_residentiel.csv
- data/silver/logements_sociaux_programmes.csv
- data/silver/logements_sociaux_arr_annee.csv

Entrées attendues (BRONZE) :
- data/bronze/dvf.csv
- data/bronze/logements-sociaux-finances-a-paris.xlsx
"""

import pandas as pd
import numpy as np
from pathlib import Path


# --- Dossiers ---
ROOT = Path(__file__).resolve().parents[2]
BRONZE = ROOT / "data" / "bronze"
SILVER = ROOT / "data" / "silver"

DVF_CSV = BRONZE / "dvf.csv"
LS_XLSX = BRONZE / "logements-sociaux-finances-a-paris.xlsx"


def ensure_dirs():
    """Crée le dossier SILVER s'il n'existe pas."""
    SILVER.mkdir(parents=True, exist_ok=True)


def build_silver_dvf(src: Path, out_path: Path) -> pd.DataFrame:
    """Nettoie le fichier DVF pour ne garder que les ventes résidentielles à Paris."""

    print(f"Lecture du fichier DVF : {src}")

    # --- Lecture sûre : on teste le bon séparateur ---
    try:
        df = pd.read_csv(src, sep=";", low_memory=False)
        if "valeur_fonciere" not in df.columns:
            df = pd.read_csv(src, sep=",", low_memory=False)
    except Exception as e:
        raise RuntimeError(f"Impossible de lire {src}: {e}")

    # --- Nettoyage de base ---
    df = df.dropna(how="all")
    df = df[df["valeur_fonciere"] != "valeur_fonciere"]  # enlève lignes doublon d'en-tête

    # --- Conversion types ---
    num_cols = ["valeur_fonciere", "surface_reelle_bati", "nombre_pieces_principales"]
    for col in num_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", ".", regex=False)
                .str.replace(" ", "", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- Filtre Paris ---
    df = df[df["code_postal"].astype(str).str.match(r"750(0[1-9]|1[0-9]|20)", na=False)]
    df["arrondissement"] = df["code_postal"].astype(str).str[-2:].astype(int)

    # --- Date / année ---
    df["date_mutation"] = pd.to_datetime(df["date_mutation"], errors="coerce")
    df["annee"] = df["date_mutation"].dt.year

    # --- Filtres métier ---
    df = df[df["nature_mutation"].isin(["Vente", "Vente en l'état futur d'achèvement"])]
    df = df[df["type_local"].isin(["Appartement", "Maison"])]

    # --- Calcul prix/m² ---
    df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]
    df = df[
        (df["prix_m2"].between(500, 30000))
        & (df["surface_reelle_bati"].between(8, 1000))
    ]

    # --- Typologie ---
    df["typologie"] = pd.cut(
        df["nombre_pieces_principales"],
        bins=[-1, 1, 2, 3, 4, 100],
        labels=["T1", "T2", "T3", "T4", "T5+"],
    )

    # --- Colonnes utiles finales ---
    cols = [
        "id_mutation",
        "date_mutation",
        "annee",
        "arrondissement",
        "code_postal",
        "type_local",
        "typologie",
        "surface_reelle_bati",
        "nombre_pieces_principales",
        "valeur_fonciere",
        "prix_m2",
        "longitude",
        "latitude",
    ]
    df = df[[c for c in cols if c in df.columns]]

    # --- Sauvegarde ---
    df.to_csv(out_path, index=False)
    print(f"✅ {len(df):,} lignes nettoyées → {out_path}")
    return df

# ---------- 2. Logements sociaux -> SILVER ----------
def build_silver_logements_sociaux(src: Path, out_prog: Path, out_agg: Path):
    """Nettoie les logements sociaux et crée une version agrégée (arrondissement / année)."""

    print(f"Lecture du fichier logements sociaux : {src}")
    df = pd.read_excel(src)

    # Renommage des colonnes
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

    # Garder seulement Paris
    df = df[df["code_postal"].astype(str).str.match(r"750(0[1-9]|1[0-9]|20)", na=False)]
    df["arrondissement"] = df["code_postal"].str[-2:].astype(int)

    # Conversion des nombres
    numeric_cols = ["annee", "arrondissement", "nb_total", "nb_plai", "nb_plus", "nb_plus_cd", "nb_pls"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Export des programmes détaillés
    df.to_csv(out_prog, index=False)
    print(f"✅ {len(df):,} programmes sauvegardéss dans {out_prog}")

    # Agrégat (arrondissement / année)
    agg = (
        df.groupby(["arrondissement", "annee"], dropna=True)
        [["nb_total", "nb_plai", "nb_plus", "nb_plus_cd", "nb_pls"]]
        .sum()
        .reset_index()
        .sort_values(["annee", "arrondissement"])
    )

    agg.to_csv(out_agg, index=False)
    print(f"✅ {len(agg):,} lignes agrégées sauvegardées dans {out_agg}")

    return df, agg


# ---------- MAIN ----------
def main():
    ensure_dirs()

    dvf_out = SILVER / "transactions_residentiel.csv"
    prog_out = SILVER / "logements_sociaux_programmes.csv"
    agg_out = SILVER / "logements_sociaux_arr_annee.csv"

    print("=== Nettoyage DVF ===")
    build_silver_dvf(DVF_CSV, dvf_out)

    print("\n=== Nettoyage Logements Sociaux ===")
    build_silver_logements_sociaux(LS_XLSX, prog_out, agg_out)

    print("\n✅ Toutes les tables SILVER ont été générées avec succès.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Nettoyage -> SILVER
- DVF (transactions résidentielles Paris) -> data/silver/transactions_residentiel.parquet
- Logements sociaux financés (programmes) -> data/silver/logements_sociaux_programmes.parquet
- Agrégat léger pour la suite (arrondissement/année) -> data/silver/logements_sociaux_arr_annee.parquet

Entrées attendues (BRONZE) :
- data/bronze/dvf.csv
- data/bronze/logements-sociaux-finances-a-paris.xlsx
"""

from __future__ import annotations

import os
from pathlib import Path
import pandas as pd
import numpy as np


ROOT = Path(__file__).resolve().parents[2]  # remonte au dossier racine (Urban-Data-Explorer/)
BRONZE = ROOT / "data" / "bronze"
SILVER = ROOT / "data" / "silver"

DVF_CSV = BRONZE / "dvf.csv"
LS_XLSX = BRONZE / "logements-sociaux-finances-a-paris.xlsx"


def ensure_dirs() -> None:
    SILVER.mkdir(parents=True, exist_ok=True)


# ---------- Utils communs ----------

def _to_int_safe(x):
    try:
        return int(x)
    except Exception:
        return pd.NA


def _clean_price(v):
    # DVF stocke des montants parfois avec virgule/espaces ; on force float.
    if pd.isna(v):
        return np.nan
    if isinstance(v, str):
        v = v.replace(",", ".").replace(" ", "")
    try:
        return float(v)
    except Exception:
        return np.nan


def _clip_series(s: pd.Series, low: float | None = None, high: float | None = None) -> pd.Series:
    if low is not None:
        s = s.where(s >= low)
    if high is not None:
        s = s.where(s <= high)
    return s


# ---------- DVF -> SILVER ----------

def build_silver_dvf(src: Path, out_path: Path) -> pd.DataFrame:
    """
    Construit la table SILVER pour DVF:
      - Paris uniquement (code_postal 75001..75020)
      - types résidentiels (Appartement, Maison)
      - prix_m2 = valeur_fonciere / surface_reelle_bati
      - champs normalisés utiles aux KPI
    """
    if not src.exists():
        raise FileNotFoundError(f"Fichier introuvable: {src}")

    # Chargement
    df = pd.read_csv(src, dtype=str)
    # Sélection colonnes utiles
    keep = [
        "id_mutation", "date_mutation", "nature_mutation",
        "valeur_fonciere",
        "code_postal", "code_commune", "nom_commune",
        "type_local", "surface_reelle_bati", "nombre_pieces_principales",
        "longitude", "latitude",
    ]
    existing = [c for c in keep if c in df.columns]
    df = df[existing].copy()

    # Nettoyage basique
    df["valeur_fonciere"] = df["valeur_fonciere"].apply(_clean_price)
    for c in ["surface_reelle_bati", "nombre_pieces_principales", "longitude", "latitude"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Conserver Paris (codes postaux 75001..75020)
    if "code_postal" in df.columns:
        df = df[df["code_postal"].astype(str).str.fullmatch(r"750(0[1-9]|1[0-9]|20)")]
        df = df.copy()

    # Arrondissement à partir du CP (01..20)
    df["arrondissement"] = df["code_postal"].str[-2:].astype("Int64")

    # Date & année
    df["date_mutation"] = pd.to_datetime(df["date_mutation"], errors="coerce", utc=False)
    df["annee"] = df["date_mutation"].dt.year.astype("Int64")

    # Filtre nature mutation (principalement ventes)
    if "nature_mutation" in df.columns:
        df = df[df["nature_mutation"].isin(["Vente", "Vente en l'état futur d'achèvement"])]

    # Typologie : garder résidentiel
    df = df[df["type_local"].isin(["Appartement", "Maison"])]

    # Calcul prix/m2 (avec garde-fous)
    df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]
    # bornes raisonnables pour Paris afin d'éliminer les outliers évidents
    df["surface_reelle_bati"] = _clip_series(df["surface_reelle_bati"], 8, 1000)
    df["prix_m2"] = _clip_series(df["prix_m2"], 500, 30000)

    # Nettoyage final NA
    df = df.dropna(subset=["annee", "arrondissement", "surface_reelle_bati", "valeur_fonciere", "prix_m2"])

    # Typologie (Studio/T1, T2, T3, T4, T5+)
    bins = [-np.inf, 1, 2, 3, 4, np.inf]
    labels = ["T1", "T2", "T3", "T4", "T5+"]
    df["typologie"] = pd.cut(df["nombre_pieces_principales"].fillna(0), bins=bins, labels=labels).astype("string")

    # Colonnes finales ordonnées
    final_cols = [
        "id_mutation", "date_mutation", "annee",
        "arrondissement", "code_postal",
        "type_local", "typologie",
        "surface_reelle_bati", "nombre_pieces_principales",
        "valeur_fonciere", "prix_m2",
        "longitude", "latitude",
    ]
    final_cols = [c for c in final_cols if c in df.columns]
    df = df[final_cols].copy()

    # Ecriture Parquet
    df.to_parquet(out_path, index=False)
    return df


# ---------- Logements sociaux -> SILVER ----------

def build_silver_logements_sociaux(src: Path, out_programmes: Path, out_arr_annee: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Construit deux tables SILVER à partir du fichier xlsx:
      - programmes nettoyés (ligne = programme financé)
      - agrégat léger (arrondissement, année) avec totaux
    """
    if not src.exists():
        raise FileNotFoundError(f"Fichier introuvable: {src}")

    # Lecture (une seule feuille)
    df = pd.read_excel(src)

    # Renommer colonnes (standard)
    rename_map = {
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
        "Commentaires": "commentaires",
        "Identifiant livraison": "id_programme",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Types
    for c in ["annee", "arrondissement", "nb_total", "nb_plai", "nb_plus", "nb_plus_cd", "nb_pls"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    if "code_postal" in df.columns:
        df = df[df["code_postal"].astype(str).str.fullmatch(r"750(0[1-9]|1[0-9]|20)")]
        df = df.copy()

    # Champs conservés
    keep = [
        "id_programme", "annee", "arrondissement", "code_postal",
        "adresse", "ville", "bailleur", "mode_realisation",
        "nb_total", "nb_plai", "nb_plus", "nb_plus_cd", "nb_pls",
    ]
    keep = [c for c in keep if c in df.columns]
    df_prog = df[keep].copy()

    # Ecriture programmes
    df_prog.to_parquet(out_programmes, index=False)

    # Agrégat (arrondissement, année)
    group_cols = ["arrondissement", "annee"]
    df_arr = (
        df_prog.groupby(group_cols, dropna=True)[["nb_total", "nb_plai", "nb_plus", "nb_plus_cd", "nb_pls"]]
        .sum(min_count=1)
        .reset_index()
        .sort_values(group_cols)
    )
    df_arr.to_parquet(out_arr_annee, index=False)

    return df_prog, df_arr


def main():
    ensure_dirs()

    dvf_out = SILVER / "transactions_residentiel.parquet"
    ls_prog_out = SILVER / "logements_sociaux_programmes.parquet"
    ls_arr_out = SILVER / "logements_sociaux_arr_annee.parquet"

    print("➡️  DVF -> SILVER …")
    df_dvf = build_silver_dvf(DVF_CSV, dvf_out)
    print(f"   ✓ {len(df_dvf):,} lignes -> {dvf_out}")

    print("➡️  Logements sociaux -> SILVER …")
    df_prog, df_arr = build_silver_logements_sociaux(LS_XLSX, ls_prog_out, ls_arr_out)
    print(f"   ✓ {len(df_prog):,} programmes -> {ls_prog_out}")
    print(f"   ✓ {len(df_arr):,} lignes agrégées -> {ls_arr_out}")

    print("\nTerminé. Les tables SILVER sont prêtes pour les agrégations GOLD.")


if __name__ == "__main__":
    main()

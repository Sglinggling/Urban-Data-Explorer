import re
from pathlib import Path

import pandas as pd


def clean_colleges(src_path, dst_path):
    src_path = Path(src_path)
    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[COLLEGES] Lecture: {src_path}")
    try:
        # Lecture CSV avec détection automatique du séparateur
        df = pd.read_csv(src_path, sep=";", dtype=str, low_memory=False)
        if not any(c in df.columns for c in ["libelle", "arr_insee"]):
            df = pd.read_csv(src_path, sep=",", dtype=str, low_memory=False)
    except Exception as e:
        raise RuntimeError(f"Lecture CSV échouée pour {src_path}: {e}")

    # Vérification des colonnes nécessaires
    required = ["libelle", "arr_libelle", "arr_insee"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes: {missing}\nColonnes trouvées: {list(df.columns)}")

    # Nettoyage de base
    df = df[required].drop_duplicates()

    # Extraction du numéro d’arrondissement
    df["arr_insee"] = df["arr_insee"].astype(str)
    df["arr_libelle"] = df["arr_libelle"].astype(str)

    df["arr_num"] = (
        df["arr_insee"].str.extract(r"751(\d{2})")[0]
        .fillna(df["arr_libelle"].str.extract(r"(\d{1,2})")[0])
    )

    df["arr_num"] = pd.to_numeric(df["arr_num"], errors="coerce")

    # Filtrage Paris uniquement (1 à 20)
    df = df[df["arr_num"].between(1, 20, inclusive="both")]

    # Renommage final
    df.rename(columns={
        "libelle": "nom_etablissement"
    }, inplace=True)

    keep = ["arr_num", "arr_insee", "arr_libelle", "nom_etablissement"]
    df = df[keep]

    # Sauvegarde
    df.to_csv(dst_path, index=False, encoding="utf-8")
    print(f"[COLLEGES] OK: {len(df):,} lignes → {dst_path.resolve()}")

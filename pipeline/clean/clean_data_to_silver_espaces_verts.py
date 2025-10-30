from pathlib import Path

import pandas as pd


def clean_espaces_verts(
    src: str | Path = "data/bronze/espaces_verts.csv",
    dst: str | Path = "data/silver/espaces_verts_clean.csv",
) -> Path:
    src, dst = Path(src), Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)

    print(f"[ESPACES_VERTS] Lecture: {src}")

    # --- Lecture robuste ---
    try:
        df = pd.read_csv(src, sep=";", dtype=str, low_memory=False)
        if len(df.columns) == 1:  # Mauvais séparateur -> relecture avec ','
            df = pd.read_csv(src, sep=",", dtype=str, low_memory=False)
    except Exception as e:
        raise RuntimeError(f"Erreur de lecture pour {src}: {e}")

    # --- Vérification des colonnes ---
    required = ["nsq_espace_vert", "nom_ev", "type_ev", "adresse_codepostal"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes: {missing}\nColonnes vues: {list(df.columns)}")

    # --- Nettoyage ---
    df = df[required].drop_duplicates(subset=["nsq_espace_vert"])

    # Normalisation du code postal
    df["adresse_codepostal"] = df["adresse_codepostal"].astype(str).str.extract(r"(\d{5})")
    df = df.dropna(subset=["adresse_codepostal"])

    # Extraction du numéro d’arrondissement
    df["arr_num"] = df["adresse_codepostal"].apply(
        lambda cp: int(cp[-2:]) if isinstance(cp, str) and cp.startswith("75") else None
    )
    df = df[df["arr_num"].between(1, 20)]

    # Renommage clair
    df.rename(columns={
        "nsq_espace_vert": "id_espace_vert",
        "nom_ev": "nom_espace_vert",
        "type_ev": "type_espace_vert",
        "adresse_codepostal": "code_postal",
    }, inplace=True)

    # Colonnes finales
    keep = ["id_espace_vert", "nom_espace_vert", "type_espace_vert", "code_postal", "arr_num"]
    df = df[keep]

    # --- Sauvegarde ---
    df.to_csv(dst, index=False, encoding="utf-8")
    print(f"[ESPACES_VERTS] OK: {len(df):,} lignes → {dst.resolve()}")

    return dst.resolve()

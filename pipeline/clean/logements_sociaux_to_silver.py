from pathlib import Path

import pandas as pd


def clean_logements_sociaux(src_path, dst_path):
    src_path = Path(src_path)
    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[LS] Lecture: {src_path}")
    try:
        df = pd.read_csv(src_path, sep=";", dtype=str, low_memory=False)
        if not any(c in df.columns for c in ["Année du financement - agrément", "Code postal"]):
            df = pd.read_csv(src_path, sep=",", dtype=str, low_memory=False)
    except Exception as e:
        raise RuntimeError(f"Lecture CSV échouée pour {src_path}: {e}")

    # Normalisation noms
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

    # Filtrage Paris
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

    df[keep].to_csv(dst_path, index=False)
    print(f"[LS] OK: {len(df):,} lignes → {dst_path.resolve()}")

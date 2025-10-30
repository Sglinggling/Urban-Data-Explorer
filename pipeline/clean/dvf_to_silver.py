from pathlib import Path

import pandas as pd


def clean_dvf(src: str = "data/bronze/dvf.csv",
              dst: str = "data/silver/transactions_residentiel.csv") -> None:
    """
    Nettoie le CSV DVF pour ne garder que les transactions résidentielles à Paris.
    """
    src_path = Path(src)
    dst_path = Path(dst)
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[DVF] Lecture: {src_path}")
    
    # Essaye ; puis , si nécessaire
    try:
        df = pd.read_csv(src_path, sep=";", dtype=str, low_memory=False)
        if "valeur_fonciere" not in df.columns:
            df = pd.read_csv(src_path, sep=",", dtype=str, low_memory=False)
    except Exception as e:
        raise RuntimeError(f"Lecture CSV échouée pour {src_path}: {e}")

    df = df.dropna(how="all")

    # En-têtes dupliquées
    if "valeur_fonciere" in df.columns:
        df = df[df["valeur_fonciere"] != "valeur_fonciere"]

    keep_cols = [
        "id_mutation","date_mutation","nature_mutation","valeur_fonciere",
        "code_postal","type_local","surface_reelle_bati",
        "nombre_pieces_principales","longitude","latitude"
    ]
    df = df[[c for c in keep_cols if c in df.columns]].copy()

    # Casts numériques
    for c in ["valeur_fonciere","surface_reelle_bati","nombre_pieces_principales"]:
        if c in df.columns:
            df[c] = (df[c].str.replace(",", ".", regex=False)
                           .str.replace(" ", "", regex=False))
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Dates et année
    if "date_mutation" in df.columns:
        df["date_mutation"] = pd.to_datetime(df["date_mutation"], errors="coerce")
        df["annee"] = df["date_mutation"].dt.year

    # Filtrage Paris et arrondissement
    if "code_postal" in df.columns:
        mask_paris = df["code_postal"].astype(str).str.match(r"750(0[1-9]|1[0-9]|20)", na=False)
        df = df[mask_paris]
        df["arrondissement"] = df["code_postal"].astype(str).str[-2:]

    # Filtrage nature_mutation et type_local
    if "nature_mutation" in df.columns:
        df = df[df["nature_mutation"].isin(["Vente","Vente en l'état futur d'achèvement"])]
    if "type_local" in df.columns:
        df = df[df["type_local"].isin(["Appartement","Maison"])]

    # Colonnes finales
    cols_out = [c for c in [
        "id_mutation","date_mutation","annee","arrondissement","code_postal",
        "nature_mutation","type_local","surface_reelle_bati",
        "nombre_pieces_principales","valeur_fonciere","longitude","latitude"
    ] if c in df.columns]

    df.to_csv(dst_path, index=False)
    print(f"[DVF] OK: {len(df):,} lignes → {dst_path.resolve()}")

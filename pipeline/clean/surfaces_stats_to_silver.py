"""
Agrégation des surfaces bâties réelles par arrondissement et année en couche Silver.
Filtre les valeurs manquantes ou nulles, puis calcule la médiane et la moyenne.
Entrée : data/silver/transactions_residentiel.csv  →  Sortie : data/silver/surfaces_stats.csv
"""
from pathlib import Path

import pandas as pd

SILVER_DIR = Path(__file__).resolve().parents[2] / "data" / "silver"


def compute_surfaces_stats(
    src: Path = None,
    dst: Path = None,
) -> None:
    # Calcule la médiane et la moyenne des surfaces bâties par arrondissement et année
    src = src or SILVER_DIR / "transactions_residentiel.csv"
    dst = dst or SILVER_DIR / "surfaces_stats.csv"

    df = pd.read_csv(src, dtype=str)
    df["annee"] = pd.to_numeric(df["annee"], errors="coerce")
    df["arrondissement"] = pd.to_numeric(df["arrondissement"], errors="coerce")
    df["surface_reelle_bati"] = pd.to_numeric(df["surface_reelle_bati"], errors="coerce")
    # Supprime les lignes incomplètes et les surfaces nulles ou négatives
    df = df.dropna(subset=["annee", "arrondissement", "surface_reelle_bati"])
    df = df[df["surface_reelle_bati"] > 0]
    df["annee"] = df["annee"].round().astype(int)
    df["arrondissement"] = df["arrondissement"].round().astype(int)

    # surface_mediane = median(surface_reelle_bati), surface_mean = mean(surface_reelle_bati), groupées par (annee, arrondissement)
    agg = (
        df.groupby(["annee", "arrondissement"])["surface_reelle_bati"]
        .agg(surface_mediane="median", surface_mean="mean")
        .reset_index()
        .rename(columns={"arrondissement": "arr_num"})
        .sort_values(["annee", "arr_num"])
        .reset_index(drop=True)
    )
    agg["surface_mediane"] = agg["surface_mediane"].round(2)
    agg["surface_mean"] = agg["surface_mean"].round(2)

    dst.parent.mkdir(parents=True, exist_ok=True)
    agg.to_csv(dst, index=False)
    print(f"[SILVER] surfaces_stats: {len(agg)} lignes → {dst.name}")

"""
Agrégation du volume de transactions immobilières en couche Silver.
Groupe les ventes résidentielles par arrondissement et année pour produire
des indicateurs de volume (nombre de ventes, surface totale bâtie).
Entrée : data/silver/transactions_residentiel.csv  →  Sortie : data/silver/volume_transactions.csv
"""

from pathlib import Path

import pandas as pd

SILVER_DIR = Path(__file__).resolve().parents[2] / "data" / "silver"


# Calcule le nombre de transactions et la surface bâtie totale par arrondissement et par année
def compute_volume_transactions(
    src: Path = None,
    dst: Path = None,
) -> None:
    src = src or SILVER_DIR / "transactions_residentiel.csv"
    dst = dst or SILVER_DIR / "volume_transactions.csv"

    df = pd.read_csv(src, dtype=str)
    df["annee"] = pd.to_numeric(df["annee"], errors="coerce")
    df["arrondissement"] = pd.to_numeric(df["arrondissement"], errors="coerce")
    df["surface_reelle_bati"] = pd.to_numeric(df["surface_reelle_bati"], errors="coerce")
    # Supprime les lignes incomplètes ou avec une surface nulle (données incohérentes)
    df = df.dropna(subset=["annee", "arrondissement", "surface_reelle_bati"])
    df = df[df["surface_reelle_bati"] > 0]
    df["annee"] = df["annee"].round().astype(int)
    df["arrondissement"] = df["arrondissement"].round().astype(int)

    agg = (
        df.groupby(["annee", "arrondissement"])
        .agg(
            nb_transactions=("surface_reelle_bati", "count"),
            surface_bati_totale=("surface_reelle_bati", "sum"),
        )
        .reset_index()
        .rename(columns={"arrondissement": "arr_num"})
        .sort_values(["annee", "arr_num"])
        .reset_index(drop=True)
    )

    dst.parent.mkdir(parents=True, exist_ok=True)
    agg.to_csv(dst, index=False)
    print(f"[SILVER] volume_transactions: {len(agg)} lignes → {dst.name}")

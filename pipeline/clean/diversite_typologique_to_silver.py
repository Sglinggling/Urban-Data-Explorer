"""
Calcul de la diversité typologique des logements par arrondissement et année.
Mesure l'équilibre entre studios, T2 et T3+ via l'écart-type de leurs parts relatives.
Entrée : data/gold/typologie_parc.csv  →  Sortie : data/silver/diversite_typologique.csv
"""
from pathlib import Path

import pandas as pd

GOLD_DIR = Path(__file__).resolve().parents[2] / "data" / "gold"
SILVER_DIR = Path(__file__).resolve().parents[2] / "data" / "silver"


def compute_diversite_typologique(
    src: Path = None,
    dst: Path = None,
) -> None:
    # Calcule l'écart-type des parts de chaque type de logement (studio, T2, T3+)
    # comme indicateur de diversité : plus l'écart est faible, plus le parc est équilibré
    src = src or GOLD_DIR / "typologie_parc.csv"
    dst = dst or SILVER_DIR / "diversite_typologique.csv"

    df = pd.read_csv(src)
    df = df.rename(columns={"arrondissement": "arr_num"})
    df["arr_num"] = pd.to_numeric(df["arr_num"], errors="coerce").round().astype(int)
    df["annee"] = pd.to_numeric(df["annee"], errors="coerce").astype(int)

    # Formule : σ(part_studio, part_T2, part_T3+) sur chaque ligne (arrondissement × année)
    df["ecart_type_parts"] = df[
        ["part_studio_pct", "part_T2_pct", "part_T3plus_pct"]
    ].std(axis=1).round(4)

    out = (
        df[["annee", "arr_num", "ecart_type_parts"]]
        .sort_values(["annee", "arr_num"])
        .reset_index(drop=True)
    )

    dst.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(dst, index=False)
    print(f"[SILVER] diversite_typologique: {len(out)} lignes → {dst.name}")

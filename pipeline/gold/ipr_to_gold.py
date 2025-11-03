"""
Calcul de l'Indicateur de Pression Résidentielle (IPR) par arrondissement et année.
Agrège volume de transactions, surfaces médianes et prix au m² en un score composite normalisé.
Entrée : silver/volume_transactions.csv, silver/surfaces_stats.csv, gold/prix_m2_median.csv
Sortie : gold/ipr.csv  (colonnes : annee, arr_num, ipr_score ∈ [0, 100])
"""

from pathlib import Path

import pandas as pd

SILVER_DIR = Path(__file__).resolve().parents[2] / "data" / "silver"
GOLD_DIR = Path(__file__).resolve().parents[2] / "data" / "gold"

IPR_YEARS = [2020, 2021, 2022, 2023, 2024, 2025]


# Normalise une série entre 0 et 100 : score = (x - min) / (max - min) × 100
def _minmax_norm(series: pd.Series) -> pd.Series:
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pd.Series([50.0] * len(series), index=series.index)
    return (series - mn) / (mx - mn) * 100


def compute_ipr(
    volume_src: Path = None,
    surfaces_src: Path = None,
    prix_src: Path = None,
    dst: Path = None,
) -> None:
    # Calcule et exporte le score IPR pour les 20 arrondissements parisiens sur chaque année cible
    volume_src = volume_src or SILVER_DIR / "volume_transactions.csv"
    surfaces_src = surfaces_src or SILVER_DIR / "surfaces_stats.csv"
    prix_src = prix_src or GOLD_DIR / "prix_m2_median.csv"
    dst = dst or GOLD_DIR / "ipr.csv"

    # Silver: volume (nb_transactions + surface_bati_totale for density)
    vol = pd.read_csv(volume_src)
    vol["annee"] = vol["annee"].astype(int)
    vol["arr_num"] = vol["arr_num"].astype(int)
    vol = vol[vol["annee"].isin(IPR_YEARS)][
        ["annee", "arr_num", "nb_transactions", "surface_bati_totale"]
    ]
    # Densité = nombre de transactions rapporté à la surface bâtie de l'arrondissement
    vol["densite_transactions"] = vol["nb_transactions"] / vol["surface_bati_totale"]

    # Silver: surfaces stats (surface_mediane)
    surf = pd.read_csv(surfaces_src)
    surf["annee"] = surf["annee"].astype(int)
    surf["arr_num"] = surf["arr_num"].astype(int)
    surf = surf[surf["annee"].isin(IPR_YEARS)][["annee", "arr_num", "surface_mediane"]]

    # Gold: prix_m2_median
    prix = pd.read_csv(prix_src)
    prix = prix.rename(columns={"arrondissement": "arr_num", "prix_m2": "prix_m2_val"})
    prix["arr_num"] = pd.to_numeric(prix["arr_num"], errors="coerce").round().astype(int)
    prix["annee"] = pd.to_numeric(prix["annee"], errors="coerce").astype(int)
    prix = prix[prix["annee"].isin(IPR_YEARS)][["annee", "arr_num", "prix_m2_val"]]

    all_arrs = list(range(1, 21))
    records = []

    for year in IPR_YEARS:
        # Grille complète des 20 arrondissements pour éviter les trous en cas de données manquantes
        base = pd.DataFrame({"annee": year, "arr_num": all_arrs})

        yr_vol = vol[vol["annee"] == year][["arr_num", "densite_transactions"]]
        base = base.merge(yr_vol, on="arr_num", how="left")

        yr_surf = surf[surf["annee"] == year][["arr_num", "surface_mediane"]]
        base = base.merge(yr_surf, on="arr_num", how="left")

        yr_prix = prix[prix["annee"] == year][["arr_num", "prix_m2_val"]]
        base = base.merge(yr_prix, on="arr_num", how="left")

        base["densite_transactions"] = base["densite_transactions"].fillna(base["densite_transactions"].mean())
        base["surface_mediane"] = base["surface_mediane"].fillna(base["surface_mediane"].mean())
        base["prix_m2_val"] = base["prix_m2_val"].fillna(base["prix_m2_val"].mean())

        base = base.copy()
        base["densite_norm"] = _minmax_norm(base["densite_transactions"]).round(2)
        base["surface_mediane_norm"] = _minmax_norm(base["surface_mediane"]).round(2)
        base["prix_norm"] = _minmax_norm(base["prix_m2_val"]).round(2)
        # IPR = 0.40 × densité_norm + 0.30 × (100 − surface_norm) + 0.30 × prix_norm
        base["ipr_score"] = (
            0.40 * base["densite_norm"]
            + 0.30 * (100 - base["surface_mediane_norm"])
            + 0.30 * base["prix_norm"]
        ).round(2)

        records.append(base)

    result = pd.concat(records, ignore_index=True)
    assert result["ipr_score"].between(0, 100).all(), "IPR hors bornes [0, 100]"

    out = result[["annee", "arr_num", "ipr_score"]].sort_values(["annee", "arr_num"]).reset_index(drop=True)
    out.to_csv(dst, index=False)

    n = len(out)
    print(f"[IPR] {n} lignes écrites → {dst.name}")
    for yr in [2024]:
        top = out[out["annee"] == yr].nlargest(3, "ipr_score")[["arr_num", "ipr_score"]].values.tolist()
        bot = out[out["annee"] == yr].nsmallest(3, "ipr_score")[["arr_num", "ipr_score"]].values.tolist()
        print(f"  {yr} top-3:    {top}")
        print(f"  {yr} bottom-3: {bot}")

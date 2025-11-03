"""
Calcul de l'Indice de Qualité de Vie (IQV) par arrondissement et par année.
Combine espaces verts, abri-bus et volume de transactions pour produire un score 0-100.
Entrées : espaces_verts_by_arr.csv, abribac_by_arr.csv, volume_transactions.csv  →  Sortie : data/gold/iqv.csv
"""
from pathlib import Path

import pandas as pd

GOLD_DIR = Path(__file__).resolve().parents[2] / "data" / "gold"
SILVER_DIR = Path(__file__).resolve().parents[2] / "data" / "silver"

IQV_YEARS = [2020, 2021, 2022, 2023, 2024, 2025]


# Normalise une série entre 0 et 100 (min-max). Renvoie 50 si la série est constante.
# Formule : score = (x - min) / (max - min) × 100
def _minmax_norm(series: pd.Series) -> pd.Series:
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pd.Series([50.0] * len(series), index=series.index)
    return (series - mn) / (mx - mn) * 100


# Calcule et exporte l'IQV pour chaque arrondissement parisien sur les années cibles.
# IQV = 0.50 × ev_norm + 0.30 × abribacs_norm + 0.20 × (100 − densite_norm)
def compute_iqv(
    espaces_src: Path = None,
    abribac_src: Path = None,
    volume_src: Path = None,
    dst: Path = None,
) -> None:
    espaces_src = espaces_src or GOLD_DIR / "espaces_verts_by_arr.csv"
    abribac_src = abribac_src or GOLD_DIR / "abribac_by_arr.csv"
    volume_src = volume_src or SILVER_DIR / "volume_transactions.csv"
    dst = dst or GOLD_DIR / "iqv.csv"

    # Static gold sources
    ev = pd.read_csv(espaces_src)
    ev["arr_num"] = pd.to_numeric(ev["arr_num"], errors="coerce").round().astype(int)
    ev = ev[["arr_num", "surface_totale_m2"]].copy()

    ab = pd.read_csv(abribac_src)
    ab = ab.rename(columns={"arrondissement": "arr_num"})
    ab["arr_num"] = pd.to_numeric(ab["arr_num"], errors="coerce").round().astype(int)
    ab = ab[["arr_num", "nb_abribacs"]].copy()

    # Silver: nb_transactions per (annee, arr_num)
    vol = pd.read_csv(volume_src)
    vol["annee"] = vol["annee"].astype(int)
    vol["arr_num"] = vol["arr_num"].astype(int)
    vol = vol[vol["annee"].isin(IQV_YEARS)][["annee", "arr_num", "nb_transactions"]]

    all_arrs = list(range(1, 21))
    records = []

    for year in IQV_YEARS:
        # Construit la grille complète des 20 arrondissements pour l'année en cours
        base = pd.DataFrame({"annee": year, "arr_num": all_arrs})
        base = base.merge(ev, on="arr_num", how="left")
        base = base.merge(ab, on="arr_num", how="left")

        yr_vol = vol[vol["annee"] == year][["arr_num", "nb_transactions"]]
        base = base.merge(yr_vol, on="arr_num", how="left")
        base["nb_transactions"] = base["nb_transactions"].fillna(0)
        base["surface_totale_m2"] = base["surface_totale_m2"].fillna(base["surface_totale_m2"].mean())
        base["nb_abribacs"] = base["nb_abribacs"].fillna(0)

        # Normalise chaque composante puis calcule le score IQV pondéré
        base = base.copy()
        base["ev_norm"] = _minmax_norm(base["surface_totale_m2"]).round(2)
        base["abribacs_norm"] = _minmax_norm(base["nb_abribacs"]).round(2)
        base["densite_norm"] = _minmax_norm(base["nb_transactions"]).round(2)
        base["iqv_score"] = (
            0.50 * base["ev_norm"]
            + 0.30 * base["abribacs_norm"]
            + 0.20 * (100 - base["densite_norm"])
        ).round(2)

        records.append(base)

    result = pd.concat(records, ignore_index=True)
    assert result["iqv_score"].between(0, 100).all(), "IQV hors bornes [0, 100]"

    out = result[["annee", "arr_num", "iqv_score"]].sort_values(["annee", "arr_num"]).reset_index(drop=True)
    out.to_csv(dst, index=False)

    n = len(out)
    print(f"[IQV] {n} lignes écrites → {dst.name}")
    for yr in [2024]:
        top = out[out["annee"] == yr].nlargest(3, "iqv_score")[["arr_num", "iqv_score"]].values.tolist()
        bot = out[out["annee"] == yr].nsmallest(3, "iqv_score")[["arr_num", "iqv_score"]].values.tolist()
        print(f"  {yr} top-3:    {top}")
        print(f"  {yr} bottom-3: {bot}")

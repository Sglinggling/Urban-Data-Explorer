"""
Construction de l'Indice d'Attractivité du Marché (IAM) immobilier par arrondissement parisien.
Agrège volume de transactions, variation annuelle et diversité typologique en un score 0-100.
Entrée : data/silver/volume_transactions.csv + diversite_typologique.csv  →  Sortie : data/gold/iam.csv
"""

from pathlib import Path

import pandas as pd

SILVER_DIR = Path(__file__).resolve().parents[2] / "data" / "silver"
GOLD_DIR = Path(__file__).resolve().parents[2] / "data" / "gold"

IAM_YEARS = [2020, 2021, 2022, 2023, 2024, 2025]


# Normalise une série sur [0, 100] via min-max ; renvoie 50 si la série est constante
# Formule : score = (x - min) / (max - min) × 100
def _minmax_norm(series: pd.Series) -> pd.Series:
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pd.Series([50.0] * len(series), index=series.index)
    return (series - mn) / (mx - mn) * 100


# Calcule et exporte l'IAM pour chaque arrondissement et chaque année disponible
def compute_iam(
    volume_src: Path = None,
    diversite_src: Path = None,
    dst: Path = None,
) -> None:
    volume_src = volume_src or SILVER_DIR / "volume_transactions.csv"
    diversite_src = diversite_src or SILVER_DIR / "diversite_typologique.csv"
    dst = dst or GOLD_DIR / "iam.csv"

    # Silver: volume transactions
    vol = pd.read_csv(volume_src)
    vol["annee"] = vol["annee"].astype(int)
    vol["arr_num"] = vol["arr_num"].astype(int)
    vol = vol[vol["annee"].isin(IAM_YEARS)][["annee", "arr_num", "nb_transactions"]]

    # Silver: diversite typologique
    div = pd.read_csv(diversite_src)
    div["annee"] = div["annee"].astype(int)
    div["arr_num"] = div["arr_num"].astype(int)
    div = div[div["annee"].isin(IAM_YEARS)][["annee", "arr_num", "ecart_type_parts"]]

    # Construit une grille complète (20 arrondissements × toutes les années) pour éviter les trous
    all_arrs = list(range(1, 21))
    all_rows = []
    for year in IAM_YEARS:
        base = pd.DataFrame({"annee": year, "arr_num": all_arrs})

        yr_vol = vol[vol["annee"] == year][["arr_num", "nb_transactions"]]
        base = base.merge(yr_vol, on="arr_num", how="left")
        base["nb_transactions"] = base["nb_transactions"].fillna(0)

        yr_div = div[div["annee"] == year][["arr_num", "ecart_type_parts"]]
        base = base.merge(yr_div, on="arr_num", how="left")
        mean_div = base["ecart_type_parts"].mean()
        base["ecart_type_parts"] = base["ecart_type_parts"].fillna(
            mean_div if not pd.isna(mean_div) else 0
        )
        all_rows.append(base)

    df = pd.concat(all_rows, ignore_index=True)

    # Variation du volume YoY per arrondissement (0 pour première année disponible)
    # Formule : variation = (nb_transactions_n - nb_transactions_n-1) / nb_transactions_n-1 × 100
    df = df.sort_values(["arr_num", "annee"]).reset_index(drop=True)
    df["nb_transactions_prev"] = df.groupby("arr_num")["nb_transactions"].shift(1)
    df["variation_volume"] = 0.0
    mask = df["nb_transactions_prev"].notna() & (df["nb_transactions_prev"] > 0)
    df.loc[mask, "variation_volume"] = (
        (df.loc[mask, "nb_transactions"] - df.loc[mask, "nb_transactions_prev"])
        / df.loc[mask, "nb_transactions_prev"] * 100
    )
    df["variation_volume"] = df["variation_volume"].fillna(0)

    # Normalise chaque composante par année, puis agrège en score IAM pondéré
    # Formule : IAM = 0.50 × volume_norm + 0.30 × variation_vol_norm + 0.20 × diversite_norm
    records = []
    for year, grp in df.groupby("annee"):
        grp = grp.copy()
        grp["volume_norm"] = _minmax_norm(grp["nb_transactions"]).round(2)
        grp["variation_vol_norm"] = _minmax_norm(grp["variation_volume"]).round(2)
        grp["ecart_type_norm"] = _minmax_norm(grp["ecart_type_parts"]).round(2)
        grp["diversite_norm"] = (100 - grp["ecart_type_norm"]).round(2)
        grp["iam_score"] = (
            0.50 * grp["volume_norm"]
            + 0.30 * grp["variation_vol_norm"]
            + 0.20 * grp["diversite_norm"]
        ).round(2)
        records.append(grp)

    result = pd.concat(records, ignore_index=True)
    assert result["iam_score"].between(0, 100).all(), "IAM hors bornes [0, 100]"

    out = result[["annee", "arr_num", "iam_score"]].sort_values(["annee", "arr_num"]).reset_index(drop=True)
    out.to_csv(dst, index=False)

    n = len(out)
    print(f"[IAM] {n} lignes écrites → {dst.name}")
    for yr in [2024]:
        top = out[out["annee"] == yr].nlargest(3, "iam_score")[["arr_num", "iam_score"]].values.tolist()
        bot = out[out["annee"] == yr].nsmallest(3, "iam_score")[["arr_num", "iam_score"]].values.tolist()
        print(f"  {yr} top-3:    {top}")
        print(f"  {yr} bottom-3: {bot}")

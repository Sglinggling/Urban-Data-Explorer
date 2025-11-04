"""
Calcul de l'Indice de Tension Immobilière (ITI) en couche Gold.
Combine prix au m², variation annuelle et part de logements sociaux
pour produire un score de tension 0-100 par arrondissement et par année.
Entrée : prix_m2_median.csv, variation_prix_m2.csv, logements_sociaux_pct.csv  →  Sortie : iti.csv
"""

from pathlib import Path

import pandas as pd

GOLD_DIR = Path(__file__).resolve().parents[2] / "data" / "gold"

ITI_YEARS = [2022, 2023, 2024, 2025]


# Normalise une série sur l'intervalle [0, 100] via min-max ; renvoie 50 si la série est constante
def _minmax_norm(series: pd.Series) -> pd.Series:
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pd.Series([50.0] * len(series), index=series.index)
    return (series - mn) / (mx - mn) * 100


# Construit l'ITI en fusionnant les trois sources Gold, normalisant par année et agrégeant selon la formule pondérée
def compute_iti(
    prix_src: Path = None,
    variation_src: Path = None,
    sociaux_src: Path = None,
    dst: Path = None,
) -> None:
    prix_src = prix_src or GOLD_DIR / "prix_m2_median.csv"
    variation_src = variation_src or GOLD_DIR / "variation_prix_m2.csv"
    sociaux_src = sociaux_src or GOLD_DIR / "logements_sociaux_pct.csv"
    dst = dst or GOLD_DIR / "iti.csv"

    prix = pd.read_csv(prix_src)
    vari = pd.read_csv(variation_src)
    logt = pd.read_csv(sociaux_src)

    prix = prix.rename(columns={"prix_m2": "prix_m2_val"})
    vari = vari.rename(columns={"variation_%": "variation_val"})
    logt = logt.rename(columns={"logements_sociaux_pct": "sociaux_pct"})

    prix = prix[["annee", "arrondissement", "prix_m2_val"]]
    vari = vari[["annee", "arrondissement", "variation_val"]]
    logt = logt[["annee", "arrondissement", "sociaux_pct"]]

    # Seules les années avec couverture complète des 20 arrondissements sont retenues
    prix = prix[prix["annee"].isin(ITI_YEARS)]
    vari = vari[vari["annee"].isin(ITI_YEARS)]
    logt = logt[logt["annee"].isin(ITI_YEARS)]

    df = prix.merge(vari, on=["annee", "arrondissement"], how="inner")
    df = df.merge(logt, on=["annee", "arrondissement"], how="inner")

    # Normalisation min-max par année pour rendre les indicateurs comparables entre arrondissements
    records = []
    for year, grp in df.groupby("annee"):
        grp = grp.copy()
        grp["prix_norm"] = _minmax_norm(grp["prix_m2_val"]).round(2)
        grp["variation_norm"] = _minmax_norm(grp["variation_val"]).round(2)
        # Inversion du score logements sociaux : plus la part est élevée, moins la tension est forte
        grp["sociaux_inv_norm"] = (100 - _minmax_norm(grp["sociaux_pct"])).round(2)
        # ITI = 0.5 × prix_norm + 0.3 × variation_norm + 0.2 × (100 − sociaux_norm)
        grp["iti"] = (
            0.5 * grp["prix_norm"]
            + 0.3 * grp["variation_norm"]
            + 0.2 * grp["sociaux_inv_norm"]
        ).round(2)
        records.append(grp)

    result = pd.concat(records, ignore_index=True)

    assert result["iti"].between(0, 100).all(), "ITI hors bornes [0, 100]"
    assert result["prix_norm"].between(0, 100).all()
    assert result["variation_norm"].between(0, 100).all()
    assert result["sociaux_inv_norm"].between(0, 100).all()

    out = result[["annee", "arrondissement", "prix_norm", "variation_norm", "sociaux_inv_norm", "iti"]]
    out = out.sort_values(["annee", "arrondissement"]).reset_index(drop=True)
    out.to_csv(dst, index=False)

    n = len(out)
    print(f"[ITI] {n} lignes écrites → {dst.name}")
    for yr in ITI_YEARS:
        top = out[out["annee"] == yr].nlargest(3, "iti")[["arrondissement", "iti"]].values.tolist()
        print(f"  {yr} top-3: {top}")

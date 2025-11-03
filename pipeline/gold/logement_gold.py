"""
Construction des indicateurs Gold sur le logement parisien.
Calcule la part de logements sociaux par arrondissement et année (cumul),
ainsi que la répartition typologique du parc (studio / T2 / T3+).
Entrées : data/silver/  →  Sorties : data/gold/
"""
from pathlib import Path

import pandas as pd


# Calcule la part de logements sociaux (%) par arrondissement pour chaque année cible,
# en cumulant les programmes sociaux livrés jusqu'à cette année.
# Formule : pct = (PLAI + PLUS + PLS cumulés jusqu'à l'année) / transactions résidentielles totales × 100
def compute_logements_sociaux_pct(
    logements_sociaux_src="data/silver/logements_sociaux_programmes.csv",
    logements_residentiel_src="data/silver/transactions_residentiel.csv",
    dst="data/gold/logements_sociaux_pct.csv",
    target_years=(2020, 2021, 2022, 2023, 2024, 2025),
):
    df_social = pd.read_csv(logements_sociaux_src)
    df_trans = pd.read_csv(logements_residentiel_src)

    for col in ("nb_plai", "nb_plus", "nb_pls"):
        df_social[col] = pd.to_numeric(df_social[col], errors="coerce").fillna(0)
    df_social["nb_total_social"] = df_social["nb_plai"] + df_social["nb_plus"] + df_social["nb_pls"]

    total_by_arr = (
        df_trans.groupby("arrondissement")
        .size()
        .reset_index(name="logements_totaux")
    )

    rows = []
    for yr in target_years:
        # Cumul des logements sociaux livrés jusqu'à l'année yr incluse
        df_yr = df_social[df_social["annee"] <= yr]
        social_by_arr = (
            df_yr.groupby("arrondissement")["nb_total_social"]
            .sum()
            .reset_index()
            .rename(columns={"nb_total_social": "logements_sociaux"})
        )
        df = social_by_arr.merge(total_by_arr, on="arrondissement", how="inner")
        df["logements_sociaux_pct"] = (
            df["logements_sociaux"] / df["logements_totaux"] * 100
        ).round(2)
        df["annee"] = yr
        rows.append(df[["annee", "arrondissement", "logements_sociaux_pct"]])

    df_out = pd.concat(rows, ignore_index=True).sort_values(["annee", "arrondissement"])
    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(dst, index=False)
    print(f"[OK] Part de logements sociaux (temporel, {len(target_years)} années) → {dst}")
    return df_out


# Répartit le parc résidentiel en trois catégories (studio, T2, T3+) et calcule
# la part de chaque type par arrondissement et année.
def compute_typologie_parc(
    logements_src="data/silver/transactions_residentiel.csv",
    dst="data/gold/typologie_parc.csv"
):
    print(f"[LOAD] {logements_src}")
    df = pd.read_csv(logements_src)

    # Découpe le nombre de pièces en trois tranches : [0-1] studio, [1-2] T2, [2-100] T3+
    df['type_logement'] = pd.cut(
        df['nombre_pieces_principales'],
        bins=[0,1,2,100],
        labels=['studio','T2','T3plus'],
        right=True
    )

    df_count = df.groupby(['annee','arrondissement','type_logement']).size().unstack(fill_value=0)
    df_count['nb_logements_total'] = df_count.sum(axis=1)

    # Formule : part_X = count_X / nb_logements_total × 100
    for col in ['studio','T2','T3plus']:
        if col in df_count.columns:
            df_count[f'part_{col}_pct'] = round(df_count[col] / df_count['nb_logements_total'] * 100,2)
        else:
            df_count[f'part_{col}_pct'] = 0.0

    df_out = df_count.reset_index()[['annee','arrondissement','part_studio_pct','part_T2_pct','part_T3plus_pct']]

    df_out['annee'] = df_out['annee'].astype(int)
    df_out = df_out.dropna(subset=['part_studio_pct','part_T2_pct','part_T3plus_pct'], how='all')
    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(dst, index=False)
    print(f"[OK] Typologie du parc immobilier → {dst}")
    return df_out

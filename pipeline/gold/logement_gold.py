# -----------------------------
# 1) Part de logements sociaux (%)
# -----------------------------
from pathlib import Path

import pandas as pd


def compute_logements_sociaux_pct(
    logements_sociaux_src="data/silver/logements_sociaux_programmes.csv",
    logements_residentiel_src="data/silver/transactions_residentiel.csv",
    dst="data/gold/logements_sociaux_pct.csv",
):
    df_social = pd.read_csv(logements_sociaux_src)
    df_trans = pd.read_csv(logements_residentiel_src)

    # Somme des unités sociales programmées par arrondissement (toutes années confondues)
    for col in ("nb_plai", "nb_plus", "nb_pls"):
        df_social[col] = pd.to_numeric(df_social[col], errors="coerce").fillna(0)
    df_social["nb_total_social"] = df_social["nb_plai"] + df_social["nb_plus"] + df_social["nb_pls"]
    social_by_arr = (
        df_social.groupby("arrondissement")["nb_total_social"]
        .sum()
        .reset_index()
        .rename(columns={"nb_total_social": "logements_sociaux"})
    )

    # Nombre de transactions résidentielles par arrondissement (proxy du parc)
    total_by_arr = (
        df_trans.groupby("arrondissement")
        .size()
        .reset_index(name="logements_totaux")
    )

    df = social_by_arr.merge(total_by_arr, on="arrondissement", how="inner")
    df["logements_sociaux_pct"] = (
        df["logements_sociaux"] / df["logements_totaux"] * 100
    ).round(2)

    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    df_out = df[["arrondissement", "logements_sociaux_pct"]].sort_values("arrondissement")
    df_out.to_csv(dst, index=False)
    print(f"[OK] Part de logements sociaux → {dst}")
    return df_out


# -----------------------------
# 2) Typologie du parc immobilier
# -----------------------------
def compute_typologie_parc(
    logements_src="data/silver/transactions_residentiel.csv",
    dst="data/gold/typologie_parc.csv"
):
    print(f"[LOAD] {logements_src}")
    df = pd.read_csv(logements_src)

    # Catégorisation
    df['type_logement'] = pd.cut(
        df['nombre_pieces_principales'],
        bins=[0,1,2,100],
        labels=['studio','T2','T3plus'],
        right=True
    )

    # Comptage par arrondissement et type
    df_count = df.groupby(['annee','arrondissement','type_logement']).size().unstack(fill_value=0)
    df_count['nb_logements_total'] = df_count.sum(axis=1)

    # Calcul des pourcentages
    for col in ['studio','T2','T3plus']:
        if col in df_count.columns:
            df_count[f'part_{col}_pct'] = round(df_count[col] / df_count['nb_logements_total'] * 100,2)
        else:
            df_count[f'part_{col}_pct'] = 0.0

    df_out = df_count.reset_index()[['annee','arrondissement','part_studio_pct','part_T2_pct','part_T3plus_pct']]

    df_out['annee'] = df_out['annee'].astype(int)
    # Supprimer les lignes où toutes les colonnes de pourcentage sont vides
    df_out = df_out.dropna(subset=['part_studio_pct','part_T2_pct','part_T3plus_pct'], how='all')
    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(dst, index=False)
    print(f"[OK] Typologie du parc immobilier → {dst}")
    return df_out

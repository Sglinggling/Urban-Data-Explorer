# -----------------------------
# 1) Part de logements sociaux (%)
# -----------------------------
from pathlib import Path

import pandas as pd


def compute_logements_sociaux_pct(social_csv, total_csv, dst="data/gold/logements_sociaux_pct.csv"):
    df_social = pd.read_csv(social_csv)
    df_total = pd.read_csv(total_csv)

    df = df_social.merge(df_total, on="arrondissement", how="left")
    df["logements_sociaux_pct"] = df["logements_sociaux"] / df["logements_totaux"] * 100

    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    df_out = df[["arrondissement","logements_sociaux_pct"]]
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

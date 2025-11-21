# on agrège les trois jeux de données scolaires (maternelles,élémentaires et collèges) nettoyés en Silver
# l’objectif est de regrouper les établissements par arrondissement pour avoir le nombre total d’établissements scolaires par arrondissement 
# et permet d’évaluer facilement l’accès à l’éducation sur l’ensemble du territoire parisien

from pathlib import Path
import pandas as pd

def education_silver_to_gold(
    mat_path="data/silver/ecoles_maternelle_clean.csv",
    ele_path="data/silver/ecoles_elementaires_clean.csv",
    col_path="data/silver/colleges_clean.csv",
    dst_path="data/gold/education_par_arrondissement.csv"
):
    print("[GOLD EDUCATION] Génération des indicateurs…")

    mat = pd.read_csv(mat_path)
    ele = pd.read_csv(ele_path)
    col = pd.read_csv(col_path)

    group_mat = mat.groupby("arr_num").size().rename("nb_maternelles")
    group_ele = ele.groupby("arr_num").size().rename("nb_elementaires")
    group_col = col.groupby("arr_num").size().rename("nb_colleges")

    df = pd.concat([group_mat, group_ele, group_col], axis=1).fillna(0)
    df["nb_total_ecoles"] = df.sum(axis=1)

    Path(dst_path).parent.mkdir(parents=True, exist_ok=True)
    df.reset_index().to_csv(dst_path, index=False)

    print(f"[GOLD EDUCATION] OK → {dst_path}")
    return Path(dst_path)

# on agrège les données Silver des abri-bacs destinés aux déchets alimentaires (PAVDA = points d'apport volontaires déchets alimentaires)
# l’objectif est de compter, pour chaque arrondissement, le nombre total de points d’apport volontaire disponibles
# cela permet d’obtenir un indicateur sur les dispositifs de collecte des biodéchets à Paris
# pour faciliter l’analyse de la couverture du service public de tri 


from pathlib import Path

import pandas as pd


def abribac_silver_to_gold(
    src_path="data/silver/abribac_dechets_alimentaires.csv",
    dst_path="data/gold/abribac_by_arr.csv"
):
    print("[GOLD ABRIBAC] Génération des indicateurs…")

    df = pd.read_csv(src_path)

    # Certains jeux ont "arrondissement", d’autres “arr"
    arr_col = "arrondissement" if "arrondissement" in df.columns else "arr_num"

    group = df.groupby(arr_col)["pavda_id"].count().rename("nb_abribacs")

    result = group.reset_index()

    Path(dst_path).parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(dst_path, index=False)

    print(f"[GOLD ABRIBAC] OK → {dst_path}")
    return Path(dst_path)

"""
Agrégation des espaces verts parisiens en couche Gold.
Regroupe les espaces verts nettoyés par arrondissement et calcule le nombre
de sites et la surface totale végétalisée (m²).
Entrée : data/silver/espaces_verts_clean.csv  →  Sortie : data/gold/espaces_verts_by_arr.csv
"""

from pathlib import Path

import pandas as pd


# Produit un indicateur de végétalisation par arrondissement :
# nb d'espaces verts et surface totale en m²
def espaces_verts_silver_to_gold(
    src_path="data/silver/espaces_verts_clean.csv",
    dst_path="data/gold/espaces_verts_by_arr.csv"
):
    print("[GOLD ESPACES VERTS] Génération des indicateurs…")

    df = pd.read_csv(src_path)

    if "surface_m2" not in df.columns:
        raise ValueError(
            "La colonne 'surface_m2' est manquante dans le Silver. "
            "Vérifie que ton script Bronze → Silver est bien mis à jour."
        )

    # Agrégation par arrondissement : COUNT(id_espace_vert) et SUM(surface_m2)
    group = df.groupby("arr_num").agg(
        nb_espaces_verts=("id_espace_vert", "count"),
        surface_totale_m2=("surface_m2", "sum")
    ).reset_index()

    group["surface_totale_m2"] = group["surface_totale_m2"].round(0).astype(int)

    Path(dst_path).parent.mkdir(parents=True, exist_ok=True)
    group.to_csv(dst_path, index=False)

    print(f"[GOLD ESPACES VERTS] OK → {dst_path}")
    return Path(dst_path)

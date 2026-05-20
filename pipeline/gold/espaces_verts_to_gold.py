# on regroupe l’ensemble des espaces verts nettoyés (couche Silver) par arrondissement 
# on calcule pour chaque arrondissement deux valeurs :
# le nombre total d’espaces verts et la somme des surfaces associées
# cet indicateur permet de mesurer le niveau de végétalisation à l’échelle locale 
# et permet d'évaluer de l’accès aux espaces naturels en milieu urbain


from pathlib import Path

import pandas as pd


def espaces_verts_silver_to_gold(
    src_path="data/silver/espaces_verts_clean.csv",
    dst_path="data/gold/espaces_verts_by_arr.csv"
):
    print("[GOLD ESPACES VERTS] Génération des indicateurs…")

    df = pd.read_csv(src_path)

    # Vérification de la colonne surface
    if "surface_m2" not in df.columns:
        raise ValueError(
            "La colonne 'surface_m2' est manquante dans le Silver. "
            "Vérifie que ton script Bronze → Silver est bien mis à jour."
        )

    # Agrégation par arrondissement
    group = df.groupby("arr_num").agg(
        nb_espaces_verts=("id_espace_vert", "count"),
        surface_totale_m2=("surface_m2", "sum")
    ).reset_index()
    
    group["surface_totale_m2"] = group["surface_totale_m2"].round(0).astype(int)


    # Création du dossier Gold si manquant
    Path(dst_path).parent.mkdir(parents=True, exist_ok=True)
    group.to_csv(dst_path, index=False)

    print(f"[GOLD ESPACES VERTS] OK → {dst_path}")
    return Path(dst_path)

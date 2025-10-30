import os

import pandas as pd


def collect_csv(outputfile,url):
    df = pd.read_csv(url, sep=';',encoding="utf-8")
    # Sauvegarder localement
    
    output_dir = os.path.join("data", "bronze")

    # Créer le dossier s'il n'existe pas
    os.makedirs(output_dir, exist_ok=True)

    # Sauvegarder le fichier
    output_path = os.path.join(output_dir, outputfile)
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"✅ Fichier téléchargé : {output_path}")

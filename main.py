import concurrent.futures
import os

import pipeline.collect.collect_data as p_collect

#import pipeline.clean.clean_data_to_silver_dvf as p_clean  # Décommenter si nécessaire

# Dictionnaire nom de fichier → URL
urls = {
    "logement_sociaux.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/logements-sociaux-finances-a-paris/exports/csv",
    "espace_verts.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/espaces_verts/exports/csv",
    "colleges.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/etablissements-scolaires-colleges/exports/csv",
    "elementaire.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/etablissements-scolaires-ecoles-elementaires/exports/csv",
    "maternelle.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/etablissements-scolaires-maternelles/exports/csv",
    "abribac_dechets_alimentaires.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/dechets-menagers-pavda/exports/csv"
}

# Vérifie si le fichier existe et télécharge sinon
def collect_if_missing(filename, url):
    path = os.path.join("data", "bronze", filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        try:
            p_collect.collect_csv(filename, url)
        except Exception as e:
            print(f"❌ Échec téléchargement {filename}: {e}")
    else:
        print(f"⚠️ Fichier déjà présent, skip : {filename}")

def main():
    # Téléchargement parallèle
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(lambda args: collect_if_missing(*args), urls.items())

    # Nettoyage / création silver
    #p_clean.build_silver_dvf()
    #p_clean.build_silver_logements_sociaux()

if __name__ == "__main__":
    main()

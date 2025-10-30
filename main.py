import concurrent.futures
import os
from pathlib import Path

import pipeline.collect.collect_data as p_collect

from pipeline.clean.dvf_to_silver import clean_dvf
from pipeline.clean.logements_sociaux_to_silver import clean_logements_sociaux
from pipeline.clean.dechet_alimentaires_to_silver import clean_dechets_silver

from pipeline.clean.colleges_to_silver import clean_colleges
from pipeline.clean.elementaires_to_silver import clean_elementaires
from pipeline.clean.maternelles_to_silver import clean_maternelles
from pipeline.clean.espaces_verts_to_silver import clean_espaces_verts

ROOT = Path(__file__).parent.resolve()
BRONZE_DIR = ROOT / "data" / "bronze"
SILVER_DIR = ROOT / "data" / "silver"

# Dictionnaire nom de fichier → URL (les noms bronze doivent matcher les SRC par défaut)
urls = {
    "dvf.csv": "https://static.data.gouv.fr/resources/demandes-de-valeurs-foncieres-dvf/20240722-141340/france.csv.gz",  # ajuste si besoin
    "logement_sociaux.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/logements-sociaux-finances-a-paris/exports/csv",
    "espaces_verts.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/espaces_verts/exports/csv",
    "etablissements-scolaires-colleges.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/etablissements-scolaires-colleges/exports/csv",
    "etablissements-scolaires-ecoles-elementaires.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/etablissements-scolaires-ecoles-elementaires/exports/csv",
    "etablissements-scolaires-maternelles.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/etablissements-scolaires-maternelles/exports/csv",
    "abribac_dechets_alimentaires.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/dechets-menagers-pavda/exports/csv",
}

def collect_if_missing(filename: str, url: str):
    path = BRONZE_DIR / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        try:
            p_collect.collect_csv(filename, url)
        except Exception as e:
            print(f"❌ Échec téléchargement {filename}: {e}")
    else:
        print(f"⚠️ Fichier déjà présent, skip : {filename}")

def main():
    # 1) Téléchargements en parallèle
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(lambda kv: collect_if_missing(*kv), urls.items())

    # 2) Silver
    clean_dvf(BRONZE_DIR / "dvf.csv", SILVER_DIR / "transactions_residentiel.csv")
    clean_logements_sociaux(BRONZE_DIR / "logement_sociaux.csv", SILVER_DIR / "logements_sociaux_programmes.csv")
    clean_dechets_silver(BRONZE_DIR / "abribac_dechets_alimentaires.csv", SILVER_DIR / "abribac_dechets_alimentaires.csv")

    clean_colleges(BRONZE_DIR / "etablissements-scolaires-colleges.csv", SILVER_DIR / "colleges_clean.csv")
    clean_elementaires(BRONZE_DIR / "etablissements-scolaires-ecoles-elementaires.csv", SILVER_DIR / "ecoles_elementaires_clean.csv")
    clean_maternelles(BRONZE_DIR / "etablissements-scolaires-maternelles.csv", SILVER_DIR / "ecoles_maternelles_clean.csv")
    clean_espaces_verts(BRONZE_DIR / "espaces_verts.csv", SILVER_DIR / "espaces_verts_clean.csv")

if __name__ == "__main__":
    main()

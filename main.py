import concurrent.futures
import os
from pathlib import Path

import pipeline.collect.collect_data as p_collect
from pipeline.clean.clean_data_to_silver_college import clean_colleges
from pipeline.clean.clean_data_to_silver_elementaire import clean_elementaires
from pipeline.clean.clean_data_to_silver_espaces_verts import \
    clean_espaces_verts
from pipeline.clean.clean_data_to_silver_maternelles import clean_maternelles
from pipeline.clean.dvf_to_silver import clean_dvf
from pipeline.clean.logements_sociaux_to_silver import clean_logements_sociaux

#from pipeline.clean.colleges_to_silver import clean_colleges

ROOT = Path(__file__).parent.resolve()
BRONZE_DIR = ROOT / "data" / "bronze"
SILVER_DIR = ROOT / "data" / "silver"

urls = {
    "logement_sociaux.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/logements-sociaux-finances-a-paris/exports/csv",
    "espace_verts.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/espaces_verts/exports/csv",
    "colleges.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/etablissements-scolaires-colleges/exports/csv",
    "elementaire.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/etablissements-scolaires-ecoles-elementaires/exports/csv",
    "maternelle.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/etablissements-scolaires-maternelles/exports/csv",
    "abribac_dechets_alimentaires.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/dechets-menagers-pavda/exports/csv"
}

def collect(filename, url):
    path = BRONZE_DIR / filename
    os.makedirs(BRONZE_DIR, exist_ok=True)
    p_collect.collect_csv(filename, url)

def main():
    # Téléchargement parallèle
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(lambda args: collect(*args), urls.items())

    # Nettoyage SEQUENTIEL
    clean_dvf(BRONZE_DIR / "dvf.csv", SILVER_DIR / "transactions_residentiel.csv")
    clean_logements_sociaux(BRONZE_DIR / "logement_sociaux.csv", SILVER_DIR / "logements_sociaux_programmes.csv")
    clean_colleges(BRONZE_DIR / "colleges.csv", SILVER_DIR / "colleges_clean.csv")
    clean_elementaires(BRONZE_DIR / "elementaire.csv", SILVER_DIR / "ecoles_elementaires_clean.csv")
    clean_maternelles(BRONZE_DIR / "maternelle.csv", SILVER_DIR / "ecoles_maternelle_clean.csv")
    clean_espaces_verts(BRONZE_DIR / "espace_verts.csv", SILVER_DIR / "espace_vert_clean.csv")


if __name__ == "__main__":
    main()

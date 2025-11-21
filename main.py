import concurrent.futures
import os
from pathlib import Path

import pipeline.collect.collect_data as p_collect
from pipeline.clean.clean_data_to_silver_abribac import clean_dechets_silver
from pipeline.clean.clean_data_to_silver_college import clean_colleges
from pipeline.clean.clean_data_to_silver_elementaire import clean_elementaires
from pipeline.clean.clean_data_to_silver_espaces_verts import \
    clean_espaces_verts
from pipeline.clean.clean_data_to_silver_maternelles import clean_maternelles
from pipeline.clean.dvf_to_silver import clean_dvf
from pipeline.clean.logements_sociaux_to_silver import clean_logements_sociaux
from pipeline.gold.abribac_to_gold import abribac_silver_to_gold
from pipeline.gold.dvf_gold import (compute_prix_m2_median,
                                    compute_variation_prix_m2)
## Importation des fonctions
from pipeline.gold.education_to_gold import education_silver_to_gold
from pipeline.gold.espaces_verts_to_gold import espaces_verts_silver_to_gold
from pipeline.gold.logement_gold import compute_typologie_parc

#from pipeline.clean.colleges_to_silver import clean_colleges

ROOT = Path(__file__).parent.resolve()
BRONZE_DIR = ROOT / "data" / "bronze"
SILVER_DIR = ROOT / "data" / "silver"
GOLD_DIR = ROOT / "data" / "gold"


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
    clean_dechets_silver(BRONZE_DIR / "abribac_dechets_alimentaires.csv", SILVER_DIR / "abribac_dechets_alimentaires.csv") 

    ######## GOLD ########################
    
    prix_m2_path = ROOT / "data" / "gold" / "prix_m2_median.csv"
    variation_path = ROOT / "data" / "gold" / "variation_prix_m2.csv"

    compute_prix_m2_median(src=SILVER_DIR / "transactions_residentiel.csv",
        dst=prix_m2_path
    )
    compute_variation_prix_m2( src=prix_m2_path, dst=variation_path)

    # Logements totaux
    logements_totaux_path = SILVER_DIR / "logements_totaux.csv"
   # compute_logements_totaux(logements_src=SILVER_DIR / "transactions_residentiel.csv", dst=logements_totaux_path)

    # Part de logements sociaux (%)
    logements_sociaux_pct_path = GOLD_DIR / "logements_sociaux_pct.csv"
    # compute_logements_sociaux_pct(
    #     logements_sociaux_src=SILVER_DIR / "logements_sociaux_programmes.csv",
    #     logements_residentiel_src=SILVER_DIR / "transactions_residentiel.csv",
    #     dst=logements_sociaux_pct_path
    # )


    # Typologie du parc immobilier
    typologie_parc_path = GOLD_DIR / "typologie_parc.csv"
    compute_typologie_parc(
        logements_src=SILVER_DIR / "transactions_residentiel.csv",
        dst=typologie_parc_path
    )
    education_silver_to_gold()
    espaces_verts_silver_to_gold() 
    abribac_silver_to_gold()

if __name__ == "__main__":
    main()

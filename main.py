import concurrent.futures
import os
import sys
from pathlib import Path

import pipeline.collect.collect_data as p_collect
from pipeline.clean.clean_data_to_silver_abribac import clean_dechets_silver
from pipeline.clean.clean_data_to_silver_college import clean_colleges
from pipeline.clean.clean_data_to_silver_elementaire import clean_elementaires
from pipeline.clean.clean_data_to_silver_espaces_verts import clean_espaces_verts
from pipeline.clean.clean_data_to_silver_maternelles import clean_maternelles
from pipeline.clean.dvf_to_silver import clean_dvf
from pipeline.clean.logements_sociaux_to_silver import clean_logements_sociaux
from pipeline.gold.abribac_to_gold import abribac_silver_to_gold
from pipeline.gold.dvf_gold import compute_prix_m2_median, compute_variation_prix_m2
from pipeline.gold.education_to_gold import education_silver_to_gold
from pipeline.gold.espaces_verts_to_gold import espaces_verts_silver_to_gold
from pipeline.gold.logement_gold import compute_logements_sociaux_pct, compute_typologie_parc

ROOT = Path(__file__).parent.resolve()
BRONZE_DIR = ROOT / "data" / "bronze"
SILVER_DIR = ROOT / "data" / "silver"
GOLD_DIR = ROOT / "data" / "gold"

GOLD_EXPECTED = [
    "prix_m2_median.csv",
    "variation_prix_m2.csv",
    "logements_sociaux_pct.csv",
    "typologie_parc.csv",
    "education_par_arrondissement.csv",
    "espaces_verts_by_arr.csv",
    "abribac_by_arr.csv",
]

urls = {
    "logement_sociaux.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/logements-sociaux-finances-a-paris/exports/csv",
    "espace_verts.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/espaces_verts/exports/csv",
    "colleges.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/etablissements-scolaires-colleges/exports/csv",
    "elementaire.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/etablissements-scolaires-ecoles-elementaires/exports/csv",
    "maternelle.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/etablissements-scolaires-maternelles/exports/csv",
    "abribac_dechets_alimentaires.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/dechets-menagers-pavda/exports/csv",
}


def collect(filename, url):
    os.makedirs(BRONZE_DIR, exist_ok=True)
    p_collect.collect_csv(filename, url)


def main():
    os.makedirs(BRONZE_DIR, exist_ok=True)
    os.makedirs(SILVER_DIR, exist_ok=True)
    os.makedirs(GOLD_DIR, exist_ok=True)

    # Téléchargement parallèle (opendata.paris.fr)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(collect, fn, url): fn for fn, url in urls.items()}
        for future in concurrent.futures.as_completed(futures):
            fn = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"[ERREUR COLLECT] {fn}: {e}", file=sys.stderr)
                sys.exit(1)

    # Silver
    dvf_bronze = BRONZE_DIR / "dvf.csv"
    dvf_silver = SILVER_DIR / "transactions_residentiel.csv"
    if dvf_bronze.exists():
        clean_dvf(dvf_bronze, dvf_silver)
    elif dvf_silver.exists():
        print(f"[DVF] bronze/dvf.csv absent, silver pré-existant conservé.")
    else:
        print("[ERREUR] bronze/dvf.csv introuvable et aucun silver pré-existant.", file=sys.stderr)
        sys.exit(1)

    clean_logements_sociaux(BRONZE_DIR / "logement_sociaux.csv", SILVER_DIR / "logements_sociaux_programmes.csv")
    clean_colleges(BRONZE_DIR / "colleges.csv", SILVER_DIR / "colleges_clean.csv")
    clean_elementaires(BRONZE_DIR / "elementaire.csv", SILVER_DIR / "ecoles_elementaires_clean.csv")
    clean_maternelles(BRONZE_DIR / "maternelle.csv", SILVER_DIR / "ecoles_maternelle_clean.csv")
    clean_dechets_silver(BRONZE_DIR / "abribac_dechets_alimentaires.csv", SILVER_DIR / "abribac_dechets_alimentaires.csv")
    clean_espaces_verts(BRONZE_DIR / "espace_verts.csv", SILVER_DIR / "espaces_verts_clean.csv")

    # Gold
    prix_m2_path = GOLD_DIR / "prix_m2_median.csv"
    variation_path = GOLD_DIR / "variation_prix_m2.csv"

    compute_prix_m2_median(src=dvf_silver, dst=prix_m2_path)
    compute_variation_prix_m2(src=prix_m2_path, dst=variation_path)

    compute_logements_sociaux_pct(
        logements_sociaux_src=SILVER_DIR / "logements_sociaux_programmes.csv",
        logements_residentiel_src=dvf_silver,
        dst=GOLD_DIR / "logements_sociaux_pct.csv",
    )
    compute_typologie_parc(
        logements_src=dvf_silver,
        dst=GOLD_DIR / "typologie_parc.csv",
    )
    education_silver_to_gold()
    espaces_verts_silver_to_gold(SILVER_DIR / "espaces_verts_clean.csv")
    abribac_silver_to_gold()

    # Vérification finale
    missing = [f for f in GOLD_EXPECTED if not (GOLD_DIR / f).exists() or (GOLD_DIR / f).stat().st_size == 0]
    if missing:
        print(f"[ERREUR] Fichiers gold manquants ou vides : {missing}", file=sys.stderr)
        sys.exit(1)

    print("Pipeline terminé, data/gold/ prêt")


if __name__ == "__main__":
    main()

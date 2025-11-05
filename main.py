"""
Point d'entrée du pipeline de données Urban Data Explorer.
Orchestre les étapes Bronze → Silver → Gold → PostgreSQL/MongoDB
en téléchargeant les sources open data et en calculant les indicateurs urbains.
Entrée : URLs opendata.paris.fr  →  Sortie : data/gold/*.csv + bases de données
"""
import concurrent.futures
import os
import shutil
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import pipeline.collect.collect_data as p_collect
from pipeline.clean.clean_data_to_silver_abribac import clean_dechets_silver
from pipeline.clean.clean_data_to_silver_espaces_verts import clean_espaces_verts
from pipeline.clean.dvf_to_silver import clean_dvf
from pipeline.clean.logements_sociaux_to_silver import clean_logements_sociaux
from pipeline.gold.abribac_to_gold import abribac_silver_to_gold
from pipeline.gold.dvf_gold import compute_prix_m2_median, compute_variation_prix_m2
from pipeline.gold.espaces_verts_to_gold import espaces_verts_silver_to_gold
from pipeline.clean.diversite_typologique_to_silver import compute_diversite_typologique
from pipeline.clean.surfaces_stats_to_silver import compute_surfaces_stats
from pipeline.clean.volume_transactions_to_silver import compute_volume_transactions
from pipeline.gold.iam_to_gold import compute_iam
from pipeline.gold.ipr_to_gold import compute_ipr
from pipeline.gold.iqv_to_gold import compute_iqv
from pipeline.gold.iti_to_gold import compute_iti
from pipeline.gold.logement_gold import compute_logements_sociaux_pct, compute_typologie_parc

ROOT = Path(__file__).parent.resolve()
BRONZE_DIR = ROOT / "data" / "bronze"
SILVER_DIR = ROOT / "data" / "silver"
GOLD_DIR = ROOT / "data" / "gold"

# Fichiers gold attendus en sortie de pipeline — utilisés pour la vérification d'intégrité finale
GOLD_EXPECTED = [
    "prix_m2_median.csv",
    "variation_prix_m2.csv",
    "logements_sociaux_pct.csv",
    "typologie_parc.csv",
    "espaces_verts_by_arr.csv",
    "abribac_by_arr.csv",
    "iti.csv",
    "iqv.csv",
    "iam.csv",
    "ipr.csv",
]

urls = {
    "logement_sociaux.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/logements-sociaux-finances-a-paris/exports/csv",
    "espace_verts.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/espaces_verts/exports/csv",
    "abribac_dechets_alimentaires.csv": "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/dechets-menagers-pavda/exports/csv",
}

_perf_log: list[tuple[str, float]] = []


# Mesure le temps d'exécution d'un bloc et l'enregistre dans _perf_log
@contextmanager
def timed_step(name: str):
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    _perf_log.append((name, elapsed))
    print(f"[PERF] {name}: {elapsed:.2f}s")


def collect(filename, url):
    os.makedirs(BRONZE_DIR, exist_ok=True)
    p_collect.collect_csv(filename, url)


def main():
    pipeline_start = time.perf_counter()

    os.makedirs(BRONZE_DIR, exist_ok=True)
    os.makedirs(SILVER_DIR, exist_ok=True)
    os.makedirs(GOLD_DIR, exist_ok=True)

    # Téléchargement parallèle (opendata.paris.fr)
    with timed_step("Bronze (collect)"):
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
    with timed_step("Silver (clean)"):
        dvf_bronze = BRONZE_DIR / "dvf.csv"
        dvf_silver = SILVER_DIR / "transactions_residentiel.csv"
        # DVF n'est pas téléchargé (fichier lourd) — on tolère un silver pré-existant
        if dvf_bronze.exists():
            clean_dvf(dvf_bronze, dvf_silver)
        elif dvf_silver.exists():
            print(f"[DVF] bronze/dvf.csv absent, silver pré-existant conservé.")
        else:
            print("[ERREUR] bronze/dvf.csv introuvable et aucun silver pré-existant.", file=sys.stderr)
            sys.exit(1)

        clean_logements_sociaux(BRONZE_DIR / "logement_sociaux.csv", SILVER_DIR / "logements_sociaux_programmes.csv")

        # dechets-menagers-pavda est vide sur opendata.paris.fr depuis fin 2025.
        # Si bronze vide (≤ header seul), on bascule sur le snapshot local.
        _bronze_abribac = BRONZE_DIR / "abribac_dechets_alimentaires.csv"
        _silver_abribac = SILVER_DIR / "abribac_dechets_alimentaires.csv"
        _snapshot_abribac = BRONZE_DIR / "abribac_snapshot.csv"
        if _bronze_abribac.stat().st_size > 100:
            clean_dechets_silver(_bronze_abribac, _silver_abribac)
        elif _snapshot_abribac.exists():
            shutil.copy(_snapshot_abribac, _silver_abribac)
            print("[ABRIBAC] Source upstream vide — snapshot 2025-11-21 utilisé en fallback")
        else:
            print("[ABRIBAC] Source upstream vide et snapshot introuvable", file=sys.stderr)
            sys.exit(1)

        clean_espaces_verts(BRONZE_DIR / "espace_verts.csv", SILVER_DIR / "espaces_verts_clean.csv")

    # Calcul des métriques Silver intermédiaires nécessaires aux indicateurs composites Gold
    with timed_step("Silver — Métriques agrégées"):
        compute_volume_transactions(
            src=dvf_silver,
            dst=SILVER_DIR / "volume_transactions.csv",
        )
        compute_surfaces_stats(
            src=dvf_silver,
            dst=SILVER_DIR / "surfaces_stats.csv",
        )

    # Gold CSV
    with timed_step("Gold CSV (aggregate)"):
        prix_m2_path = GOLD_DIR / "prix_m2_median.csv"
        variation_path = GOLD_DIR / "variation_prix_m2.csv"

        compute_prix_m2_median(src=dvf_silver, dst=prix_m2_path)
        compute_variation_prix_m2(src=prix_m2_path, dst=variation_path)

        compute_logements_sociaux_pct(
            logements_sociaux_src=SILVER_DIR / "logements_sociaux_programmes.csv",
            logements_residentiel_src=dvf_silver,
            dst=GOLD_DIR / "logements_sociaux_pct.csv",
        )
        # ITI agrège prix médian, variation et taux de logements sociaux en un indice territorial
        compute_iti(
            prix_src=GOLD_DIR / "prix_m2_median.csv",
            variation_src=GOLD_DIR / "variation_prix_m2.csv",
            sociaux_src=GOLD_DIR / "logements_sociaux_pct.csv",
            dst=GOLD_DIR / "iti.csv",
        )
        compute_typologie_parc(
            logements_src=dvf_silver,
            dst=GOLD_DIR / "typologie_parc.csv",
        )
        espaces_verts_silver_to_gold(SILVER_DIR / "espaces_verts_clean.csv")
        abribac_silver_to_gold()

        compute_diversite_typologique(
            src=GOLD_DIR / "typologie_parc.csv",
            dst=SILVER_DIR / "diversite_typologique.csv",
        )

        compute_iqv(
            espaces_src=GOLD_DIR / "espaces_verts_by_arr.csv",
            abribac_src=GOLD_DIR / "abribac_by_arr.csv",
            volume_src=SILVER_DIR / "volume_transactions.csv",
            dst=GOLD_DIR / "iqv.csv",
        )
        compute_iam(
            volume_src=SILVER_DIR / "volume_transactions.csv",
            diversite_src=SILVER_DIR / "diversite_typologique.csv",
            dst=GOLD_DIR / "iam.csv",
        )
        compute_ipr(
            volume_src=SILVER_DIR / "volume_transactions.csv",
            surfaces_src=SILVER_DIR / "surfaces_stats.csv",
            prix_src=GOLD_DIR / "prix_m2_median.csv",
            dst=GOLD_DIR / "ipr.csv",
        )

    # Vérification finale : tous les fichiers gold doivent exister et être non vides
    missing = [f for f in GOLD_EXPECTED if not (GOLD_DIR / f).exists() or (GOLD_DIR / f).stat().st_size == 0]
    if missing:
        print(f"[ERREUR] Fichiers gold manquants ou vides : {missing}", file=sys.stderr)
        sys.exit(1)

    print("Pipeline terminé, data/gold/ prêt")

    # Ingestion PostgreSQL
    pg_rows = 0
    with timed_step("Gold Postgres (ingest)"):
        from pipeline.db.load_gold_to_postgres import load_gold_to_postgres
        pg_rows = load_gold_to_postgres() or 0
    print("Pipeline terminé, BDD PostgreSQL prête")

    # Ingestion MongoDB
    mongo_rows = 0
    with timed_step("MongoDB (ingest)"):
        from pipeline.mongo_loader import load_mongo
        try:
            mongo_rows = load_mongo()
        except Exception as e:
            print(f"[MONGO] Avertissement : ingestion MongoDB échouée — {e}", file=sys.stderr)

    # Résumé de performance
    total_elapsed = time.perf_counter() - pipeline_start
    total_rows = pg_rows + mongo_rows
    print("\n[PERF] === Pipeline summary ===")
    for step_name, step_time in _perf_log:
        print(f"[PERF]   {step_name}: {step_time:.2f}s")
    print(f"[PERF] Total: {total_elapsed:.2f}s")
    if total_rows > 0 and total_elapsed > 0:
        print(f"[PERF] Throughput: {total_rows / total_elapsed:.0f} rows/s "
              f"({total_rows} rows PostgreSQL+MongoDB)")


if __name__ == "__main__":
    main()

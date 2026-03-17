"""
Démo micro-batch : surveille data/incoming/ et ingère automatiquement
tout nouveau CSV DVF déposé toutes les INTERVAL_SECONDS secondes.

Représente le pattern d'ingestion incrémentale qu'on remplacerait
par Kafka + Spark Streaming en production pour des flux temps réel.

Utilisation :
    DATABASE_URL=postgresql://urban:urban_pwd@localhost:5432/urban_data \
    python pipeline/micro_batch_demo.py

Puis déposer un CSV dans data/incoming/ pour déclencher l'ingestion.
"""

import os
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

INCOMING_DIR = Path("data/incoming")
PROCESSED_DIR = INCOMING_DIR / "processed"
INTERVAL_SECONDS = 10


# Crée la table de réception si elle n'existe pas encore, évite les erreurs au premier lancement
def ensure_table(engine) -> None:
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS incoming_dvf (
                id SERIAL PRIMARY KEY,
                ingested_at TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.commit()


# Charge le CSV, l'insère en append dans PostgreSQL, puis déplace le fichier dans processed/
def process_file(file_path: Path, engine) -> int:
    df = pd.read_csv(file_path)
    df.to_sql("incoming_dvf", engine, if_exists="append", index=False)
    n = len(df)
    print(f"[MICRO-BATCH] {file_path.name}: {n} lignes ingérées → incoming_dvf")
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    file_path.rename(PROCESSED_DIR / file_path.name)
    return n


# Boucle de surveillance : scanne le répertoire à intervalle fixe et traite chaque nouveau CSV
def main() -> None:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL non défini")

    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    engine = create_engine(db_url)
    ensure_table(engine)

    total_files = 0
    total_rows = 0

    print(f"[MICRO-BATCH] Watching {INCOMING_DIR}/ every {INTERVAL_SECONDS}s")
    print("[MICRO-BATCH] Déposer un CSV dans data/incoming/ pour déclencher l'ingestion")
    print("[MICRO-BATCH] Ctrl+C pour arrêter")

    try:
        while True:
            files = sorted(INCOMING_DIR.glob("*.csv"))
            if files:
                print(f"[MICRO-BATCH] {len(files)} nouveau(x) fichier(s) détecté(s)")
                for f in files:
                    n = process_file(f, engine)
                    total_files += 1
                    total_rows += n
                print(f"[MICRO-BATCH] Total cumulé : {total_files} fichier(s), {total_rows} lignes")
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print(f"\n[MICRO-BATCH] Arrêt — {total_files} fichier(s) traité(s), {total_rows} lignes ingérées")


if __name__ == "__main__":
    main()

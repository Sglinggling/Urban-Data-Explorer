import io
import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

GOLD_DIR = Path(__file__).resolve().parents[2] / "data" / "gold"

_LIBELLES = {
    1: "1er", 2: "2e", 3: "3e", 4: "4e", 5: "5e", 6: "6e", 7: "7e",
    8: "8e", 9: "9e", 10: "10e", 11: "11e", 12: "12e", 13: "13e",
    14: "14e", 15: "15e", 16: "16e", 17: "17e", 18: "18e", 19: "19e",
    20: "20e",
}


def _copy_df(cur, df: pd.DataFrame, table: str) -> None:
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=True)
    buf.seek(0)
    cur.copy_expert(f"COPY {table} FROM STDIN WITH CSV HEADER", buf)


def load_gold_to_postgres() -> None:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL non défini")

    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                # Tout effacer atomiquement (CASCADE vide les tables dépendantes)
                cur.execute("TRUNCATE arrondissements CASCADE")

                # 1. Arrondissements
                rows = [
                    (i, f"750{i:02d}", _LIBELLES[i]) for i in range(1, 21)
                ]
                execute_values(
                    cur,
                    "INSERT INTO arrondissements (arr_num, arr_insee, arr_libelle) VALUES %s",
                    rows,
                )

                # 2. prix_m2_median
                df = pd.read_csv(GOLD_DIR / "prix_m2_median.csv")
                df = df.rename(columns={"arrondissement": "arr_num", "prix_m2": "prix_m2_median"})
                df["arr_num"] = df["arr_num"].astype(int)
                df["annee"] = df["annee"].astype(int)
                _copy_df(cur, df[["arr_num", "annee", "prix_m2_median"]], "prix_m2_median")

                # 3. variation_prix_m2
                df = pd.read_csv(GOLD_DIR / "variation_prix_m2.csv")
                df = df.rename(columns={"arrondissement": "arr_num", "variation_%": "variation_prix_m2"})
                df["arr_num"] = df["arr_num"].astype(int)
                df["annee"] = df["annee"].astype(int)
                _copy_df(cur, df[["arr_num", "annee", "variation_prix_m2"]], "variation_prix_m2")

                # 4. logements_sociaux_pct
                df = pd.read_csv(GOLD_DIR / "logements_sociaux_pct.csv")
                df = df.rename(columns={
                    "arrondissement": "arr_num",
                    "logements_sociaux_pct": "pct_logements_sociaux",
                })
                df["arr_num"] = df["arr_num"].astype(int)
                _copy_df(cur, df[["arr_num", "pct_logements_sociaux"]], "logements_sociaux_pct")

                # 5. espaces_verts_by_arr
                df = pd.read_csv(GOLD_DIR / "espaces_verts_by_arr.csv")
                df["arr_num"] = df["arr_num"].astype(int)
                _copy_df(cur, df[["arr_num", "nb_espaces_verts", "surface_totale_m2"]], "espaces_verts_by_arr")

                # 6. education_par_arrondissement
                df = pd.read_csv(GOLD_DIR / "education_par_arrondissement.csv")
                df["arr_num"] = df["arr_num"].astype(int)
                _copy_df(
                    cur,
                    df[["arr_num", "nb_maternelles", "nb_elementaires", "nb_colleges", "nb_total_ecoles"]],
                    "education_par_arrondissement",
                )

                # 7. abribac_by_arr
                df = pd.read_csv(GOLD_DIR / "abribac_by_arr.csv")
                df = df.rename(columns={"arrondissement": "arr_num"})
                df["arr_num"] = df["arr_num"].astype(int)
                _copy_df(cur, df[["arr_num", "nb_abribacs"]], "abribac_by_arr")

                # 8. typologie_parc
                df = pd.read_csv(GOLD_DIR / "typologie_parc.csv")
                df = df.rename(columns={
                    "arrondissement": "arr_num",
                    "part_T2_pct": "part_t2_pct",
                    "part_T3plus_pct": "part_t3plus_pct",
                })
                df["arr_num"] = df["arr_num"].astype(int)
                df["annee"] = df["annee"].astype(int)
                _copy_df(
                    cur,
                    df[["arr_num", "annee", "part_studio_pct", "part_t2_pct", "part_t3plus_pct"]],
                    "typologie_parc",
                )

        report = {
            "arrondissements": None,
            "prix_m2_median": "(6 partitions)",
            "variation_prix_m2": "(5 partitions)",
            "logements_sociaux_pct": None,
            "espaces_verts_by_arr": None,
            "education_par_arrondissement": None,
            "abribac_by_arr": None,
            "typologie_parc": "(6 partitions)",
        }
        with conn.cursor() as cur:
            for table, extra in report.items():
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                n = cur.fetchone()[0]
                suffix = f" {extra}" if extra else ""
                print(f"[DB] {table}: {n} rows{suffix}")

        print("[DB] Ingestion terminée")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

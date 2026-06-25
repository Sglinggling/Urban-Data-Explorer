## Choix de la base de données Gold

### Décision : PostgreSQL 16

### Justification

Le projet manipule des données :
- Tabulaires régulières (arrondissement × année × indicateur)
- Petits volumes mais croissance prévisible
- Requêtes analytiques (filtres, agrégations, comparaisons)
- Avec dimension géospatiale (coordonnées, polygones)

PostgreSQL répond à tous ces besoins avec :
- Modèle relationnel adapté à la régularité de nos données
- Expressivité SQL pour les requêtes analytiques
- Support natif des données géospatiales via PostGIS
- Standard industrie, écosystème mature

NoSQL (MongoDB) a été écarté pour la couche Gold car nos données
sont structurées de façon régulière sans avoir besoin de schéma
flexible. MongoDB est utilisé pour la couche géospatiale (entités
individuelles géolocalisées) — voir section dédiée ci-dessous.

DuckDB a été considéré pour ses performances analytiques mais
écarté car il est embarqué (mono-machine), donc non scalable
horizontalement par sharding — critère central de notre cahier
des charges.

### Scalabilité

Les tables temporelles (prix, variation, typologie) sont
partitionnées par année via `PARTITION BY RANGE (annee)`.

Bénéfices :
- Requêtes filtrant sur une année ne scannent qu'une partition
- Ingestion incrémentale d'une nouvelle année sans toucher
  les anciennes données (`ALTER TABLE ... ATTACH PARTITION`)
- Possibilité de stocker les partitions anciennes sur du
  stockage moins cher (tiering)
- Suppression rapide de données obsolètes (`DROP PARTITION`
  au lieu de `DELETE`)

Pour passer à l'échelle horizontale (millions d'enregistrements,
ex : extension à toute l'Île-de-France ou à plusieurs villes),
on peut activer l'extension Citus qui transforme PostgreSQL en
base distribuée avec sharding automatique :
- Sharding par hash sur `arr_num` pour distribuer la charge géographique
- Replication factor 2 pour la haute disponibilité
- Coordinator + workers pour parallélisation des requêtes

### Schéma

```
arrondissements (arr_num PK, arr_insee, arr_libelle)
       |
       +──── logements_sociaux_pct    (arr_num FK, pct_logements_sociaux)
       +──── espaces_verts_by_arr     (arr_num FK, nb_espaces_verts, surface_totale_m2)
       +──── abribac_by_arr           (arr_num FK, nb_abribacs)
       |
       +──── iti                 [PARTITIONNÉ par annee]
       |        └── _2022, _2023, _2024, _2025
       |
       +──── prix_m2_median      [PARTITIONNÉ par annee]
       |        └── _2020, _2021, _2022, _2023, _2024, _2025
       |
       +──── variation_prix_m2   [PARTITIONNÉ par annee]
       |        └── _2021, _2022, _2023, _2024, _2025
       |
       +──── typologie_parc      [PARTITIONNÉ par annee]
                └── _2020, _2021, _2022, _2023, _2024, _2025
```

---

## Indicateur composé : Indice de Tension Immobilière (ITI)

### Objectif

Mesurer la difficulté d'accès au logement par arrondissement
en fusionnant 3 dimensions : prix, dynamique de prix, et
amortisseur social.

### Formule

```
ITI = 0.5 × prix_m2_norm
    + 0.3 × variation_norm
    + 0.2 × (100 - logements_sociaux_pct_norm)
```

Toutes les composantes sont normalisées min-max sur les 20
arrondissements pour chaque année (échelle 0-100).

### Justification des poids

- 50% pour le prix : facteur dominant de l'accès au logement
- 30% pour la variation : capture la dynamique (un quartier
  qui s'embourgeoise est plus tendu même si le prix absolu
  reste bas)
- 20% pour l'inverse du % social : un quartier avec beaucoup
  de logements sociaux offre un amortisseur qui réduit la
  tension sur le marché privé

### Période

2022-2025. 2020-2021 exclus car la couverture des
arrondissements est partielle (rendrait la normalisation
non comparable entre années).

### Interprétation

| Plage ITI | Niveau de tension | Exemples (2024) |
|---|---|---|
| > 70 | Forte | 7e (96.2), 6e (83.4), 8e (76.6) |
| 40-70 | Moyenne | 11e (46.1), 9e (47.2), 16e (47.4) |
| < 40 | Faible | 18e (16.6), 20e (12.8), 19e (3.4) |

### Limites

- Les poids sont des choix méthodologiques discutables
- N'intègre pas le revenu médian local (donnée non
  disponible dans notre périmètre)
- La normalisation par année rend les comparaisons
  inter-annuelles indicatives (ITI 2022 ≠ ITI 2024 en
  valeur absolue, seul le classement interne est stable)

---

## KPIs de performance du pipeline

Le pipeline mesure chaque étape via `timed_step()` (contextmanager
`time.perf_counter`) et affiche un résumé en fin d'exécution.

```
[PERF] Bronze (collect):        ~30s  (téléchargements parallèles)
[PERF] Silver (clean):          ~5s   (nettoyage CSV)
[PERF] Gold CSV (aggregate):    ~10s  (agrégations pandas)
[PERF] Gold Postgres (ingest):  ~3s   (COPY bulk psycopg2)
[PERF] MongoDB (ingest):        ~1s   (insert_many pymongo)
[PERF] Total:                   ~50s
[PERF] Throughput:              ~XX rows/s (PostgreSQL + MongoDB cumulés)
```

Le throughput est calculé comme `(pg_rows + mongo_rows) / total_elapsed`.

---

## Résilience aux pannes

Le script `docs/test_resilience.sh` valide la dégradation gracieuse
et la récupération automatique lors d'une panne PostgreSQL.

Scénario testé :

1. État initial : tous les services UP → GET /api/prix renvoie 200
2. `docker compose stop postgres` → GET /api/prix renvoie 500
3. `docker compose start postgres` + attente healthcheck → GET /api/prix renvoie 200

La récupération est automatique grâce au connection pool SQLAlchemy
qui reconnaît les connexions valides dès que PostgreSQL est à nouveau
disponible, sans redémarrage de l'API.

```bash
bash docs/test_resilience.sh
```

---

## Streaming et micro-batch

`pipeline/micro_batch_demo.py` démontre le pattern d'ingestion
incrémentale (micro-batch) : surveillance d'un répertoire entrant
et ingestion automatique de tout nouveau CSV DVF.

Commande :

```bash
DATABASE_URL=postgresql://urban:urban_pwd@localhost:5432/urban_data \
python pipeline/micro_batch_demo.py
```

Puis déposer un CSV dans `data/incoming/` pour déclencher l'ingestion.
Les fichiers traités sont déplacés dans `data/incoming/processed/`.

En production, ce pattern serait remplacé par **Kafka + Spark Streaming**
pour des flux temps réel avec des garanties de livraison (at-least-once /
exactly-once), gestion des retards de messages (watermarking), et
scalabilité horizontale des consumers.

---

## Couche Silver — Métriques agrégées (indices composés)

Ces trois fichiers Silver sont produits après le nettoyage DVF et
avant le calcul des indices Gold IQV/IAM/IPR. Ils centralisent les
agrégations brutes réutilisables par plusieurs scripts Gold.

### `data/silver/volume_transactions.csv`

**Script producteur :** `pipeline/clean/volume_transactions_to_silver.py`
**Source :** `data/silver/transactions_residentiel.csv`

| Colonne | Type | Description |
|---|---|---|
| annee | int | Année (2020-2025) |
| arr_num | int | Numéro d'arrondissement (1-20) |
| nb_transactions | int | Nombre de ventes résidentielles |
| surface_bati_totale | float | Somme des surfaces bâties (m²) |

**Utilisé par :** IQV (proxy densité), IAM (volume + variation), IPR (densité transactions)

### `data/silver/surfaces_stats.csv`

**Script producteur :** `pipeline/clean/surfaces_stats_to_silver.py`
**Source :** `data/silver/transactions_residentiel.csv`

| Colonne | Type | Description |
|---|---|---|
| annee | int | Année (2020-2025) |
| arr_num | int | Numéro d'arrondissement (1-20) |
| surface_mediane | float | Surface médiane des biens (m²) |
| surface_mean | float | Surface moyenne des biens (m²) |

**Utilisé par :** IPR (composante surface médiane)

### `data/silver/diversite_typologique.csv`

**Script producteur :** `pipeline/clean/diversite_typologique_to_silver.py`
**Source :** `data/gold/typologie_parc.csv`

| Colonne | Type | Description |
|---|---|---|
| annee | int | Année (2020-2025) |
| arr_num | int | Numéro d'arrondissement (1-20) |
| ecart_type_parts | float | Écart-type des 3 parts (studio/T2/T3+) |

**Logique :** un écart-type bas signifie un parc équilibré (diversifié) ;
l'inversion se fait en Gold (`diversite_norm = 100 - ecart_type_norm`).
**Utilisé par :** IAM (composante diversité typologique)

### Ordre d'exécution dans main.py

```
Bronze (collect)
  └── Silver DVF, logements_sociaux, espaces_verts, abribac
        └── Silver métriques : volume_transactions, surfaces_stats
              └── Gold : prix_m2_median, variation, logements_sociaux_pct, iti, typologie_parc
                    └── Silver : diversite_typologique (lit gold/typologie_parc)
                          └── Gold indices composés : IQV, IAM, IPR
```

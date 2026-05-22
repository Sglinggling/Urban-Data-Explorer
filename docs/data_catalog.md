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

NoSQL (MongoDB) a été écarté car nos données sont structurées
de façon régulière sans avoir besoin de schéma flexible.

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
       +──── education_par_arr        (arr_num FK, nb_maternelles, nb_elementaires, ...)
       +──── abribac_by_arr           (arr_num FK, nb_abribacs)
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

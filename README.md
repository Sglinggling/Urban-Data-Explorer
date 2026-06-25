# 🌆 Urban Data Explorer

[![Python 3.11](https://img.shields.io/badge/Python-3.11-blue.svg?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg?logo=docker&logoColor=white)](https://www.docker.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-6.0-47A248.svg?logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![Frontend](https://img.shields.io/badge/Frontend-MapLibre_GL_%26_Chart.js-orange.svg?logo=javascript&logoColor=white)](#frontend)
[![Pair Programming](https://img.shields.io/badge/Pair_Programming-Samy_HALIT_%26_Ananda-brightgreen.svg?logo=github)](https://github.com/Sglinggling)

Tableau de bord décisionnel interactif permettant de visualiser le marché immobilier parisien croisé avec des indicateurs de cadre de vie (logements sociaux, espaces verts, écoles, points de collecte des biodéchets) par arrondissement.

---

## 🏗️ Architecture Technique

Le projet repose sur un pipeline de données à trois étapes (Bronze → Silver → Gold), stocké en base relationnelle PostgreSQL et document MongoDB, servi par une API FastAPI sécurisée, et rendu sur un tableau de bord dynamique Nginx/MapLibre.

```mermaid
graph TD
    %% Source Data
    subgraph DataSources [Sources de Données]
        ODP["opendata.paris.fr"]
        DVF["Demandes de Valeurs Foncières (DVF)"]
    end

    %% Pipeline Stage
    subgraph PipelineETL [Pipeline ETL - Python 3.11]
        Bronze["Bronze: Fichiers Bruts"]
        Silver["Silver: Nettoyage & Standardisation"]
        Gold["Gold: Calcul des Indicateurs"]
        Bronze -->|clean_data.py| Silver
        Silver -->|metrics_engine.py| Gold
    end

    ODP -->|collect_data.py| Bronze
    DVF -->|Manuel| Bronze

    %% Storage Stage
    subgraph Storage [Stockage & Orchestration]
        Postgres[("PostgreSQL (Métriques Gold)")]
        MongoDB[("MongoDB (Documents Non-relationnels)")]
        LocalGold["Gold CSVs (Volume Docker)"]
    end

    Gold -->|load_gold_to_postgres.py| Postgres
    Gold -->|mongo_loader.py| MongoDB
    Gold --> LocalGold

    %% API Backend Stage
    subgraph BackendAPI [FastAPI Backend]
        Auth["Middleware Key Auth"]
        Limiter["SlowAPI Rate Limiter"]
        Endpoints["Endpoints API /api/*"]
        Auth --> Limiter --> Endpoints
    end

    Postgres --> Endpoints
    MongoDB --> Endpoints
    LocalGold --> Endpoints

    %% Client Frontend Stage
    subgraph FrontendService [Nginx Frontend]
        Index["index.html & style.css"]
        Map["Carte Choroplèthe MapLibre GL"]
        Charts["Graphiques Analytiques Chart.js"]
    end

    Endpoints -->|Fetch API (CORS + API Key)| Map
    Endpoints -->|Fetch API (CORS + API Key)| Charts
```

---

## 📂 Organisation du Projet

```
.
├── api/                       # Service Backend FastAPI
│   ├── app.py                 # Point d'entrée de l'application ASGI
│   ├── auth.py                # Middleware de validation X-API-Key
│   ├── db.py                  # Connecteur PostgreSQL & MongoDB
│   ├── endpoints.py           # Routes REST (/prix, /logements_sociaux, /kpis...)
│   ├── limiter.py             # Configuration du Rate Limiter (SlowAPI)
│   ├── requirements.txt       # Dépendances API
│   └── Dockerfile             # Dockerfile de production
├── frontend/                  # Service Web Frontend
│   ├── index.html             # Structure du Dashboard
│   ├── style.css              # Feuille de style personnalisée (Thèmes Sombre/Clair)
│   ├── nginx.conf             # Configuration de Nginx servant les statiques
│   ├── js/
│   │   ├── main.js            # Initialisation des graphiques Chart.js et thèmes
│   │   └── map.js             # Rendu géographique de la carte MapLibre GL
│   ├── data/
│   │   └── arrondissements.geojson  # Découpage géographique de Paris
│   └── Dockerfile
├── pipeline/                  # Pipeline d'ingestion ETL
│   ├── collect/
│   │   └── collect_data.py    # Ingestion automatisée depuis OpenData Paris
│   ├── clean/                 # Transformation Bronze -> Silver (Nettoyage)
│   │   ├── dvf_to_silver.py
│   │   ├── clean_data_to_silver_abribac.py
│   │   ├── clean_data_to_silver_espaces_verts.py
│   │   ├── diversite_typologique_to_silver.py
│   │   ├── logements_sociaux_to_silver.py
│   │   └── surfaces_stats_to_silver.py
│   ├── gold/                  # Aggregats Silver -> Gold (Calcul de KPIs)
│   │   ├── dvf_gold.py
│   │   ├── abribac_to_gold.py
│   │   ├── espaces_verts_to_gold.py
│   │   ├── iam_to_gold.py     # Indice d'Attractivité Municipale
│   │   ├── ipr_to_gold.py     # Indice de Proximité Résidentielle
│   │   ├── iqv_to_gold.py     # Indice de Qualité de Vie
│   │   └── logement_gold.py
│   ├── db/
│   │   ├── init.sql           # Schéma relationnel SQL
│   │   └── load_gold_to_postgres.py  # Script de chargement PostgreSQL
│   ├── mongo_loader.py        # Script de chargement MongoDB NoSQL
│   ├── Dockerfile
│   └── requirements.txt
├── data/                      # Volumes locaux de données partagés
│   ├── bronze/                # Données brutes
│   ├── silver/                # Données nettoyées de niveau intermédiaire
│   └── gold/                  # Vue consolidée prête-à-servir
├── main.py                    # Script principal d'orchestration ETL
├── docker-compose.yml         # Fichier d'orchestration multi-services Docker
└── README.md                  # Documentation du projet
```

---

## ⚡ Lancement Rapide (Docker)

### Prérequis
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installé et en cours d'exécution.
- Connexion Internet active (nécessaire au premier démarrage pour l'ETL).

### Démarrage global

Exécutez la commande suivante à la racine :

```bash
docker compose up --build
```

Au premier démarrage :
1. Le container `pipeline` démarre en mode one-shot.
2. Il récupère les jeux de données bruts sur OpenData Paris, effectue les transformations, charge les tables dans PostgreSQL / MongoDB et génère les CSV locaux dans `data/gold`.
3. Une fois le pipeline terminé avec succès (environ 3 à 5 minutes), les conteneurs `api`, `frontend`, `postgres`, et `mongodb` s'activent de manière transparente.

Accédez ensuite au Dashboard sur : **[http://localhost:5500](http://localhost:5500)**

Pour arrêter l'environnement :
```bash
docker compose down -v
```

---

## 🔒 Endpoints de l'API & Securité

Toutes les requêtes de l'API (sauf `/api/ping`) nécessitent une clé API valide dans le header HTTP `X-API-Key`.
- **Clé de développement par défaut :** `urban-data-explorer-dev-key`
- **Rate Limit :** Les endpoints de consultation sont limités à un maximum de **60 requêtes/minute** par adresse IP cliente.

### Échantillon d'Endpoints disponibles :

*   `GET /api/prix` : Évolution temporelle et distribution des prix/m² immobiliers.
*   `GET /api/logements_sociaux` : Statistiques de la répartition des logements sociaux par rapport aux objectifs légaux.
*   `GET /api/espaces_verts` : Surfaces d'espaces verts (m²) et ratios par habitant.
*   `GET /api/kpis` : Récupération des trois indices personnalisés consolidés :
    *   **IQV (Indice de Qualité de Vie)** : Poids des espaces verts et des infrastructures de tri par habitant.
    *   **IPR (Indice de Proximité Résidentielle)** : Ratio de commerces et de bacs de tri à moins de 500m.
    *   **IAM (Indice d'Attractivité Municipale)** : Mixité d'accès aux services publics et dynamisme foncier.

Exemple d'appel avec curl :
```bash
curl -H "X-API-Key: urban-data-explorer-dev-key" \
  "http://localhost:8000/api/prix?annee=2024&arrondissement=18"
```

---

## 💻 Mode Développement (sans Docker)

Pour exécuter les services de manière isolée en local :

1.  **Exécuter le pipeline de données :**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r pipeline/requirements.txt
    python3 main.py
    ```

2.  **Lancer l'API FastAPI :**
    ```bash
    cd api
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    uvicorn app:app --host 0.0.0.0 --port 8000 --reload
    ```

3.  **Lancer le Frontend Nginx / Serveur Web local :**
    ```bash
    cd frontend
    python3 -m http.server 5500
    ```

---

## 👥 Binôme et Auteurs
Ce projet a été réalisé en pair programming par :
- **Samy HALIT** ([@Sglinggling](https://github.com/Sglinggling))
- **Ananda** ([@ananda3cassini](https://github.com/ananda3cassini))

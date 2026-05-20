# Urban Data Explorer

Tableau de bord interactif du marché immobilier parisien — prix/m², logements sociaux, espaces verts, écoles et abribacs par arrondissement.

## Quick Start

Prérequis : Docker Desktop installé et lancé, connexion internet.

```bash
docker compose up --build
```

Au premier lancement, le service `pipeline` télécharge les données depuis opendata.paris.fr et génère les CSV Gold (5 à 10 minutes selon la connexion). Les lancements suivants sont instantanés car les données sont déjà présentes.

Puis ouvre http://localhost:5500

Pour relancer le pipeline (rafraîchir les données) :

```bash
docker compose run --rm pipeline
```

Pour arrêter : `Ctrl+C` puis `docker compose down`

---

## Architecture

```
.
├── api/               # Service FastAPI (Python 3.11)
│   ├── app.py         # Application ASGI + CORS
│   ├── endpoints.py   # Routes /prix, /timeline, /logements_sociaux…
│   └── Dockerfile
├── frontend/          # Service nginx (assets statiques)
│   ├── index.html
│   ├── js/
│   │   ├── main.js    # Graphiques Chart.js
│   │   └── map.js     # Carte choroplèthe MapLibre GL
│   ├── style.css
│   ├── data/          # GeoJSON arrondissements parisiens
│   ├── nginx.conf
│   └── Dockerfile
├── pipeline/          # Scripts Bronze → Silver → Gold
│   ├── collect/       # Téléchargement opendata.paris.fr
│   ├── clean/         # Bronze → Silver
│   ├── gold/          # Silver → Gold
│   └── Dockerfile
├── data/
│   └── gold/          # CSV prêts-à-servir (monté en volume :ro dans Docker)
├── main.py            # Orchestrateur pipeline complet
└── docker-compose.yml
```

## Services Docker

| Service    | Image base        | Port host | Port container |
|------------|-------------------|-----------|----------------|
| `pipeline` | python:3.11-slim  | —         | —              |
| `api`      | python:3.11-slim  | 8000      | 8000           |
| `frontend` | nginx:alpine      | 5500      | 80             |

Le service `pipeline` s'exécute en one-shot avant l'API (condition `service_completed_successfully`). Le volume `./data:/app/data` est partagé en lecture-écriture pour le pipeline, et en lecture seule pour l'API.

### Note sur les données DVF

Le fichier `data/bronze/dvf.csv` (Demandes de Valeurs Foncières) n'est pas téléchargé automatiquement. Si ce fichier est absent, le pipeline conserve les données silver/gold pré-existantes dans le dépôt. Pour régénérer les données de prix depuis une nouvelle source DVF, place le fichier dans `data/bronze/dvf.csv` avant de lancer le pipeline.

## Développement sans Docker

```bash
# Pipeline
python main.py

# API
cd api
pip install -r requirements.txt
uvicorn api.app:app --host 0.0.0.0 --port 8000 --reload

# Frontend (depuis la racine)
cd frontend && python -m http.server 5500
```

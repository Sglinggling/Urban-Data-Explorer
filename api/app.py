from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from . import endpoints # Assurez-vous d'importer votre routeur

app = FastAPI(title="Urban Data Explorer API")

# --- Configuration CORS ---
# L'origine à autoriser est celle où votre page HTML s'exécute.
# Si vous ouvrez le fichier index.html directement, l'origine est "null" ou "file://".
# Si vous utilisez un serveur de développement (souvent sur le port 8000), elle sera "http://localhost:port".

origins = [
    # Si vous ouvrez index.html via un serveur de développement local :
    "http://localhost",
    "http://localhost:8080", # Exemple de port souvent utilisé
    "http://127.0.0.1:8080",
    
    # Pour autoriser n'importe quelle origine pendant le développement (moins sûr, mais simple)
    "*" 
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins, # Liste des origines autorisées (ex: "http://localhost:8080")
    allow_credentials=True,
    allow_methods=["*"], # Autorise toutes les méthodes (GET, POST, etc.)
    allow_headers=["*"], # Autorise tous les headers
)

# Inclusion des routes
app.include_router(endpoints.router, prefix="/api")

# Vous pouvez laisser le endpoint /ping ici ou dans endpoints.py
@app.get("/")
def read_root():
    return {"message": "Bienvenue sur l'API Urban Data Explorer"}
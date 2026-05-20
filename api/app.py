from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from . import endpoints # Assurez-vous d'importer votre routeur

app = FastAPI(title="Urban Data Explorer API")

origins = [
    "http://localhost:5500",       # frontend Docker (nginx)
    "http://127.0.0.1:5500",
    "http://localhost:8080",       # dev local classique
    "http://127.0.0.1:8080",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "null",                  
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,       # pas de cookies/sessions
    allow_methods=["*"],
    allow_headers=["*"],
)

# Inclusion des routes
app.include_router(endpoints.router, prefix="/api")

# Vous pouvez laisser le endpoint /ping ici ou dans endpoints.py
@app.get("/")
def read_root():
    return {"message": "Bienvenue sur l'API Urban Data Explorer"}
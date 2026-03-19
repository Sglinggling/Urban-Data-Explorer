"""
Point d'entrée de l'API FastAPI Urban Data Explorer.
Configure le middleware CORS, la limitation de débit (SlowAPI) et monte le routeur des endpoints.
Entrée : requêtes HTTP  →  Sortie : réponses JSON via les routes /api/*
"""
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from slowapi.errors import RateLimitExceeded
from slowapi import _rate_limit_exceeded_handler

from . import endpoints
from .db import engine  # initialise le pool SQLAlchemy au démarrage
from .limiter import limiter

app = FastAPI(title="Urban Data Explorer API")

# Rattache le limiteur de débit à l'état de l'application et enregistre le gestionnaire d'erreur 429
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

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

@app.get("/")
@limiter.limit("5/minute")
def read_root(request: Request):
    return {"message": "Bienvenue sur l'API Urban Data Explorer"}

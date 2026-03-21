"""
Middleware d'authentification pour l'API Urban Data Explorer.
Vérifie la clé API transmise dans l'en-tête HTTP X-API-Key.
Entrée : header HTTP  →  Sortie : clé validée ou exception 401
"""
import os

from fastapi import Header, HTTPException

API_KEY = os.getenv("API_KEY", "urban-data-explorer-dev-key")


# Dépendance FastAPI injectée sur les routes protégées : rejette toute requête sans clé valide
def verify_api_key(x_api_key: str = Header(..., alias="X-API-Key")):
    if x_api_key != API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing X-API-Key header",
        )
    return x_api_key

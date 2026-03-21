"""
Initialisation du moteur de connexion PostgreSQL via SQLAlchemy.
Entrée : variable d'environnement DATABASE_URL  →  Sortie : engine SQLAlchemy utilisé par les routes FastAPI.
"""
import os
from sqlalchemy import create_engine

_url = os.environ.get("DATABASE_URL", "")

# Crée le moteur uniquement si l'URL est définie, avec détection automatique des connexions mortes
engine = create_engine(_url, pool_pre_ping=True) if _url else None

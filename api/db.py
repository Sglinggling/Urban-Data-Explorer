import os
from sqlalchemy import create_engine

_url = os.environ.get("DATABASE_URL", "")
engine = create_engine(_url, pool_pre_ping=True) if _url else None

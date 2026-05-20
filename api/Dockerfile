FROM python:3.11-slim

WORKDIR /app

# Dépendances en couche dédiée (cache Docker)
COPY api/requirements.txt ./api/requirements.txt
RUN pip install --no-cache-dir -r api/requirements.txt

# Code API uniquement (data/ est monté en volume)
COPY api/ ./api/

EXPOSE 8000

CMD ["uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8000"]

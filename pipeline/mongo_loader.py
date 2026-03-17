"""
Chargement des points abribacs/PAVDA dans MongoDB avec index géospatial 2dsphere.
Lit depuis data/silver/abribac_dechets_alimentaires.csv (colonnes lon/lat issues
du nettoyage silver ou du snapshot 2025-11-21 en fallback).
"""

import os
from pathlib import Path

import pandas as pd
from pymongo import MongoClient, GEOSPHERE


def load_mongo() -> int:
    # Insère tous les points abribacs dans MongoDB et crée l'index géospatial ; retourne le nombre de documents chargés
    mongo_url = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
    silver_path = Path("data/silver/abribac_dechets_alimentaires.csv")

    client = MongoClient(mongo_url, serverSelectionTimeoutMS=5000)
    db = client["urban_data"]
    col = db["abribacs"]

    df = pd.read_csv(silver_path)

    # Vérifie la présence des colonnes indispensables avant toute transformation
    required = {"pavda_id", "longitude", "latitude", "arrondissement"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        raise ValueError(f"[MONGO] Colonnes manquantes dans silver: {missing}")

    df = df.dropna(subset=["longitude", "latitude", "arrondissement"])
    df["arrondissement"] = df["arrondissement"].astype(int)
    df["longitude"] = df["longitude"].astype(float)
    df["latitude"] = df["latitude"].astype(float)

    col.drop()

    # Construit les documents GeoJSON Point (format attendu par l'index 2dsphere : [lon, lat])
    docs = [
        {
            "pavda_id": row["pavda_id"],
            "arrondissement": int(row["arrondissement"]),
            "location": {
                "type": "Point",
                "coordinates": [row["longitude"], row["latitude"]],
            },
        }
        for _, row in df.iterrows()
    ]

    if docs:
        col.insert_many(docs)

    # Index 2dsphere requis pour les requêtes $near et $geoWithin
    col.create_index([("location", GEOSPHERE)])

    n = col.count_documents({})
    print(f"[MONGO] abribacs: {n} documents + index 2dsphere")
    client.close()
    return n

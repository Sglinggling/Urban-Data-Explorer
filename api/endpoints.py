from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

# On crée un router pour regrouper tous les endpoints de l'API
router = APIRouter()

# CHEMINS DATA #

ROOT = Path(__file__).resolve().parents[1]  # dossier Urban-Data-Explorer
GOLD = ROOT / "data" / "gold"


def load_csv_gold(filename: str) -> pd.DataFrame:
    """
    Charge un CSV depuis la zone GOLD. Lève une erreur HTTP 500 si le fichier est manquant.
    """
    path = GOLD / filename
    if not path.exists():
        raise HTTPException(
            status_code=500,
            detail=f"Fichier GOLD introuvable : {path}.",
        )
    return pd.read_csv(path)


# Endpoint de test  #

@router.get("/ping")
def ping():
    """
    Petit endpoint de test : /api/ping
    """
    return {"status": "ok", "message": "API /api/ping répond bien"}


# INDICATEUR 1 : PRIX / m² #

@router.get("/arrondissements")
def list_arrondissements():
    """
    Renvoie la liste des arrondissements présents dans les données GOLD de prix.
    """
    df = load_csv_gold("prix_m2_median.csv")
    arrs = sorted(df["arrondissement"].dropna().unique().tolist())
    return {"arrondissements": arrs}


@router.get("/prix")
def get_prix(
    annee: Optional[int] = Query(None, description="Année (ex: 2020)"),
    arrondissement: Optional[int] = Query(None, description="Arrondissement (1-20)"),
):
    """
    Retourne le prix/m² médian par arrondissement et par année (zone GOLD).

    - Sans filtre : tous les arrondissements, toutes les années.
    - Avec annee : tous les arrondissements pour cette année.
    - Avec arrondissement : toutes les années pour cet arrondissement.
    """
    df = load_csv_gold("prix_m2_median.csv")

    if annee is not None:
        df = df[df["annee"] == annee]

    if arrondissement is not None:
        df = df[df["arrondissement"] == arrondissement]

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée pour ces filtres (annee / arrondissement).",
        )

    # On renomme prix_m2 -> prix_m2_median
    df = df.rename(columns={"prix_m2": "prix_m2_median"})
    df = df.sort_values(["annee", "arrondissement"])

    return df.to_dict(orient="records")


@router.get("/timeline")
def get_timeline(
    arr: int = Query(..., description="Numéro d'arrondissement (1-20)"),
):
    """
    Retourne l'évolution du prix/m² (et variation %) pour un arrondissement,
    année par année, à partir de la table GOLD de variation.
    """
    df = load_csv_gold("variation_prix_m2.csv")

    df = df[df["arrondissement"] == arr].copy()

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Aucune donnée pour l'arrondissement {arr}.",
        )

    # On renomme pour avoir des clés propres côté frontend
    df = df.rename(
        columns={
            "prix_m2": "prix_m2_median",
            "prix_m2_prec": "prix_m2_prec_median",
            "variation_%": "variation_pct",
        }
    )
    df = df.sort_values("annee")

    return {
        "arrondissement": arr,
        "timeline": df[["annee", "prix_m2_median", "prix_m2_prec_median", "variation_pct"]].to_dict(
            orient="records"
        ),
    }


@router.get("/comparaison")
def comparaison(
    arr1: int = Query(..., description="Premier arrondissement (1-20)"),
    arr2: int = Query(..., description="Deuxième arrondissement (1-20)"),
    annee: Optional[int] = Query(None, description="Année (facultative, ex: 2022)"),
):
    """
    Compare deux arrondissements sur le prix/m² médian (zone GOLD).

    - Si 'annee' est précisée : comparaison pour cette année uniquement.
    - Sinon : renvoie la série de toutes les années pour les deux arrondissements.
    """
    df = load_csv_gold("prix_m2_median.csv")

    df = df[df["arrondissement"].isin([arr1, arr2])]

    if annee is not None:
        df = df[df["annee"] == annee]

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée pour cette comparaison (arr1 / arr2 / annee).",
        )

    df = df.rename(columns={"prix_m2": "prix_m2_median"})
    df = df.sort_values(["annee", "arrondissement"])

    return {
        "arr1": arr1,
        "arr2": arr2,
        "annee": annee,
        "data": df.to_dict(orient="records"),
    }


# INDICATEUR 2 : LOGEMENTS SOCIAUX #

@router.get("/logements_sociaux")
def logements_sociaux(
    arrondissement: Optional[int] = Query(None, description="Arrondissement (1-20, optionnel)"),
):
    """
    Part de logements sociaux (%) par arrondissement (zone GOLD).
    """
    df = load_csv_gold("logements_sociaux_pct.csv")

    if arrondissement is not None:
        df = df[df["arrondissement"] == arrondissement]

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée logements sociaux pour ces filtres.",
        )

    return df.to_dict(orient="records")


# INDICATEUR 3 : TYPOLOGIE DU PARC #

@router.get("/typologie")
def typologie(
    arrondissement: Optional[int] = Query(None, description="Arrondissement (1-20, optionnel)"),
    annee: Optional[int] = Query(None, description="Année (optionnelle)"),
):
    """
    Typologie du parc immobilier (part de studios, T2, T3+) par arrondissement et par année.
    """
    df = load_csv_gold("typologie_parc.csv")

    # La colonne 'annee' est float dans le CSV → on la cast en int pour les filtres
    if "annee" in df.columns:
        df["annee"] = df["annee"].astype("Int64")

    if arrondissement is not None:
        df = df[df["arrondissement"] == arrondissement]

    if annee is not None:
        df = df[df["annee"] == annee]

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée de typologie pour ces filtres.",
        )

    return df.to_dict(orient="records")


# INDICATEUR 4 : ESPACES VERTS #

@router.get("/espaces_verts")
def espaces_verts(
    arrondissement: Optional[int] = Query(None, description="Arrondissement (1-20, optionnel)"),
):
    """
    Espaces verts par arrondissement (nombre et surface totale).
    """
    df = load_csv_gold("espaces_verts_by_arr.csv")

    # arr_num est un float dans le CSV → on le cast en int et renomme en 'arrondissement'
    df["arrondissement"] = df["arr_num"].astype("Int64")
    df = df.drop(columns=["arr_num"])

    if arrondissement is not None:
        df = df[df["arrondissement"] == arrondissement]

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée d'espaces verts pour ces filtres.",
        )

    return df.to_dict(orient="records")


# INDICATEUR 5 : ÉTABLISSEMENTS SCOLAIRES #

@router.get("/etablissements_scolaires")
def etablissements_scolaires(
    arrondissement: Optional[int] = Query(None, description="Arrondissement (1-20, optionnel)"),
):
    """
    Établissements scolaires par arrondissement :
    - nb de maternelles
    - nb d'élémentaires
    - nb de collèges
    - total d'écoles
    """
    df = load_csv_gold("education_par_arrondissement.csv")

    # arr_num -> arrondissement
    df["arrondissement"] = df["arr_num"].astype("Int64")
    df = df.drop(columns=["arr_num"])

    if arrondissement is not None:
        df = df[df["arrondissement"] == arrondissement]

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée d'établissements scolaires pour ces filtres.",
        )

    return df.to_dict(orient="records")


# INDICATEUR 6 : ABRIBACS / PAVDA #

@router.get("/abribacs")
def abribacs(
    arrondissement: Optional[int] = Query(None, description="Arrondissement (1-20, optionnel)"),
):
    """
    Points d'Apport Volontaire de Déchets Alimentaires (PAVDA) par arrondissement (nombre).
    """
    df = load_csv_gold("abribac_by_arr.csv")

    # arr_num est un float dans le CSV → on le cast en int et renomme en 'arrondissement'
    if "arr_num" in df.columns:
        df["arrondissement"] = df["arr_num"].astype("Int64")
        df = df.drop(columns=["arr_num"])

    if arrondissement is not None:
        df = df[df["arrondissement"] == arrondissement]

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée Abribac/PAVDA pour ces filtres.",
        )

    return df.to_dict(orient="records")
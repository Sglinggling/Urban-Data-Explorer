"""
Points d'entrée REST de l'API Urban Data Explorer.
Expose les indicateurs immobiliers et urbains de Paris (prix, logements sociaux, espaces verts, indices composites).
Entrée : requêtes HTTP avec filtres optionnels (annee, arrondissement)  →  Sortie : JSON paginable par arrondissement.
"""

from typing import Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import text

from .auth import verify_api_key
from .db import engine
from .limiter import limiter

router = APIRouter()


# Exécute une requête paramétrée et retourne un DataFrame prêt à sérialiser
def _read_sql(query: str, params: dict = None) -> pd.DataFrame:
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        return pd.DataFrame(result.fetchall(), columns=list(result.keys()))


@router.get("/ping")
@limiter.limit("30/minute")
def ping(request: Request):
    return {"status": "ok", "message": "API /api/ping répond bien"}


# PRIX / m²

# Retourne la liste des arrondissements présents dans la table des prix médians
@router.get("/arrondissements", dependencies=[Depends(verify_api_key)])
@limiter.limit("100/minute")
def list_arrondissements(request: Request):
    df = _read_sql(
        "SELECT DISTINCT arr_num AS arrondissement FROM prix_m2_median ORDER BY arr_num"
    )
    return {"arrondissements": df["arrondissement"].tolist()}


# Retourne le prix médian au m² filtrable par année et/ou arrondissement
@router.get("/prix", dependencies=[Depends(verify_api_key)])
@limiter.limit("100/minute")
def get_prix(
    request: Request,
    annee: Optional[int] = Query(None, description="Année (ex: 2020)"),
    arrondissement: Optional[int] = Query(None, description="Arrondissement (1-20)"),
):
    # Construction dynamique de la requête selon les filtres fournis
    query = (
        "SELECT annee, arr_num AS arrondissement, prix_m2_median"
        " FROM prix_m2_median WHERE 1=1"
    )
    params = {}
    if annee is not None:
        query += " AND annee = :annee"
        params["annee"] = annee
    if arrondissement is not None:
        query += " AND arr_num = :arr"
        params["arr"] = arrondissement
    query += " ORDER BY annee, arr_num"

    df = _read_sql(query, params)
    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée pour ces filtres (annee / arrondissement).",
        )
    return df.to_dict(orient="records")


# Retourne l'évolution temporelle du prix au m² avec variation annuelle pour un arrondissement
# variation_pct = (prix_annee - prix_annee_precedente) / prix_annee_precedente * 100
@router.get("/timeline", dependencies=[Depends(verify_api_key)])
@limiter.limit("100/minute")
def get_timeline(
    request: Request,
    arr: int = Query(..., description="Numéro d'arrondissement (1-20)"),
):
    query = """
        SELECT
            v.annee,
            pm.prix_m2_median,
            pm_prev.prix_m2_median AS prix_m2_prec_median,
            v.variation_prix_m2    AS variation_pct
        FROM variation_prix_m2 v
        JOIN prix_m2_median pm
          ON pm.arr_num = v.arr_num AND pm.annee = v.annee
        LEFT JOIN prix_m2_median pm_prev
          ON pm_prev.arr_num = v.arr_num AND pm_prev.annee = v.annee - 1
        WHERE v.arr_num = :arr
        ORDER BY v.annee
    """
    df = _read_sql(query, {"arr": arr})
    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Aucune donnée pour l'arrondissement {arr}.",
        )
    return {
        "arrondissement": arr,
        "timeline": df[["annee", "prix_m2_median", "prix_m2_prec_median", "variation_pct"]].to_dict(
            orient="records"
        ),
    }


# Compare le prix médian au m² entre deux arrondissements, avec filtre annuel optionnel
@router.get("/comparaison", dependencies=[Depends(verify_api_key)])
@limiter.limit("100/minute")
def comparaison(
    request: Request,
    arr1: int = Query(..., description="Premier arrondissement (1-20)"),
    arr2: int = Query(..., description="Deuxième arrondissement (1-20)"),
    annee: Optional[int] = Query(None, description="Année (facultative, ex: 2022)"),
):
    query = (
        "SELECT annee, arr_num AS arrondissement, prix_m2_median"
        " FROM prix_m2_median WHERE arr_num IN (:a1, :a2)"
    )
    params = {"a1": arr1, "a2": arr2}
    if annee is not None:
        query += " AND annee = :annee"
        params["annee"] = annee
    query += " ORDER BY annee, arr_num"

    df = _read_sql(query, params)
    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée pour cette comparaison (arr1 / arr2 / annee).",
        )
    return {
        "arr1": arr1,
        "arr2": arr2,
        "annee": annee,
        "data": df.to_dict(orient="records"),
    }


# LOGEMENTS SOCIAUX

# Retourne le taux de logements sociaux par arrondissement ; utilise l'année la plus récente si non précisée
@router.get("/logements_sociaux", dependencies=[Depends(verify_api_key)])
@limiter.limit("100/minute")
def logements_sociaux(
    request: Request,
    annee: Optional[int] = Query(None, description="Année (ex: 2024)"),
    arrondissement: Optional[int] = Query(None, description="Arrondissement (1-20, optionnel)"),
):
    if annee is None:
        df_yr = _read_sql("SELECT MAX(annee) AS max_annee FROM logements_sociaux_pct")
        if df_yr.empty or df_yr["max_annee"].iloc[0] is None:
            raise HTTPException(status_code=404, detail="Aucune donnée logements sociaux.")
        annee = int(df_yr["max_annee"].iloc[0])

    query = (
        "SELECT annee, arr_num AS arrondissement,"
        "       pct_logements_sociaux AS logements_sociaux_pct"
        " FROM logements_sociaux_pct WHERE annee = :annee"
    )
    params: dict = {"annee": annee}
    if arrondissement is not None:
        query += " AND arr_num = :arr"
        params["arr"] = arrondissement
    query += " ORDER BY arr_num"

    df = _read_sql(query, params)
    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée logements sociaux pour ces filtres.",
        )
    return df.to_dict(orient="records")


# INDICATEUR 3 : TYPOLOGIE DU PARC

# Retourne la répartition du parc résidentiel en studios, T2 et T3+ par arrondissement et année
@router.get("/typologie", dependencies=[Depends(verify_api_key)])
@limiter.limit("100/minute")
def typologie(
    request: Request,
    arrondissement: Optional[int] = Query(None, description="Arrondissement (1-20, optionnel)"),
    annee: Optional[int] = Query(None, description="Année (optionnelle)"),
):
    query = """
        SELECT annee,
               arr_num AS arrondissement,
               part_studio_pct,
               part_t2_pct     AS "part_T2_pct",
               part_t3plus_pct AS "part_T3plus_pct"
        FROM typologie_parc WHERE 1=1
    """
    params = {}
    if arrondissement is not None:
        query += " AND arr_num = :arr"
        params["arr"] = arrondissement
    if annee is not None:
        query += " AND annee = :annee"
        params["annee"] = annee

    df = _read_sql(query, params)
    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée de typologie pour ces filtres.",
        )
    return df.to_dict(orient="records")


# ESPACES VERTS

# Retourne le nombre et la surface totale des espaces verts par arrondissement
@router.get("/espaces_verts", dependencies=[Depends(verify_api_key)])
@limiter.limit("100/minute")
def espaces_verts(
    request: Request,
    arrondissement: Optional[int] = Query(None, description="Arrondissement (1-20, optionnel)"),
):
    query = (
        "SELECT arr_num AS arrondissement, nb_espaces_verts, surface_totale_m2"
        " FROM espaces_verts_by_arr WHERE 1=1"
    )
    params = {}
    if arrondissement is not None:
        query += " AND arr_num = :arr"
        params["arr"] = arrondissement

    df = _read_sql(query, params)
    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée d'espaces verts pour ces filtres.",
        )
    return df.to_dict(orient="records")


# INDICE DE TENSION IMMOBILIÈRE (ITI)

# Retourne l'ITI et ses composantes normalisées : prix, variation, part de logements sociaux inversée
# ITI = f(prix_norm, variation_norm, sociaux_inv_norm) — agrégation pondérée en couche Gold
@router.get("/iti", dependencies=[Depends(verify_api_key)])
@limiter.limit("100/minute")
def get_iti(
    request: Request,
    annee: Optional[int] = Query(None, description="Année (2022-2025)"),
    arrondissement: Optional[int] = Query(None, description="Arrondissement (1-20, optionnel)"),
):
    query = (
        "SELECT annee, arr_num AS arrondissement,"
        "       prix_norm, variation_norm, sociaux_inv_norm, iti"
        " FROM iti WHERE 1=1"
    )
    params = {}
    if annee is not None:
        query += " AND annee = :annee"
        params["annee"] = annee
    if arrondissement is not None:
        query += " AND arr_num = :arr"
        params["arr"] = arrondissement
    query += " ORDER BY annee, arr_num"

    df = _read_sql(query, params)
    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée ITI pour ces filtres (années disponibles : 2022-2025).",
        )
    return df.to_dict(orient="records")


# ABRIBACS / PAVDA

# Retourne le nombre d'équipements Abribac/PAVDA (mobilier urbain) par arrondissement
@router.get("/abribacs", dependencies=[Depends(verify_api_key)])
@limiter.limit("100/minute")
def abribacs(
    request: Request,
    arrondissement: Optional[int] = Query(None, description="Arrondissement (1-20, optionnel)"),
):
    query = (
        "SELECT arr_num AS arrondissement, nb_abribacs"
        " FROM abribac_by_arr WHERE 1=1"
    )
    params = {}
    if arrondissement is not None:
        query += " AND arr_num = :arr"
        params["arr"] = arrondissement

    df = _read_sql(query, params)
    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée Abribac/PAVDA pour ces filtres.",
        )
    return df.to_dict(orient="records")


# IQV — Indice de Qualité de Vie

# Retourne le score IQV composite par arrondissement et année
@router.get("/iqv", dependencies=[Depends(verify_api_key)])
@limiter.limit("100/minute")
def get_iqv(
    request: Request,
    annee: Optional[int] = Query(None, description="Année (2020-2025)"),
    arrondissement: Optional[int] = Query(None, description="Arrondissement (1-20, optionnel)"),
):
    query = (
        "SELECT annee, arr_num AS arrondissement, iqv_score"
        " FROM iqv WHERE 1=1"
    )
    params = {}
    if annee is not None:
        query += " AND annee = :annee"
        params["annee"] = annee
    if arrondissement is not None:
        query += " AND arr_num = :arr"
        params["arr"] = arrondissement
    query += " ORDER BY annee, arr_num"

    df = _read_sql(query, params)
    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée IQV pour ces filtres (années disponibles : 2020-2025).",
        )
    return df.to_dict(orient="records")


# IAM — Indice d'Activité du Marché

# Retourne le score IAM mesurant le volume et la dynamique des transactions immobilières
@router.get("/iam", dependencies=[Depends(verify_api_key)])
@limiter.limit("100/minute")
def get_iam(
    request: Request,
    annee: Optional[int] = Query(None, description="Année (2020-2025)"),
    arrondissement: Optional[int] = Query(None, description="Arrondissement (1-20, optionnel)"),
):
    query = (
        "SELECT annee, arr_num AS arrondissement, iam_score"
        " FROM iam WHERE 1=1"
    )
    params = {}
    if annee is not None:
        query += " AND annee = :annee"
        params["annee"] = annee
    if arrondissement is not None:
        query += " AND arr_num = :arr"
        params["arr"] = arrondissement
    query += " ORDER BY annee, arr_num"

    df = _read_sql(query, params)
    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée IAM pour ces filtres (années disponibles : 2020-2025).",
        )
    return df.to_dict(orient="records")


# IPR — Indice de Pression Résidentielle

# Retourne le score IPR croisant prix élevés, faible part de logements sociaux et forte activité du marché
@router.get("/ipr", dependencies=[Depends(verify_api_key)])
@limiter.limit("100/minute")
def get_ipr(
    request: Request,
    annee: Optional[int] = Query(None, description="Année (2020-2025)"),
    arrondissement: Optional[int] = Query(None, description="Arrondissement (1-20, optionnel)"),
):
    query = (
        "SELECT annee, arr_num AS arrondissement, ipr_score"
        " FROM ipr WHERE 1=1"
    )
    params = {}
    if annee is not None:
        query += " AND annee = :annee"
        params["annee"] = annee
    if arrondissement is not None:
        query += " AND arr_num = :arr"
        params["arr"] = arrondissement
    query += " ORDER BY annee, arr_num"

    df = _read_sql(query, params)
    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée IPR pour ces filtres (années disponibles : 2020-2025).",
        )
    return df.to_dict(orient="records")

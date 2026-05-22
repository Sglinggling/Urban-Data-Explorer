from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text

from .db import engine

router = APIRouter()


def _read_sql(query: str, params: dict = None) -> pd.DataFrame:
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        return pd.DataFrame(result.fetchall(), columns=list(result.keys()))


@router.get("/ping")
def ping():
    return {"status": "ok", "message": "API /api/ping répond bien"}


# PRIX / m²

@router.get("/arrondissements")
def list_arrondissements():
    df = _read_sql(
        "SELECT DISTINCT arr_num AS arrondissement FROM prix_m2_median ORDER BY arr_num"
    )
    return {"arrondissements": df["arrondissement"].tolist()}


@router.get("/prix")
def get_prix(
    annee: Optional[int] = Query(None, description="Année (ex: 2020)"),
    arrondissement: Optional[int] = Query(None, description="Arrondissement (1-20)"),
):
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


@router.get("/timeline")
def get_timeline(
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


@router.get("/comparaison")
def comparaison(
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

@router.get("/logements_sociaux")
def logements_sociaux(
    arrondissement: Optional[int] = Query(None, description="Arrondissement (1-20, optionnel)"),
):
    query = (
        "SELECT arr_num AS arrondissement,"
        "       pct_logements_sociaux AS logements_sociaux_pct"
        " FROM logements_sociaux_pct WHERE 1=1"
    )
    params = {}
    if arrondissement is not None:
        query += " AND arr_num = :arr"
        params["arr"] = arrondissement

    df = _read_sql(query, params)
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
    # part_t2_pct / part_t3plus_pct sont stockés en minuscule dans PostgreSQL ;
    # on les alias avec la casse d'origine attendue par le frontend.
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

@router.get("/espaces_verts")
def espaces_verts(
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


# ÉTABLISSEMENTS SCOLAIRES

@router.get("/etablissements_scolaires")
def etablissements_scolaires(
    arrondissement: Optional[int] = Query(None, description="Arrondissement (1-20, optionnel)"),
):
    query = """
        SELECT arr_num AS arrondissement,
               nb_maternelles, nb_elementaires, nb_colleges, nb_total_ecoles
        FROM education_par_arrondissement WHERE 1=1
    """
    params = {}
    if arrondissement is not None:
        query += " AND arr_num = :arr"
        params["arr"] = arrondissement

    df = _read_sql(query, params)
    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée d'établissements scolaires pour ces filtres.",
        )
    return df.to_dict(orient="records")


# ABRIBACS / PAVDA

@router.get("/abribacs")
def abribacs(
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

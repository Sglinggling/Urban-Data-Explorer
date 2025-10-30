import os
import pandas as pd
import re

SRC = "data/bronze/etablissements-scolaires-maternelles.csv"
DST_DIR = "data/silver"
DST = os.path.join(DST_DIR, "ecoles_maternelles_clean.csv")

os.makedirs(DST_DIR, exist_ok=True)

# 1) Lecture : les jeux OpenData Paris sont souvent en ';'
df = pd.read_csv(SRC, sep=';', engine="python")

# 2) On garde exactement ces colonnes
required = ["Libellé établissement", "Arrondissement", "Code INSEE"]
missing = [c for c in required if c not in df.columns]
if missing:
    raise ValueError(f"Colonnes manquantes dans le CSV : {missing}\nColonnes vues: {list(df.columns)}")

df = df[required].copy()

# 3) Nettoyage basique
# - dédoublonner sur (Libellé établissement + Arrondissement + Code INSEE)
df = df.drop_duplicates(subset=["Libellé établissement", "Arrondissement", "Code INSEE"])

# - normaliser Arrondissement en un numéro 1..20
def to_arr_num(row):
    # priorité au Code INSEE (ex: 75106 -> 6)
    code = str(row["Code INSEE"]) if pd.notna(row["Code INSEE"]) else ""
    m = re.search(r"751(\d{2})", code)
    if m:
        return int(m.group(1))
    # fallback : extraire le nombre depuis "Arrondissement" (ex: "6ème Arrdt" -> 6)
    al = str(row["Arrondissement"])
    m2 = re.search(r"(\d{1,2})", al)
    return int(m2.group(1)) if m2 else None

df["arr_num"] = df.apply(to_arr_num, axis=1)
df = df[df["arr_num"].between(1, 20)]

# 4) Renommer les colonnes pour avoir une sortie standardisée
out = df.rename(columns={
    "Libellé établissement": "nom_etablissement",
    "Arrondissement": "arr_libelle",
    "Code INSEE": "arr_insee"
})[["arr_num", "arr_insee", "arr_libelle", "nom_etablissement"]]

# 5) Sauvegarde
out.to_csv(DST, index=False)
print(f"✅ Silver écrit : {DST} ({len(out)} lignes)")
print(out.head(8))

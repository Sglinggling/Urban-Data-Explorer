import os
import pandas as pd

# --- 1) Charger le fichier Bronze ---
bronze_path = "data/bronze/espaces_verts.csv" # si tu l'as mis en .csv, change le nom
df = pd.read_csv(bronze_path, sep=';')  

print("✅ Fichier chargé :", df.shape)
print("Colonnes disponibles :", list(df.columns))

# --- 2) Sélectionner les colonnes utiles ---
cols_to_keep = [
    "Identifiant espace vert",
    "Nom de l'espace vert",
    "Typologie d'espace vert",
    "Code postal"
]
df = df[cols_to_keep].copy()

# --- 3) Nettoyer les données ---
# Retirer les doublons
df = df.drop_duplicates(subset=["Identifiant espace vert"])

# Nettoyer les codes postaux (ex: 75006.0 -> 75006)
df["Code postal"] = df["Code postal"].astype(str).str.extract(r"(\d{5})")

# Supprimer les lignes avec code postal vide
df = df.dropna(subset=["Code postal"])

# --- 4) Créer une colonne 'arr_num' ---
def extract_arr_num(cp):
    try:
        if cp.startswith("75"):
            return int(cp[-2:])  # prend les 2 derniers chiffres
    except:
        return None

df["arr_num"] = df["Code postal"].apply(extract_arr_num)

# --- 5) Garder seulement les arrondissements parisiens (1 à 20) ---
df = df[df["arr_num"].between(1, 20)]

# --- 6) Sauvegarder en Silver ---
os.makedirs("data/silver", exist_ok=True)
silver_path = "data/silver/espaces_verts_clean.csv"
df.to_csv(silver_path, index=False)

print("✅ Données nettoyées sauvegardées dans :", silver_path)
print(df.head())

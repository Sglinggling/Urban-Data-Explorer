import pandas as pd

df = pd.read_csv("../data/bronze/dvf.csv", sep=',',low_memory=False)

#Suppresions des données vide
df = df[df['surface_reelle_bati'].notna() & df['valeur_fonciere'].notna()]

#Récupération des colonnes voulus
cols = ['id_mutation','date_mutation', 'nature_mutation','valeur_fonciere','surface_reelle_bati','type_local','adresse_numero','adresse_suffixe','adresse_nom_voie', 'code_postal']
df_silver = df[cols]

#Création du fichier silver
output_path = "../data/silver/dvf_silver.csv"
df_silver.to_csv(output_path, index=False, sep=',')

# Affichage d’un aperçu du résultat
print(df_silver.head())

print(f"\n Fichier sauvegardé avec succès : {output_path}")

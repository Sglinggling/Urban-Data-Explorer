"""
Agrégation des transactions DVF en couche Gold.
Calcule le prix médian au m² par arrondissement et année,
puis dérive la variation annuelle en pourcentage.
Entrée : data/silver/transactions_residentiel.csv  →  Sortie : data/gold/
"""

from pathlib import Path

import pandas as pd


def compute_prix_m2_median(
    src="data/silver/transactions_residentiel.csv",
    dst="data/gold/prix_m2_median.csv"
):
    # Calcule la médiane des prix au m² par arrondissement et année
    print(f"[LOAD] {src}")
    df = pd.read_csv(src, dtype=str)

    df["valeur_fonciere"] = pd.to_numeric(df["valeur_fonciere"], errors="coerce")
    df["surface_reelle_bati"] = pd.to_numeric(df["surface_reelle_bati"], errors="coerce")

    df["annee"] = pd.to_datetime(df["date_mutation"], errors="coerce").dt.year

    df = df.dropna(subset=["valeur_fonciere", "surface_reelle_bati", "annee"])

    # prix_m2 = valeur_fonciere / surface_reelle_bati
    df["prix_m2"] = round(df["valeur_fonciere"] / df["surface_reelle_bati"],0).astype(int)

    prix_median = (
        df.groupby(["annee", "arrondissement"])["prix_m2"]
          .median()
          .reset_index()
          .sort_values(["annee", "arrondissement"])
    )

    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    prix_median.to_csv(dst, index=False)

    print(f"[OK] Prix médian au m² → {dst}")
    return prix_median



def compute_variation_prix_m2(
    src="data/gold/prix_m2_median.csv",
    dst="data/gold/variation_prix_m2.csv"
):
    # Calcule la variation annuelle du prix médian au m² par arrondissement
    print(f"[LOAD] {src}")
    df = pd.read_csv(src)

    # Décale les prix d'un an pour obtenir la valeur de l'année précédente par arrondissement
    df["prix_m2_prec"] = df.groupby("arrondissement")["prix_m2"].shift(1)

    df["prix_m2_prec"] = df["prix_m2_prec"].round(0)

    # variation_% = (prix_m2 - prix_m2_prec) / prix_m2_prec * 100
    df["variation_%"] = round((
        (df["prix_m2"] - df["prix_m2_prec"])
        / df["prix_m2_prec"] * 100
    ), 2)

    df_var = df.dropna(subset=["prix_m2_prec"])

    df_var["prix_m2_prec"] = df_var["prix_m2_prec"].round(0).astype(int)
    df_var["prix_m2"] = df_var["prix_m2"].round(0).astype(int)

    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    df_var.to_csv(dst, index=False)

    print(f"[OK] Variation % du prix → {dst}")
    return df_var

if __name__ == "__main__":
    median = compute_prix_m2_median()
    variation = compute_variation_prix_m2()

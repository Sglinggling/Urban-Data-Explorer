from pathlib import Path

import pandas as pd


# -----------------------------
# 1) Prix médian au m² par arrondissement
# -----------------------------
def compute_prix_m2_median(
    src="data/silver/transactions_residentiel.csv",
    dst="data/gold/prix_m2_median.csv"
):
    print(f"[LOAD] {src}")
    df = pd.read_csv(src, dtype=str)

    # # Casts numériques
    df["valeur_fonciere"] = pd.to_numeric(df["valeur_fonciere"], errors="coerce")
    df["surface_reelle_bati"] = pd.to_numeric(df["surface_reelle_bati"], errors="coerce")

    # # Date → année
    df["annee"] = pd.to_datetime(df["date_mutation"], errors="coerce").dt.year

    # Filtrer lignes utiles
    df = df.dropna(subset=["valeur_fonciere", "surface_reelle_bati", "annee"])

    # Prix au m²
    df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]

    # Prix médian par arrondissement
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



# -----------------------------
# 2) Variation temporelle du prix médian
# -----------------------------
def compute_variation_prix_m2(
    src="data/gold/prix_m2_median.csv",
    dst="data/gold/variation_prix_m2.csv"
):
    print(f"[LOAD] {src}")
    df = pd.read_csv(src)

    # Calcul de la variation par arrondissement
    df["prix_m2_prec"] = df.groupby("arrondissement")["prix_m2"].shift(1)

    df["variation_%"] = (
        (df["prix_m2"] - df["prix_m2_prec"])
        / df["prix_m2_prec"] * 100
    )

    # Garder uniquement années comparables
    df_var = df.dropna(subset=["prix_m2_prec"])

    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    df_var.to_csv(dst, index=False)

    print(f"[OK] Variation % du prix → {dst}")
    return df_var



# -----------------------------
# Exécution optionnelle
# -----------------------------
if __name__ == "__main__":
    median = compute_prix_m2_median()
    variation = compute_variation_prix_m2()

import re
from pathlib import Path
import pandas as pd  # type: ignore

def clean_colleges(
    src: str | Path = "data/bronze/etablissements-scolaires-colleges.csv",
    dst: str | Path = "data/silver/colleges_clean.csv",
) -> Path:
    src, dst = Path(src), Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(src, sep=";", engine="python", dtype=str, low_memory=False)

    required = ["Libellé établissement", "Arrondissement", "Code INSEE"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes: {missing}\nColonnes vues: {list(df.columns)}")

    df = df[required].drop_duplicates()

    def to_arr_num(row):
        code = str(row["Code INSEE"]) if pd.notna(row["Code INSEE"]) else ""
        m = re.search(r"751(\d{2})", code)
        if m:
            return int(m.group(1))
        al = str(row["Arrondissement"])
        m2 = re.search(r"(\d{1,2})", al)
        return int(m2.group(1)) if m2 else None

    df["arr_num"] = df.apply(to_arr_num, axis=1)
    df = df[df["arr_num"].between(1, 20)]

    out = df.rename(columns={
        "Libellé établissement": "nom_etablissement",
        "Arrondissement": "arr_libelle",
        "Code INSEE": "arr_insee",
    })[["arr_num", "arr_insee", "arr_libelle", "nom_etablissement"]]

    out.to_csv(dst, index=False)
    print(f"[COLLEGES] OK: {len(out):,} → {dst.resolve()}")
    return dst.resolve()

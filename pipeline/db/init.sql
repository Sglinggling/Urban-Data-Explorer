-- Table de référence arrondissements
CREATE TABLE arrondissements (
    arr_num INTEGER PRIMARY KEY,
    arr_insee VARCHAR(5) UNIQUE,
    arr_libelle VARCHAR(50)
);

CREATE TABLE logements_sociaux_pct (
    arr_num INTEGER PRIMARY KEY REFERENCES arrondissements(arr_num),
    pct_logements_sociaux NUMERIC(5,2)
);

CREATE TABLE espaces_verts_by_arr (
    arr_num INTEGER PRIMARY KEY REFERENCES arrondissements(arr_num),
    nb_espaces_verts INTEGER,
    surface_totale_m2 NUMERIC
);

CREATE TABLE education_par_arrondissement (
    arr_num INTEGER PRIMARY KEY REFERENCES arrondissements(arr_num),
    nb_maternelles INTEGER,
    nb_elementaires INTEGER,
    nb_colleges INTEGER,
    nb_total_ecoles INTEGER
);

CREATE TABLE abribac_by_arr (
    arr_num INTEGER PRIMARY KEY REFERENCES arrondissements(arr_num),
    nb_abribacs INTEGER
);

-- Tables temporelles PARTITIONNÉES PAR ANNÉE (narratif scalabilité)
CREATE TABLE prix_m2_median (
    arr_num INTEGER NOT NULL REFERENCES arrondissements(arr_num),
    annee INTEGER NOT NULL,
    prix_m2_median NUMERIC,
    PRIMARY KEY (annee, arr_num)
) PARTITION BY RANGE (annee);

CREATE TABLE prix_m2_median_2020 PARTITION OF prix_m2_median
    FOR VALUES FROM (2020) TO (2021);
CREATE TABLE prix_m2_median_2021 PARTITION OF prix_m2_median
    FOR VALUES FROM (2021) TO (2022);
CREATE TABLE prix_m2_median_2022 PARTITION OF prix_m2_median
    FOR VALUES FROM (2022) TO (2023);
CREATE TABLE prix_m2_median_2023 PARTITION OF prix_m2_median
    FOR VALUES FROM (2023) TO (2024);
CREATE TABLE prix_m2_median_2024 PARTITION OF prix_m2_median
    FOR VALUES FROM (2024) TO (2025);
CREATE TABLE prix_m2_median_2025 PARTITION OF prix_m2_median
    FOR VALUES FROM (2025) TO (2026);

CREATE TABLE variation_prix_m2 (
    arr_num INTEGER NOT NULL REFERENCES arrondissements(arr_num),
    annee INTEGER NOT NULL,
    variation_prix_m2 NUMERIC,
    PRIMARY KEY (annee, arr_num)
) PARTITION BY RANGE (annee);

CREATE TABLE variation_prix_m2_2021 PARTITION OF variation_prix_m2
    FOR VALUES FROM (2021) TO (2022);
CREATE TABLE variation_prix_m2_2022 PARTITION OF variation_prix_m2
    FOR VALUES FROM (2022) TO (2023);
CREATE TABLE variation_prix_m2_2023 PARTITION OF variation_prix_m2
    FOR VALUES FROM (2023) TO (2024);
CREATE TABLE variation_prix_m2_2024 PARTITION OF variation_prix_m2
    FOR VALUES FROM (2024) TO (2025);
CREATE TABLE variation_prix_m2_2025 PARTITION OF variation_prix_m2
    FOR VALUES FROM (2025) TO (2026);

CREATE TABLE typologie_parc (
    arr_num INTEGER NOT NULL REFERENCES arrondissements(arr_num),
    annee INTEGER NOT NULL,
    part_studio_pct NUMERIC,
    part_t2_pct NUMERIC,
    part_t3plus_pct NUMERIC,
    PRIMARY KEY (annee, arr_num)
) PARTITION BY RANGE (annee);

CREATE TABLE typologie_parc_2020 PARTITION OF typologie_parc
    FOR VALUES FROM (2020) TO (2021);
CREATE TABLE typologie_parc_2021 PARTITION OF typologie_parc
    FOR VALUES FROM (2021) TO (2022);
CREATE TABLE typologie_parc_2022 PARTITION OF typologie_parc
    FOR VALUES FROM (2022) TO (2023);
CREATE TABLE typologie_parc_2023 PARTITION OF typologie_parc
    FOR VALUES FROM (2023) TO (2024);
CREATE TABLE typologie_parc_2024 PARTITION OF typologie_parc
    FOR VALUES FROM (2024) TO (2025);
CREATE TABLE typologie_parc_2025 PARTITION OF typologie_parc
    FOR VALUES FROM (2025) TO (2026);

-- Index supplémentaires pour performance des requêtes
CREATE INDEX idx_prix_arr ON prix_m2_median (arr_num);
CREATE INDEX idx_variation_arr ON variation_prix_m2 (arr_num);
CREATE INDEX idx_typologie_arr ON typologie_parc (arr_num);

-- Référentiel des 20 arrondissements parisiens, clé de jointure pour toutes les tables
CREATE TABLE arrondissements (
    arr_num INTEGER PRIMARY KEY,
    arr_insee VARCHAR(5) UNIQUE,
    arr_libelle VARCHAR(50)
);

-- Taux de logements sociaux par arrondissement et année (source : RPLS)
-- Partitionnée par année pour isoler les scans temporels et faciliter l'ajout de nouvelles années
CREATE TABLE logements_sociaux_pct (
    arr_num INTEGER NOT NULL REFERENCES arrondissements(arr_num),
    annee INTEGER NOT NULL,
    pct_logements_sociaux NUMERIC(5,2),
    PRIMARY KEY (annee, arr_num)
) PARTITION BY RANGE (annee);

CREATE TABLE logements_sociaux_pct_2020 PARTITION OF logements_sociaux_pct FOR VALUES FROM (2020) TO (2021);
CREATE TABLE logements_sociaux_pct_2021 PARTITION OF logements_sociaux_pct FOR VALUES FROM (2021) TO (2022);
CREATE TABLE logements_sociaux_pct_2022 PARTITION OF logements_sociaux_pct FOR VALUES FROM (2022) TO (2023);
CREATE TABLE logements_sociaux_pct_2023 PARTITION OF logements_sociaux_pct FOR VALUES FROM (2023) TO (2024);
CREATE TABLE logements_sociaux_pct_2024 PARTITION OF logements_sociaux_pct FOR VALUES FROM (2024) TO (2025);
CREATE TABLE logements_sociaux_pct_2025 PARTITION OF logements_sociaux_pct FOR VALUES FROM (2025) TO (2026);

-- Agrégat statique : nombre et surface totale des espaces verts par arrondissement
CREATE TABLE espaces_verts_by_arr (
    arr_num INTEGER PRIMARY KEY REFERENCES arrondissements(arr_num),
    nb_espaces_verts INTEGER,
    surface_totale_m2 NUMERIC
);

-- Agrégat statique : densité de mobilier urbain (abri-bacs) par arrondissement
CREATE TABLE abribac_by_arr (
    arr_num INTEGER PRIMARY KEY REFERENCES arrondissements(arr_num),
    nb_abribacs INTEGER
);

-- Prix médian au m² des transactions immobilières par arrondissement et année (source : DVF)
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

-- Variation annuelle du prix médian au m² : (prix_n - prix_n-1) / prix_n-1 × 100
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

-- Répartition des types de logements vendus (studio, T2, T3+) en pourcentage par arrondissement et année
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

-- ITI — Indice de Tension Immobilière : agrège prix normalisé, variation et part de logements sociaux inversée
-- Formule : ITI = (prix_norm + variation_norm + sociaux_inv_norm) / 3
CREATE TABLE iti (
    arr_num INTEGER NOT NULL REFERENCES arrondissements(arr_num),
    annee INTEGER NOT NULL,
    prix_norm NUMERIC(5,2),
    variation_norm NUMERIC(5,2),
    sociaux_inv_norm NUMERIC(5,2),
    iti NUMERIC(5,2),
    PRIMARY KEY (annee, arr_num)
) PARTITION BY RANGE (annee);

CREATE TABLE iti_2022 PARTITION OF iti FOR VALUES FROM (2022) TO (2023);
CREATE TABLE iti_2023 PARTITION OF iti FOR VALUES FROM (2023) TO (2024);
CREATE TABLE iti_2024 PARTITION OF iti FOR VALUES FROM (2024) TO (2025);
CREATE TABLE iti_2025 PARTITION OF iti FOR VALUES FROM (2025) TO (2026);

-- IQV — Indice de Qualité de Vie : combine espaces verts et équipements urbains par arrondissement
CREATE TABLE iqv (
    arr_num INTEGER NOT NULL REFERENCES arrondissements(arr_num),
    annee INTEGER NOT NULL,
    iqv_score NUMERIC(5,2),
    PRIMARY KEY (annee, arr_num)
) PARTITION BY RANGE (annee);

CREATE TABLE iqv_2020 PARTITION OF iqv FOR VALUES FROM (2020) TO (2021);
CREATE TABLE iqv_2021 PARTITION OF iqv FOR VALUES FROM (2021) TO (2022);
CREATE TABLE iqv_2022 PARTITION OF iqv FOR VALUES FROM (2022) TO (2023);
CREATE TABLE iqv_2023 PARTITION OF iqv FOR VALUES FROM (2023) TO (2024);
CREATE TABLE iqv_2024 PARTITION OF iqv FOR VALUES FROM (2024) TO (2025);
CREATE TABLE iqv_2025 PARTITION OF iqv FOR VALUES FROM (2025) TO (2026);

-- IAM — Indice d'Activité du Marché : mesure le dynamisme transactionnel d'un arrondissement
CREATE TABLE iam (
    arr_num INTEGER NOT NULL REFERENCES arrondissements(arr_num),
    annee INTEGER NOT NULL,
    iam_score NUMERIC(5,2),
    PRIMARY KEY (annee, arr_num)
) PARTITION BY RANGE (annee);

CREATE TABLE iam_2020 PARTITION OF iam FOR VALUES FROM (2020) TO (2021);
CREATE TABLE iam_2021 PARTITION OF iam FOR VALUES FROM (2021) TO (2022);
CREATE TABLE iam_2022 PARTITION OF iam FOR VALUES FROM (2022) TO (2023);
CREATE TABLE iam_2023 PARTITION OF iam FOR VALUES FROM (2023) TO (2024);
CREATE TABLE iam_2024 PARTITION OF iam FOR VALUES FROM (2024) TO (2025);
CREATE TABLE iam_2025 PARTITION OF iam FOR VALUES FROM (2025) TO (2026);

-- IPR — Indice de Pression Résidentielle : reflète la demande relative au parc disponible
CREATE TABLE ipr (
    arr_num INTEGER NOT NULL REFERENCES arrondissements(arr_num),
    annee INTEGER NOT NULL,
    ipr_score NUMERIC(5,2),
    PRIMARY KEY (annee, arr_num)
) PARTITION BY RANGE (annee);

CREATE TABLE ipr_2020 PARTITION OF ipr FOR VALUES FROM (2020) TO (2021);
CREATE TABLE ipr_2021 PARTITION OF ipr FOR VALUES FROM (2021) TO (2022);
CREATE TABLE ipr_2022 PARTITION OF ipr FOR VALUES FROM (2022) TO (2023);
CREATE TABLE ipr_2023 PARTITION OF ipr FOR VALUES FROM (2023) TO (2024);
CREATE TABLE ipr_2024 PARTITION OF ipr FOR VALUES FROM (2024) TO (2025);
CREATE TABLE ipr_2025 PARTITION OF ipr FOR VALUES FROM (2025) TO (2026);

-- Index secondaires sur arr_num : accélèrent les jointures et filtrages par arrondissement
-- sans dupliquer la clé primaire (annee, arr_num) déjà gérée par les partitions
CREATE INDEX idx_prix_arr ON prix_m2_median (arr_num);
CREATE INDEX idx_variation_arr ON variation_prix_m2 (arr_num);
CREATE INDEX idx_typologie_arr ON typologie_parc (arr_num);
CREATE INDEX idx_iti_arr ON iti (arr_num);
CREATE INDEX idx_log_soc_arr ON logements_sociaux_pct (arr_num);
CREATE INDEX idx_iqv_arr ON iqv (arr_num);
CREATE INDEX idx_iam_arr ON iam (arr_num);
CREATE INDEX idx_ipr_arr ON ipr (arr_num);

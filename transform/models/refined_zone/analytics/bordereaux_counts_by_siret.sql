{{ config(
    materialized = 'table',
    indexes = [ {'columns': ['siret'],
    'unique': True },],
) }}

WITH emitter_counts AS (
    SELECT
        emitter_company_siret AS "siret",
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSDD'
        )                     AS num_bsdd_emitter,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSDA'
        )                     AS num_bsda_emitter,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSFF'
        )                     AS num_bsff_emitter,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSDASRI'
        )                     AS num_bsdasri_emitter,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSVHU'
        )                     AS num_bsvhu_emitter
    FROM
         {{ ref('bordereaux_enriched') }} 
    GROUP BY
        emitter_company_siret
),

destination_counts AS (
    SELECT
        destination_company_siret AS "siret",
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSDD'
        )                         AS num_bsdd_destination,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSDA'
        )                         AS num_bsda_destination,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSFF'
        )                         AS num_bsff_destination,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSDASRI'
        )                         AS num_bsdasri_destination,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSVHU'
        )                         AS num_bsvhu_destination
    FROM
        {{ ref('bordereaux_enriched') }}
    GROUP BY
        destination_company_siret
),

full_ AS (
    SELECT
        coalesce(emitter_counts.siret,destination_counts.siret) as siret,
        COALESCE(
            emitter_counts.num_bsdd_emitter,
            0
        ) AS num_bsdd_emitter,
        COALESCE(
            emitter_counts.num_bsda_emitter,
            0
        ) AS num_bsda_emitter,
        COALESCE(
            emitter_counts.num_bsff_emitter,
            0
        ) AS num_bsff_emitter,
        COALESCE(
            emitter_counts.num_bsdasri_emitter,
            0
        ) AS num_bsdasri_emitter,
        COALESCE(
            emitter_counts.num_bsvhu_emitter,
            0
        ) AS num_bsvhu_emitter,
        COALESCE(
            destination_counts.num_bsdd_destination, 0
        ) AS num_bsdd_destination,
        COALESCE(
            destination_counts.num_bsda_destination, 0
        ) AS num_bsda_destination,
        COALESCE(
            destination_counts.num_bsff_destination, 0
        ) AS num_bsff_destination,
        COALESCE(
            destination_counts.num_bsdasri_destination, 0
        ) AS num_bsdasri_destination,
        COALESCE(
            destination_counts.num_bsvhu_destination, 0
        ) AS num_bsvhu_destination
    FROM
        emitter_counts
    FULL
    OUTER JOIN destination_counts ON
        emitter_counts.siret = destination_counts.siret
)

SELECT
    *,
    num_bsdd_emitter
    + num_bsda_emitter
    + num_bsff_emitter
    + num_bsdasri_emitter
    + num_bsvhu_emitter
    + num_bsdd_destination
    + num_bsda_destination
    + num_bsff_destination
    + num_bsdasri_destination
    + num_bsvhu_destination AS total_bordereaux
FROM
    full_

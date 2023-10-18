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
        )                     AS num_bsdd_as_emitter,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSDA'
        )                     AS num_bsda_as_emitter,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSFF'
        )                     AS num_bsff_as_emitter,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSDASRI'
        )                     AS num_bsdasri_as_emitter,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSVHU'
        )                     AS num_bsvhu_as_emitter,
        sum(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSDD'
        )                     AS quantity_bsdd_as_emitter,
        sum(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSDA'
        )                     AS quantity_bsda_as_emitter,
        sum(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSFF'
        )                     AS quantity_bsff_as_emitter,
        sum(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSDASRI'
        )                     AS quantity_bsdasri_as_emitter,
        sum(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSVHU'
        )                     AS quantity_bsvhu_as_emitter
    FROM
        {{ ref('bordereaux_enriched') }}
    GROUP BY
        emitter_company_siret
),

transporter_counts AS (
    SELECT
        transporter_company_siret AS "siret",
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSDD'
        )                     AS num_bsdd_as_transporter,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSDA'
        )                     AS num_bsda_as_transporter,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSFF'
        )                     AS num_bsff_as_transporter,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSDASRI'
        )                     AS num_bsdasri_as_transporter,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSVHU'
        )                     AS num_bsvhu_as_transporter,
        sum(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSDD'
        )                     AS quantity_bsdd_as_transporter,
        sum(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSDA'
        )                     AS quantity_bsda_as_transporter,
        sum(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSFF'
        )                     AS quantity_bsff_as_transporter,
        sum(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSDASRI'
        )                     AS quantity_bsdasri_as_transporter,
        sum(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSVHU'
        )                     AS quantity_bsvhu_as_transporter
    FROM
        {{ ref('bordereaux_enriched') }}
    GROUP BY
        transporter_company_siret
),

destination_counts AS (
    SELECT
        destination_company_siret AS "siret",
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSDD'
        )                         AS num_bsdd_as_destination,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSDA'
        )                         AS num_bsda_as_destination,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSFF'
        )                         AS num_bsff_as_destination,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSDASRI'
        )                         AS num_bsdasri_as_destination,
        COUNT(id) FILTER (
            WHERE
            _bs_type = 'BSVHU'
        )                         AS num_bsvhu_as_destination,
        sum(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSDD'
        )                     AS quantity_bsdd_as_destination,
        sum(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSDA'
        )                     AS quantity_bsda_as_destination,
        sum(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSFF'
        )                     AS quantity_bsff_as_destination,
        sum(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSDASRI'
        )                     AS quantity_bsdasri_as_destination,
        sum(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSVHU'
        )                     AS quantity_bsvhu_as_destination
    FROM
        {{ ref('bordereaux_enriched') }}
    GROUP BY
        destination_company_siret
),

full_ AS (
    SELECT
        COALESCE(emitter_counts.siret, transporter_counts.siret, destination_counts.siret,c.siret) AS siret,
        COALESCE(
            emitter_counts.num_bsdd_as_emitter,
            0
        )                                                        AS num_bsdd_as_emitter,
        COALESCE(
            emitter_counts.num_bsda_as_emitter,
            0
        )                                                        AS num_bsda_as_emitter,
        COALESCE(
            emitter_counts.num_bsff_as_emitter,
            0
        )                                                        AS num_bsff_as_emitter,
        COALESCE(
            emitter_counts.num_bsdasri_as_emitter,
            0
        )                                                        AS num_bsdasri_as_emitter,
        COALESCE(
            emitter_counts.num_bsvhu_as_emitter,
            0
        )                                                        AS num_bsvhu_as_emitter,
        COALESCE(
            emitter_counts.quantity_bsdd_as_emitter,
            0
        )                                                        AS quantity_bsdd_as_emitter,
        COALESCE(
            emitter_counts.quantity_bsda_as_emitter,
            0
        )                                                        AS quantity_bsda_as_emitter,
        COALESCE(
            emitter_counts.quantity_bsff_as_emitter,
            0
        )                                                        AS quantity_bsff_as_emitter,
        COALESCE(
            emitter_counts.quantity_bsdasri_as_emitter,
            0
        )                                                        AS quantity_bsdasri_as_emitter,
        COALESCE(
            emitter_counts.quantity_bsvhu_as_emitter,
            0
        )                                                        AS quantity_bsvhu_as_emitter,
        COALESCE(
            transporter_counts.num_bsdd_as_transporter,
            0
        )                                                        AS num_bsdd_as_transporter,
        COALESCE(
            transporter_counts.num_bsda_as_transporter,
            0
        )                                                        AS num_bsda_as_transporter,
        COALESCE(
            transporter_counts.num_bsff_as_transporter,
            0
        )                                                        AS num_bsff_as_transporter,
        COALESCE(
            transporter_counts.num_bsdasri_as_transporter,
            0
        )                                                        AS num_bsdasri_as_transporter,
        COALESCE(
            transporter_counts.num_bsvhu_as_transporter,
            0
        )                                                        AS num_bsvhu_as_transporter,
        COALESCE(
            transporter_counts.quantity_bsdd_as_transporter,
            0
        )                                                        AS quantity_bsdd_as_transporter,
        COALESCE(
            transporter_counts.quantity_bsda_as_transporter,
            0
        )                                                        AS quantity_bsda_as_transporter,
        COALESCE(
            transporter_counts.quantity_bsff_as_transporter,
            0
        )                                                        AS quantity_bsff_as_transporter,
        COALESCE(
            transporter_counts.quantity_bsdasri_as_transporter,
            0
        )                                                        AS quantity_bsdasri_as_transporter,
        COALESCE(
            transporter_counts.quantity_bsvhu_as_transporter,
            0
        )                                                        AS quantity_bsvhu_as_transporter,        
        COALESCE(
            destination_counts.num_bsdd_as_destination, 0
        )                                                        AS num_bsdd_as_destination,
        COALESCE(
            destination_counts.num_bsda_as_destination, 0
        )                                                        AS num_bsda_as_destination,
        COALESCE(
            destination_counts.num_bsff_as_destination, 0
        )                                                        AS num_bsff_as_destination,
        COALESCE(
            destination_counts.num_bsdasri_as_destination, 0
        )                                                        AS num_bsdasri_as_destination,
        COALESCE(
            destination_counts.num_bsvhu_as_destination, 0
        )                                                        AS num_bsvhu_as_destination,
        COALESCE(
            destination_counts.quantity_bsdd_as_destination,
            0
        )                                                        AS quantity_bsdd_as_destination,
        COALESCE(
            destination_counts.quantity_bsda_as_destination,
            0
        )                                                        AS quantity_bsda_as_destination,
        COALESCE(
            destination_counts.quantity_bsff_as_destination,
            0
        )                                                        AS quantity_bsff_as_destination,
        COALESCE(
            destination_counts.quantity_bsdasri_as_destination,
            0
        )                                                        AS quantity_bsdasri_as_destination,
        COALESCE(
            destination_counts.quantity_bsvhu_as_destination,
            0
        )                                                        AS quantity_bsvhu_as_destination
    FROM
        emitter_counts
    FULL
    OUTER JOIN
        transporter_counts ON
        emitter_counts.siret = transporter_counts.siret
    FULL
    OUTER JOIN
        destination_counts ON
        coalesce(emitter_counts.siret,transporter_counts.siret) = destination_counts.siret
    FULL OUTER join {{ ref('company') }} c on coalesce(emitter_counts.siret,transporter_counts.siret,destination_counts.siret) = c.siret
)

SELECT
    *,
    num_bsdd_as_emitter
    + num_bsda_as_emitter
    + num_bsff_as_emitter
    + num_bsdasri_as_emitter
    + num_bsvhu_as_emitter
    + num_bsdd_as_transporter
    + num_bsda_as_transporter
    + num_bsff_as_transporter
    + num_bsdasri_as_transporter
    + num_bsvhu_as_transporter
    + num_bsdd_as_destination
    + num_bsda_as_destination
    + num_bsff_as_destination
    + num_bsdasri_as_destination
    + num_bsvhu_as_destination AS total_mentions_bordereaux
FROM
    full_

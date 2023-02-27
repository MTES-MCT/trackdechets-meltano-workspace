{{ config(
    materialized = 'table',
    indexes = [ {'columns': ['created_at'] },{ 'columns': ['emitter_company_siret'] },{ 'columns' :['destination_company_siret'] },{ 'columns' :['transporter_company_siret'] }],
    post_hook = after_commit('DROP TABLE IF EXISTS refined_zone_enriched.bsda_enriched_temp')
) }}

WITH etabs AS (

    SELECT
        cgc.*,
        se.siret,
        se.activite_principale_etablissement
    FROM
        {{ ref('stock_etablissement') }}
        AS se
    INNER JOIN
        {{ ref('code_geo_communes') }}
        AS cgc
        ON se.code_commune_etablissement = cgc.code_commune
    WHERE
        cgc.type_commune = 'COM'
)

SELECT
    b.*,
    etabs.code_departement                  AS "destination_departement",
    etabs.code_region                       AS "destination_region",
    etabs.activite_principale_etablissement AS "destination_naf"
FROM
    {{ ref('bsda_enriched_temp') }}
    AS b
LEFT JOIN etabs
    ON b.destination_company_siret = etabs.siret

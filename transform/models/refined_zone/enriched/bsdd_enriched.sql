{{ config(
    materialized = 'table',
    indexes = [ {'columns': ['created_at'] },
    { 'columns': ['emitter_company_siret'] },
    { 'columns' :['recipient_company_siret'] },
    { 'columns' :['transporter_company_siret'] }],
    post_hook=after_commit('DROP TABLE IF EXISTS refined_zone_enriched.bsdd_enriched_temp')
) }}

WITH etabs AS (
    {{ create_sirene_etabs_enriched_statement() }}
)
SELECT
    b.*,
    etabs.code_departement                  AS "recipient_departement",
    etabs.code_region                       AS "recipient_region",
    etabs.activite_principale_etablissement AS "recipient_naf"
FROM
    {{ ref('bsdd_enriched_temp') }}
    AS b
LEFT JOIN etabs
    ON b.recipient_company_siret = etabs.siret

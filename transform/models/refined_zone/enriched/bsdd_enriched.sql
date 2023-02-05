WITH etabs AS (
    SELECT
        se.siret,
        cgc.*
    FROM
        {{ ref('stock_etablissement') }}
        se
        INNER JOIN {{ ref('code_geo_communes') }}
        cgc
        ON se.code_commune_etablissement = cgc.code_commune
    WHERE
        cgc.type_commune = 'COM'
)
SELECT
    b.*,
    e.code_departement AS "recipient_departement",
    e.code_region AS "recipient_region"
FROM
    {{ ref('bsdd_enriched_temp') }}
    b
    LEFT JOIN etabs e
    ON b.recipient_company_siret = e.siret

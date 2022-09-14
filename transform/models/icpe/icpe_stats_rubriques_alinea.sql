{{ config(
    materialized = 'incremental',
    unique_key = 'inserted_at'
) }}

WITH incremental_data AS (

    SELECT
        *
    FROM
        {{ ref("icpe_siretise") }}
        ic

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE
    ic."inserted_at" > (
        SELECT
            MAX("inserted_at")
        FROM
            {{ this }}
    )
{% endif %}
),
data_ AS (
    SELECT
        MAX("inserted_at") AS "inserted_at",
        "rubrique_ic",
        "alinea",
        COUNT(
            DISTINCT "codeS3ic"
        ) AS "Nombre d'installations classées",
        COUNT(
            DISTINCT CASE
                WHEN "siret_clean" IS NULL THEN NULL
                ELSE "siret_clean"
            END
        ) AS "Nombre d'établissements aprés siretisation",
        COUNT(
            DISTINCT CASE
                WHEN (
                    "siret_clean" IS NOT NULL
                    AND "etatAdministratifEtablissement" = 'A'
                ) THEN "siret_clean"
                ELSE NULL
            END
        ) AS "Nombre d'établissements actifs aprés siretisations",
        COUNT(
            DISTINCT CASE
                WHEN (
                    "siret_clean" IS NOT NULL
                    AND "etatAdministratifEtablissement" = 'A'
                    AND inscrit_sur_td
                ) THEN "siret_clean"
                ELSE NULL
            END
        ) AS "Nombre d'établissements actifs aprés siretisations et inscrits"
    FROM
        incremental_data
    GROUP BY
        "rubrique_ic","alinea"
)
SELECT
    data_.*,
    100 * "Nombre d'établissements actifs aprés siretisations et inscrits" / NULLIF(
        "Nombre d'établissements aprés siretisation",
        0
    ) AS "Pourcentage d'établissements inscrits sur TD"
FROM
    data_

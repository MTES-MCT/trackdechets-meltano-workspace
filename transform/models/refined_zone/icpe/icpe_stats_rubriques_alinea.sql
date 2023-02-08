{{ config(
    materialized = 'incremental',
    unique_key = 'inserted_at'
) }}

WITH incremental_data AS (

    SELECT *
    FROM
        {{ ref("icpe_siretise") }}
        AS ic

    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        WHERE
            ic."inserted_at" > (
                SELECT MAX("inserted_at")
                FROM
                    {{ this }}
            )
    {% endif %}
),

data_ AS (
    SELECT
        "rubrique",
        "alinea",
        MAX("inserted_at") AS "inserted_at",
        COUNT(
            DISTINCT code_s3ic
        ) AS "Nombre d'installations classées",
        COUNT(
            DISTINCT "siret_clean"
        ) AS "Nombre d'établissements aprés siretisation",
        COUNT(
            DISTINCT CASE
                WHEN (
                    "siret_clean" IS NOT NULL
                    AND "etat_administratif_etablissement" = 'A'
                ) THEN "siret_clean"
            END
        ) AS "Nombre d'établissements actifs aprés siretisation",
        COUNT(
            DISTINCT CASE
                WHEN (
                    "siret_clean" IS NOT NULL
                    AND "etat_administratif_etablissement" = 'A'
                    AND inscrit_sur_td
                ) THEN "siret_clean"
            END
        ) AS "Nombre d'établissements actifs aprés siretisation et inscrits"
    FROM
        incremental_data
    GROUP BY
        "rubrique", "alinea"
)

SELECT
    data_.*,
    100
    * data_."Nombre d'établissements actifs aprés siretisation et inscrits"
    / NULLIF(
        data_."Nombre d'établissements aprés siretisation",
        0
    ) AS "Pourcentage d'établissements inscrits sur TD"
FROM
    data_

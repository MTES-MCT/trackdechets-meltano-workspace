{{ config(
    materialized = 'incremental',
    unique_key='inserted_at'
) }}

SELECT
    DISTINCT
    ON (
        "codeS3ic",
        "siret_clean",
        id_ref_nomencla_ic
    ) ic.inserted_at,
    ic."codeS3ic",
    ic.id_ref_nomencla_ic,
    nomen.rubrique_ic,
    nomen.alinea,
    etabs."s3icNumeroSiret" AS siret_icpe,
    gerep."numero_siret" AS siret_gerep,
    COALESCE(
        etabs."s3icNumeroSiret",
        gerep."numero_siret"
    ) AS siret_clean,
    sirene."etatAdministratifEtablissement",
    CASE
        WHEN td_etabs.siret IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS inscrit_sur_td
FROM
    raw_zone_icpe.ic_installation_classee ic
    LEFT JOIN raw_zone_icpe.ic_ref_nomenclature_ic nomen
    ON ic."id_ref_nomencla_ic" = nomen."id"
    LEFT JOIN raw_zone_icpe.ic_etablissement etabs
    ON ic."codeS3ic" = etabs."codeS3ic"
    LEFT JOIN {{ ref('gerep') }}
    gerep
    ON ic."codeS3ic" = gerep."codeS3ic"
    LEFT JOIN raw_zone_insee.stock_etablissement AS sirene
    ON COALESCE(
        etabs."s3icNumeroSiret",
        gerep."numero_siret"
    ) = sirene.siret
    LEFT JOIN raw_zone_trackdechets.company td_etabs
    ON COALESCE(
        etabs."s3icNumeroSiret",
        gerep."numero_siret"
    ) = td_etabs.siret
WHERE
    (
        ic."date_fin_validite" IS NULL
        OR TO_DATE(
            ic."date_fin_validite",
            'DD/MM/YYYY'
        ) >= CURRENT_DATE
    )
    AND (
        etabs."s3icNumeroSiret" IS NULL
        OR LENGTH(
            etabs."s3icNumeroSiret"
        ) = 14
    )

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
AND (
    ic."inserted_at" > (
        SELECT
            MAX("inserted_at")
        FROM
            {{ this }}
    )
    AND etabs."inserted_at" > (
        SELECT
            MAX("inserted_at")
        FROM
            {{ this }}
    )
    AND nomen."inserted_at" > (
        SELECT
            MAX("inserted_at")
        FROM
            {{ this }}
    )
)
{% endif %}

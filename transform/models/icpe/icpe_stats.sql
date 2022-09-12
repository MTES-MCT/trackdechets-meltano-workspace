SELECT
    DISTINCT
    ON (
        "codeS3ic",
        "siret_clean",
        id_ref_nomencla_ic
    ) ic.*,
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
    {{ ref('ic_installations_classes') }}
    ic
    LEFT JOIN raw_zone.ic_ref_nomenclature_ic nomen
    ON ic."id_ref_nomencla_ic" = nomen."id"
    LEFT JOIN {{ ref('ic_etablissements') }}
    etabs
    ON ic."codeS3ic" = etabs."codeS3ic"
    LEFT JOIN {{ ref('gerep') }}
    gerep
    ON ic."codeS3ic" = gerep."codeS3ic"
    LEFT JOIN raw_zone.stock_etablissement_sirene AS sirene
    ON COALESCE(
        etabs."s3icNumeroSiret",
        gerep."numero_siret"
    ) = sirene.siret
    LEFT JOIN raw_zone.company td_etabs
    ON COALESCE(
        etabs."s3icNumeroSiret",
        gerep."numero_siret"
    ) = td_etabs.siret

SELECT
    DISTINCT
    ON (
        ic.code_s3ic,
        "siret_clean",
        id_nomenclature
    ) ic.inserted_at,
    ic.code_s3ic,
    ic.id_nomenclature,
    ic.date_debut_exploitation,
    ic.date_fin_validite,
    ic.volume,
    ic.unite,
    ic.statut_ic,
    nomen.rubrique,
    nomen.alinea,
    nomen.libelle_court_activite,
    nomen.en_vigueur,
    nomen.id_regime,
    etabs.siret AS siret_icpe,
    etabs.nom_etablissement as nom_etablissement_icpe,
    gerep."numero_siret" AS siret_gerep,
    COALESCE(
        etabs.siret,
        gerep."numero_siret"
    ) AS siret_clean,
    sirene.etat_administratif_etablissement,
    CASE
        WHEN td_etabs.siret IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS inscrit_sur_td
FROM
    {{ ref('installations_classees') }}
    ic
    LEFT JOIN {{ ref('nomenclature') }}
    nomen
    ON ic.id_nomenclature = nomen."id"
    LEFT JOIN {{ ref('etablissements') }}
    etabs
    ON ic.code_s3ic = etabs.code_s3ic
    LEFT JOIN {{ ref('gerep') }}
    gerep
    ON ic.code_s3ic = gerep."codeS3ic"
    LEFT JOIN {{ ref('stock_etablissement') }} AS sirene
    ON COALESCE(
        etabs.siret,
        gerep."numero_siret"
    ) = sirene.siret
    LEFT JOIN {{ ref('company') }}
    td_etabs
    ON COALESCE(
        etabs.siret,
        gerep."numero_siret"
    ) = td_etabs.siret
WHERE
    (etabs.siret IS NULL
    OR LENGTH(
        etabs.siret
    ) = 14)

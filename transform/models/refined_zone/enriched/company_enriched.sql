SELECT
    c.id,
    c.siret,
    c.created_at,
    c.updated_at,
    c.security_code,
    c."name",
    c.gerep_id,
    c.code_naf,
    c.given_name,
    c.contact_email,
    c.contact_phone,
    c.website,
    c.transporter_receipt_id,
    c.trader_receipt_id,
    c.eco_organisme_agreements,
    c.company_types,
    c.address,
    c.latitude,
    c.longitude,
    c.broker_receipt_id,
    c.verification_code,
    c.verification_status,
    c.verification_mode,
    c.verification_comment,
    c.verified_at,
    c.vhuagrement_demolisseur_id,
    c.vhu_agrement_broyeur_id,
    c.allow_bsdasri_take_over_without_signature,
    c.vat_number,
    c.contact,
    c.worker_certification_id,
    etabs.etat_administratif_etablissement,
    comm.code_commune,
    comm.code_departement,
    comm.code_region,
    naf.*
FROM
    {{ ref('company') }} C
    LEFT JOIN {{ ref('stock_etablissement') }}
    etabs
    ON C.siret = etabs.siret
    LEFT JOIN {{ ref('code_geo_communes') }}
    comm
    ON COALESCE(
        etabs.code_commune_etablissement,
        etabs.code_commune_2_etablissement
    ) = comm.code_commune
    LEFT JOIN {{ ref('nomenclature_activites_francaises') }}
    naf
    ON REPLACE(
        C.code_naf,
        '.',
        ''
    ) = REPLACE(
        naf.code_sous_classe,
        '.',
        ''
    )

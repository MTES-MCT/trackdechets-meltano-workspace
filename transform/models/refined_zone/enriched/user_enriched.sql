SELECT
    u.*,
    C.id as "company_id",
    C.siret,
    C.created_at as "company_created_at",
    C.updated_at as "company_updated_at",
    C.security_code,
    C."name" as "company_name",
    C.gerep_id,
    C.code_naf,
    C.given_name,
    C.contact_email,
    C.contact_phone,
    C.website,
    C.transporter_receipt_id,
    C.trader_receipt_id,
    C.eco_organisme_agreements,
    C.company_types,
    C.address,
    C.latitude,
    C.longitude,
    C.broker_receipt_id,
    C.verification_code,
    C.verification_status,
    c.verification_mode,
    c.verification_comment,
    c.verified_at,
    c.vhu_agrement_demolisseur_id,
    c.vhu_agrement_broyeur_id,
    c.allow_bsdasri_take_over_without_signature,
    c.vat_number,
    c.contact,
    c.code_departement,
    c.worker_certification_id,
    c.etat_administratif_etablissement,
    c.code_commune_insee,
    c.code_departement_insee,
    c.code_region_insee,
    c.code_section,
    c.libelle_section,
    c.code_division,
    c.libelle_division,
    c.code_groupe,
    c.libelle_groupe,
    c.code_classe,
    c.libelle_classe,
    c.code_sous_classe,
    c.libelle_sous_classe
FROM
    {{ ref('user') }}
    u
    INNER JOIN {{ ref('company_association') }}
    ca
    ON u.id = ca.user_id
    LEFT JOIN {{ ref('company_enriched') }} C
    ON ca.company_id = C.id

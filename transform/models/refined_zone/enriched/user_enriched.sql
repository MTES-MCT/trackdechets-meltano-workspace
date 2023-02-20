SELECT
    u.*,
    c.id         AS "company_id",
    c.siret,
    c.created_at AS "company_created_at",
    c.updated_at AS "company_updated_at",
    c.security_code,
    c."name"     AS "company_name",
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
    AS u
INNER JOIN
    {{ ref('company_association') }}
    AS ca
    ON u.id = ca.user_id
LEFT JOIN {{ ref('company_enriched') }} AS c
    ON ca.company_id = c.id

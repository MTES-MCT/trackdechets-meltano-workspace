{{ config(
  pre_hook = "{{ create_indexes_for_source(['createdat','siret']) }}"
) }}
SELECT
    id                                   AS "id",
    siret                                AS "siret",
    createdat                            AS "created_at",
    updatedat                            AS "updated_at",
    securitycode                         AS "security_code",
    "name",
    gerepid                              AS "gerep_id",
    codenaf                              AS "code_naf",
    givenname                            AS "given_name",
    contactemail                         AS "contact_email",
    contactphone                         AS "contact_phone",
    website                              AS "website",
    transporterreceiptid                 AS "transporter_receipt_id",
    traderreceiptid                      AS "trader_receipt_id",
    ecoorganismeagreements               AS "eco_organisme_agreements",
    companytypes                         AS "company_types",
    address                              AS "address",
    latitude                             AS "latitude",
    longitude                            AS "longitude",
    brokerreceiptid                      AS "broker_receipt_id",
    verificationcode                     AS "verification_code",
    verificationstatus                   AS "verification_status",
    verificationmode                     AS "verification_mode",
    verificationcomment                  AS "verification_comment",
    verifiedat                           AS "verified_at",
    vhuagrementdemolisseurid             AS "vhu_agrement_demolisseur_id",
    vhuagrementbroyeurid                 AS "vhu_agrement_broyeur_id",
    allowbsdasritakeoverwithoutsignature AS "allow_bsdasri_take_over_without_signature",
    vatnumber                            AS "vat_number",
    contact                              AS "contact",
    codedepartement                      AS "code_departement",
    workercertificationid                AS "worker_certification_id"
FROM
    {{ source("raw_zone_trackdechets", "company_raw") }}

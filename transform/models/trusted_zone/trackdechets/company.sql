SELECT
    id as "id",
    siret as "siret",
    createdat as "created_at",
    updatedat as "updated_at",
    securitycode as "security_code",
    "name",
    gerepid as "gerep_id",
    codenaf as "code_naf",
    givenname as "given_name",
    contactemail as "contact_email",
    contactphone as "contact_phone",
    website as "website",
    transporterreceiptid as "transporter_receipt_id",
    traderreceiptid as "trader_receipt_id",
    ecoorganismeagreements as "eco_organisme_agreements",
    companytypes as "company_types",
    address as "address",
    latitude as "latitude",
    longitude as "longitude",
    brokerreceiptid as "broker_receipt_id",
    verificationcode as "verification_code",
    verificationstatus as "verification_status",
    verificationmode as "verification_mode",
    verificationcomment as "verification_comment",
    verifiedat as "verified_at",
    vhuagrementdemolisseurid as "vhuagrement_demolisseur_id",
    vhuagrementbroyeurid as "vhu_agrement_broyeur_id",
    allowbsdasritakeoverwithoutsignature as "allow_bsdasri_take_over_without_signature",
    vatnumber as "vat_number",
    contact as "contact",
    codedepartement as "code_departement",
    workercertificationid as "worker_certification_id"
FROM
    {{ source("raw_zone_trackdechets", "company") }}

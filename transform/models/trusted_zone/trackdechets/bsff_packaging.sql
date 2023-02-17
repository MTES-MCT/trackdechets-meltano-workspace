SELECT
    id,
    "bsffid" AS bsff_id,
    numero,
    "type",
    volume,
    "acceptationstatus" AS acceptation_status,
    "acceptationdate" AS acceptation_date,
    "acceptationsignaturedate" AS acceptation_signature_date,
    "acceptationsignatureauthor" AS acceptation_signature_author,
    "acceptationrefusalreason" AS acceptationrefusalreason,
    "acceptationweight" AS acceptation_weight,
    "acceptationwastecode" AS acceptation_waste_code,
    "acceptationwastedescription" AS acceptation_waste_description,
    "operationdate" AS operation_date,
    "operationsignaturedate" AS operation_signature_date,
    "operationsignatureauthor" AS operation_signature_author,
    "operationcode" AS operation_code,
    "operationdescription" AS operation_description,
    "operationnotraceability" AS operation_no_traceability,
    "operationnextdestinationcompanysiret" AS operation_next_destination_company_siret,
    "operationnextdestinationcompanyvatnumber" AS operation_next_destination_company_vat_number,
    "operationnextdestinationcompanyname" AS operation_next_destination_company_name,
    "operationnextdestinationcompanyaddress" AS operation_next_destination_company_address,
    "operationnextdestinationcompanycontact" AS operation_next_destination_company_contact,
    "operationnextdestinationcompanymail" AS operation_next_destination_company_mail,
    "operationnextdestinationcompanyphone" AS operation_next_destination_company_phone,
    "operationnextdestinationplannedoperationcode" AS operation_next_destination_planned_operation_code,
    "operationnextdestinationcap" AS operation_next_destination_cap,
    "nextpackagingid" AS next_packaging_id,
    other,
    weight / 1000 AS "weight"
FROM
    {{ source('raw_zone_trackdechets', 'bsff_packaging_raw') }}
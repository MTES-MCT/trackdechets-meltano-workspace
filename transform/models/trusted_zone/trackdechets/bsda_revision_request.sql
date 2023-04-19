SELECT
    id                              AS id,
    createdat                       AS created_at,
    updatedat                       AS updated_at,
    bsdaid                          AS bsda_id,
    authoringcompanyid              AS authoring_company_id,
    "comment"                       AS "comment",
    "status"                        AS "status",
    iscanceled                      AS is_canceled,
    wastecode                       AS waste_code,
    wastepop                        AS waste_pop,
    packagings                      AS packagings,
    wastesealnumbers                AS waste_seal_numbers,
    wastematerialname               AS waste_material_name,
    destinationcap                  AS destination_cap,
    destinationreceptionweight      AS destination_reception_weight,
    destinationoperationcode        AS destination_operation_code,
    destinationoperationdescription AS destination_operation_description,
    brokercompanysiret              AS broker_company_siret,
    brokercompanyname               AS broker_company_name,
    brokercompanyaddress            AS broker_company_address,
    brokercompanycontact            AS broker_company_contact,
    brokercompanymail               AS broker_company_mail,
    brokercompanyphone              AS broker_company_phone,
    brokerrecepissenumber           AS broker_recepisse_number,
    brokerrecepissevaliditylimit    AS broker_recepisse_validity_limit,
    brokerrecepissedepartment       AS broker_recepisse_department,
    emitterpickupsitename           AS emitter_pickup_sitename,
    emitterpickupsiteaddress        AS emitter_pickup_site_address,
    emitterpickupsitecity           AS emitter_pickup_site_city,
    emitterpickupsitepostalcode     AS emitter_pickup_site_postal_code,
    emitterpickupsiteinfos          AS emitter_pickup_site_infos
FROM
    {{ source('raw_zone_trackdechets', 'bsda_revision_request_raw') }}

{{ config(
  pre_hook = "{{ create_indexes_for_source(['createdat','updatedat','emittercompanysiret','transportercompanysiret','destinationcompanysiret']) }}"
) }}
SELECT
    id,
    createdat                                           AS "created_at",
    updatedat                                           AS "updated_at",
    isdeleted                                           AS "is_deleted",
    isdraft                                             AS "is_draft",
    status                                              AS "status",
    "type",
    wastecode                                           AS "waste_code",
    wastedescription                                    AS "waste_description",
    wasteadr                                            AS "waste_adr",
    -- Passage en tonnes
    weightisestimate                                    AS "weight_is_estimate",
    emittercompanysiret                                 AS "emitter_company_siret",
    emittercompanyname                                  AS "emitter_company_name",
    emittercompanyaddress                               AS "emitter_company_address",
    emittercompanycontact                               AS "emitter_company_contact",
    emittercompanymail                                  AS "emitter_company_mail",
    emittercompanyphone                                 AS "emitter_company_phone",
    emittercustominfo                                   AS "emitter_custom_info",
    emitteremissionsignaturedate                        AS "emitter_emission_signature_date",
    emitteremissionsignatureauthor                      AS "emitter_emission_signature_author",
    transportercompanyaddress                           AS "transporter_company_address",
    transportercompanycontact                           AS "transporter_company_contact",
    transportercompanymail                              AS "transporter_company_mail",
    transportercompanyname                              AS "transporter_company_name",
    transportercompanyphone                             AS "transporter_company_phone",
    transportercompanysiret                             AS "transporter_company_siret",
    transportercompanyvatnumber                         AS "transporter_company_vat_number",
    transportercustominfo                               AS "transporter_custom_info",
    transporterrecepissedepartment                      AS "transporter_recepisse_department",
    transporterrecepissenumber                          AS "transporter_recepisse_number",
    transporterrecepissevaliditylimit                   AS "transporter_recepisse_validity_limit",
    transportertransportmode                            AS "transporter_transport_mode",
    transportertransportplates                          AS "transporter_transport_plates",
    transportertransportsignatureauthor                 AS "transporter_transport_signature_author",
    transportertransportsignaturedate                   AS "transporter_transport_signature_date",
    transportertransporttakenoverat                     AS "transporter_transport_taken_over_at",
    destinationcompanysiret                             AS "destination_company_siret",
    destinationcompanyaddress                           AS "destination_company_address",
    destinationcompanycontact                           AS "destination_company_contact",
    destinationcompanymail                              AS "destination_company_mail",
    destinationcompanyname                              AS "destination_company_name",
    destinationcompanyphone                             AS "destination_company_phone",
    destinationcustominfo                               AS "destination_custom_info",
    destinationreceptiondate                            AS "destination_reception_date",
    destinationreceptionsignaturedate                   AS "destination_reception_signature_date",
    destinationreceptionsignatureauthor                 AS "destination_reception_signature_author",
    destinationreceptionacceptationstatus               AS "destination_reception_acceptation_status",
    destinationreceptionrefusalreason                   AS "destination_reception_refusal_reason",
    destinationplannedoperationcode                     AS "destination_planned_operation_code",
    destinationoperationcode                            AS "destination_operation_code",
    destinationoperationsignaturedate                   AS "destination_operation_signature_date",
    destinationoperationsignatureauthor                 AS "destination_operation_signature_author",
    destinationoperationnextdestinationcompanysiret     AS "destination_operation_next_destination_company_siret",
    destinationoperationnextdestinationcompanyname      AS "destination_operation_next_destination_company_name",
    destinationoperationnextdestinationcompanyaddress   AS "destination_operation_next_destination_company_address",
    destinationoperationnextdestinationcompanycontact   AS "destination_operation_next_destination_company_contact",
    destinationoperationnextdestinationcompanymail      AS "destination_operation_next_destination_company_mail",
    destinationoperationnextdestinationcompanyphone     AS "destination_operation_next_destination_company_phone",
    destinationoperationnextdestinationcompanyvatnumber AS "destination_operation_next_destination_company_vat_number",
    forwardingid                                        AS "forwarding_id",
    groupedinid                                         AS "grouped_in_id",
    repackagedinid                                      AS "repackaged_in_id",
    weightvalue / 1000                                  AS "weight_value",
    destinationreceptionweight
    / 1000                                              AS "destination_reception_weight"
FROM
    {{ source("raw_zone_trackdechets", "bsff_raw") }}

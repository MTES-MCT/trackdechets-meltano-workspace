{{ config(
  pre_hook = "{{ create_indexes_for_source(['createdat','emittercompanysiret','transportercompanysiret','destinationcompanysiret']) }}"
) }}
SELECT
    id ,
    createdat as "created_at",
    updatedat as "updated_at",
    isdeleted as "is_deleted",
    isdraft as "is_draft",
    status as "status",
    "type",
    wastecode as "waste_code",
    wastedescription as "waste_description",
    wasteadr as "waste_adr",
    weightvalue/1000 as "weight_value", -- Passage en tonnes
    weightisestimate as "weight_is_estimate",
    emittercompanysiret as "emitter_company_siret",
    emittercompanyname as "emitter_company_name",
    emittercompanyaddress as "emitter_company_address",
    emittercompanycontact as "emitter_company_contact",
    emittercompanymail as "emitter_company_mail",
    emittercompanyphone as "emitter_company_phone",
    emittercustominfo as "emitter_custom_info",
    emitteremissionsignaturedate as "emitter_emission_signature_date",
    emitteremissionsignatureauthor as "emitter_emission_signature_author",
    transportercompanyaddress as "transporter_company_address",
    transportercompanycontact as "transporter_company_contact",
    transportercompanymail as "transporter_company_mail",
    transportercompanyname as "transporter_company_name",
    transportercompanyphone as "transporter_company_phone",
    transportercompanysiret as "transporter_company_siret",
    transportercompanyvatnumber as "transporter_company_vat_number",
    transportercustominfo as "transporter_custom_info",
    transporterrecepissedepartment as "transporter_recepisse_department",
    transporterrecepissenumber as "transporter_recepisse_number",
    transporterrecepissevaliditylimit as "transporter_recepisse_validity_limit",
    transportertransportmode as "transporter_transport_mode",
    transportertransportplates as "transporter_transport_plates",
    transportertransportsignatureauthor as "transporter_transport_signature_author",
    transportertransportsignaturedate as "transporter_transport_signature_date",
    transportertransporttakenoverat as "transporter_transport_taken_over_at",
    destinationcompanysiret as "destination_company_siret",
    destinationcompanyaddress as "destination_company_address",
    destinationcompanycontact as "destination_company_contact",
    destinationcompanymail as "destination_company_mail",
    destinationcompanyname as "destination_company_name",
    destinationcompanyphone as "destination_company_phone",
    destinationcustominfo as "destination_custom_info",
    destinationreceptiondate as "destination_reception_date",
    destinationreceptionweight/1000 as "destination_reception_weight",
    destinationreceptionsignaturedate as "destination_reception_signature_date",
    destinationreceptionsignatureauthor as "destination_reception_signature_author",
    destinationreceptionacceptationstatus as "destination_reception_acceptation_status",    
    destinationreceptionrefusalreason as "destination_reception_refusal_reason",
    destinationplannedoperationcode as "destination_planned_operation_code",
    destinationoperationcode as "destination_operation_code",
    destinationoperationsignaturedate as "destination_operation_signature_date",
    destinationoperationsignatureauthor as "destination_operation_signature_author",
    destinationoperationnextdestinationcompanysiret as "destination_operation_next_destination_company_siret",
    destinationoperationnextdestinationcompanyname as "destination_operation_next_destination_company_name",
    destinationoperationnextdestinationcompanyaddress as "destination_operation_next_destination_company_address",
    destinationoperationnextdestinationcompanycontact as "destination_operation_next_destination_company_contact",
    destinationoperationnextdestinationcompanymail as "destination_operation_next_destination_company_mail",
    destinationoperationnextdestinationcompanyphone as "destination_operation_next_destination_company_phone",
    destinationoperationnextdestinationcompanyvatnumber as "destination_operation_next_destination_company_vat_number",
    forwardingid as "forwarding_id",
    groupedinid as "grouped_in_id",
    repackagedinid as "repackaged_in_id"
FROM
   {{ source("raw_zone_trackdechets", "bsff_raw") }}

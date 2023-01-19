SELECT
    id as "id",
    createdat as "created_at",
    updatedat as "updated_at",
    status as "status",
    "type",
    isdeleted as "is_deleted",
    isdraft as "is_draft",
    wasteadr as "waste_adr",
    wastecode as "waste_code",
    identificationnumbers as "identification_numbers",
    emittercompanysiret as "emitter_company_siret",
    emittercompanyname as "emitter_company_name",
    emittercompanyaddress as "emitter_company_address",
    emittercompanycontact as "emitter_company_contact",
    emittercompanymail as "emitter_company_mail",
    emittercompanyphone as "emitter_company_phone",
    emittercustominfo as "emitter_custom_info",
    emitterpickupsitename as "emitter_pickup_sitename",
    emitterpickupsiteaddress as "emitter_pickup_site_address",
    emitterpickupsitepostalcode as "emitter_pickup_site_postal_code",
    emitterpickupsitecity as "emitter_pickup_site_city",
    emitterpickupsiteinfos as "emitter_pickup_site_infos",
    emitteremissionsignaturedate as "emitter_emission_signature_date",
    emitteremissionsignatureauthor as "emitter_emission_signature_author",
    emittedbyecoorganisme as "emitted_by_eco_organisme",
    emitterwastevolume as "emitter_waste_volume",
    emitterwasteweightvalue as "emitter_waste_weight_value",
    emitterwasteweightisestimate as "emitter_waste_weight_is_estimate",
    emitterwastepackagings as "emitter_waste_packagings",
    emissionsignatoryid as "emission_signatory_id",
    isemissiondirecttakenover as "is_emission_direct_taken_over",
    isemissiontakenoverwithsecretcode as "is_emission_taken_over_with_secret_code",
    transportercompanysiret as "transporter_company_siret",
    transportercompanyname as "transporter_company_name",
    transportercompanyaddress as "transporter_company_address",
    transportercompanycontact as "transporter_company_contact",
    transportercompanymail as "transporter_company_mail",
    transportercompanyphone as "transporter_company_phone",
    transportercompanyvatnumber as "transporter_company_vat_number",
    transportercustominfo as "transporter_custom_info",
    transportsignatoryid as "transport_signatory_id",
    transporterrecepissenumber as "transporter_recepisse_number",
    transporterrecepissedepartment as "transporter_recepisse_department",
    transporterrecepissevaliditylimit as "transporter_recepisse_validity_limit",
    transportertransportmode as "transporter_transport_mode",
    transportertransportplates as "transporter_transport_plates",
    transporteracceptationstatus as "transporter_acceptation_status",
    transportertakenoverat as "transporter_taken_over_at",
    transportertransportsignaturedate as "transporter_transport_signature_date",
    transportertransportsignatureauthor as "transporter_transport_signature_author",
    transporterwasteweightvalue/1000 as "transporter_waste_weight_value",
    transporterwastevolume as "transporter_waste_volume",
    transporterwasteweightisestimate as "transporter_waste_weight_is_estimate",
    transporterwastepackagings as "transporter_waste_packagings",
    transporterwasterefusalreason as "transporter_waste_refusal_reason",
    transporterwasterefusedweightvalue as "transporter_waste_refused_weight_value",
    destinationcompanysiret as "destination_company_siret",
    destinationcompanyname as "destination_company_name",
    destinationcompanyaddress as "destination_company_address",
    destinationcompanycontact as "destination_company_contact",
    destinationcompanymail as "destination_company_mail",
    destinationcompanyphone as "destination_company_phone",
    destinationcustominfo as "destination_custom_info",
    destinationreceptiondate as "destination_reception_date",
    destinationreceptionsignaturedate as "destination_reception_signature_date",
    destinationreceptionsignatureauthor as "destination_reception_signature_author",
    destinationwastepackagings as "destination_waste_packagings",
    destinationreceptionwastevolume as "destination_reception_waste_volume",
    destinationreceptionwasteweightvalue/1000 as "destination_reception_waste_weight_value",
    destinationreceptionwasterefusalreason as "destination_reception_waste_refusal_reason",
    destinationreceptionwasterefusedweightvalue/1000 as "destination_reception_waste_refused_weight_value",
    receptionsignatoryid as "reception_signatory_id",
    destinationoperationdate as "destination_operation_date",
    destinationoperationcode as "destination_operation_code",
    destinationoperationsignaturedate as "destination_operation_signature_date",
    destinationoperationsignatureauthor as "destination_operation_signature_author",
    operationsignatoryid as "operation_signatory_id",
    destinationreceptionacceptationstatus as "destination_reception_acceptation_status",
    handedovertorecipientat as "handed_over_to_recipient_at",
    ecoorganismesiret as "eco_organisme_siret",
    ecoorganismename as "eco_organisme_name",
    groupedinid as "grouped_in_id",
    synthesizedinid as "synthesized_in_id"
FROM
   {{ source("raw_zone_trackdechets", "bsdasri_raw") }}

{{ config(
  pre_hook = "{{ create_indexes_for_source(['formid','createdat','updatedat','transportercompanysiret','takenoverat']) }}"
) }}

select
    id,
    createdat                       as created_at,
    updatedat                       as updated_at,
    "number",
    formid                          as form_id,
    readytotakeover                 as ready_to_take_over,
    takenoverat                     as taken_over_at,
    takenoverby                     as taken_over_by,
    transportercompanysiret         as transporter_company_siret,
    transportercompanyvatnumber     as transporter_company_vat_number,
    transportercompanyname          as transporter_company_name,
    transportercompanyaddress       as transporter_company_address,
    transportercompanycontact       as transporter_company_contact,
    transportercompanymail          as transporter_company_mail,
    transportercompanyphone         as transporter_company_phone,
    transportercustominfo           as transporter_custom_info,
    transporterdepartment           as transporter_department,
    transporterisexemptedofreceipt  as transporter_isexempted_of_receipt,
    transporternumberplate          as transporter_number_plate,
    transporterreceipt              as transporter_receipt,
    transportertransportmode        as transporter_transport_mode,
    transportervaliditylimit        as transporter_validity_limit,
    previoustransportercompanyorgid as previous_transporter_company_org_id
from
    {{ source("raw_zone_trackdechets","bsdd_transporter_raw") }}

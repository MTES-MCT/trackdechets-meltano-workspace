select
    id,
    "codeS3ic" as code_s3ic,
    volume::float as volume,
    unite,
    to_date(date_debut_exploitation,'DD/MM/YYYY') as date_debut_exploitation,
    to_date(date_fin_validite,'DD/MM/YYYY') as date_fin_validite,
    statut_ic::bool,
    id_ref_nomencla_ic as id_nomenclature,
    inserted_at
from
    {{ source('raw_zone_icpe', 'installations_classees') }}
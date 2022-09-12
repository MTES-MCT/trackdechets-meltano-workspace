select
    "codeS3ic",
    id_ref_nomencla_ic,
    to_date(date_fin_validite, 'DD/MM/YYYY') as date_fin_validite
from
    raw_zone.ic_installation_classee iic
where
    "date_fin_validite" is null or to_date(date_fin_validite, 'DD/MM/YYYY')>current_date
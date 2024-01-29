{{
  config(
    materialized = 'table',
    indexes=[{"columns":["code_aiot"]},{"columns":["siret"]},{"columns":["rubrique"]}]
    )
}}

select
    ii.*,
    idpw.day_of_processing,
    idpw.quantite_traitee
from {{ ref('installations_icpe') }} as ii
left join {{ ref('installations_daily_processed_wastes') }} as idpw
    on ii.siret = idpw.siret and ii.rubrique = idpw.rubrique

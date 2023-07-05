select
    is2.*,
    r.rubrique_ic as rubrique,
    r.alinea      as alinea,
    r.quantite_totale,
    r.unite,
    r.nature,
    r.id_nomenclature_regime
from
    {{ ref('installations_siretise') }} as is2
left join {{ ref('rubriques') }} as r on
    is2.code_aiot = r.code_aiot

select
    code_aiot,
    alinea,
    rubrique_ic,
    quantite_totale::float,
    unite,
    nature,
    id_nomenclature_regime
from
    {{ source('raw_zone_icpe', 'rubriques') }}

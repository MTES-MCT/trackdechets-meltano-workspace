select
    id,
    rubrique_ic as rubrique,
    famille_ic as famille,
    sfamille_ic as s_famille,
    ssfamille_ic as ss_famille,
    alinea, 
    libellecourt_activite as libelle_court_activite,
    id_regime,
    envigueur::bool as en_vigueur,
    ippc::bool as ippc,
    inserted_at
from
    {{ source('raw_zone_icpe', 'nomenclature') }}
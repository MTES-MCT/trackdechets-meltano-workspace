select
    td.dep,
    100*(td."num_etabs"::float / sirene."num_etabs"::float) as prop_etab_inscrits_td_sur_sirene
from
    {{ref('etablissements_inscrits_par_departements')}} td
inner join {{ref('etablissements_par_departements')}} sirene on
    td.dep = sirene.dep
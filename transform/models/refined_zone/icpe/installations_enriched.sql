select
    coalesce(
        etabs.siret, gerep.numero_siret
    ) as siret,
    coalesce(
        max(etabs.nom_etablissement), max(gerep.nom_etablissement)
    ) as nom_etablissement,
    coalesce(
        max(etabs.code_commune_etablissement), max(gerep.code_commune_insee)
    ) as code_commune_etablissement,
    array_agg(
        distinct ic.code_s3ic
    ) as codes_installations,
    array_agg(
        distinct n.rubrique || coalesce('-' || alinea, '')
    ) as rubriques_autorisees,
    max(
        bcp.latitude
    ) as latitude_etablissement,
    max(
        bcp.longitude
    ) as longitude_etablissement
from
    {{ ref('etablissements') }} as etabs
inner join {{ ref('installations_classees') }} as ic
    on
        ic.code_s3ic = etabs.code_s3ic
inner join {{ ref('nomenclature') }} as n
    on
        n.id = ic.id_nomenclature
left join {{ ref('gerep_traiteurs') }} as gerep
    on
        ic.code_s3ic = '0' || gerep.code_s3ic
        and length(gerep.numero_siret) = 14
left join
    {{ ref('base_codes_postaux') }} as bcp
    on
        bcp.code_commune_insee
        = coalesce(etabs.code_commune_etablissement, gerep.code_commune_insee)
where
    ic.statut_ic
group by
    coalesce(etabs.siret, gerep.numero_siret)
having
    coalesce(etabs.siret, gerep.numero_siret) is not null

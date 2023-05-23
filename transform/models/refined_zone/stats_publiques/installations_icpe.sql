select
    siret,
    max(nom_etablissement)                          as nom_etablissement,
    max(
        code_commune_etablissement
    )                                               as code_commune_etablissement,
    max(array_to_string(codes_installations, ','))  as codes_installations,
    max(array_to_string(rubriques_autorisees, ',')) as rubriques_autorisees,
    max(latitude_etablissement)                     as latitude_etablissement,
    max(longitude_etablissement)                    as longitude_etablissement,
    sum(quantity_received)                          as waste_quantity_received
from
    {{ ref('installations_enriched') }} as ie
left join
    {{ ref('bordereaux_enriched') }} as be
    on
        ie.siret = be.destination_company_siret
where
    rubriques_autorisees && array[
        '2790',
        '2770',
        '2760-1'
    ]
    and latitude_etablissement is not null
    and longitude_etablissement is not null
group by ie.siret

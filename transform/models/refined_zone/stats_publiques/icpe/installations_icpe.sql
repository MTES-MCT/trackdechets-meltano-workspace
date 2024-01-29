{{
  config(
    materialized = 'table',
    indexes=[{"columns":["code_aiot"]},{"columns":["siret"]},{"columns":["rubrique"]}]
    )
}}

with cp_data as (
    select
        code_postal,
        max(code_commune_insee) as code_commune_insee,
        avg(latitude)           as latitude,
        avg(longitude)          as longitude
    from
        {{ ref('base_codes_postaux') }}
    group by
        code_postal
),

installations as (
    select
        ir.code_aiot,
        ir.raison_sociale,
        ir.siret           as siret,
        ir.etat_activite,
        ir.regime,
        ir.quantite_totale as quantite_autorisee,
        ir.unite,
        ir.latitude        as latitude,
        ir.longitude       as longitude,
        ir.adresse1        as adresse1,
        ir.adresse2        as adresse2,
        ir.code_postal     as code_postal,
        ir.commune         as commune,
        ir.code_insee      as code_insee,
        rubrique || coalesce(
            '-' || alinea,
            ''
        )                  as rubrique
    from
        {{ ref('installations_rubriques') }} as ir
)

select
    i.code_aiot,
    i.raison_sociale,
    i.siret,
    i.etat_activite,
    i.regime,
    i.rubrique,
    i.quantite_autorisee,
    i.unite,
    i.adresse1,
    i.adresse2,
    i.code_postal,
    i.commune,
    cgc.code_departement as code_departement_insee,
    cgc.code_region      as code_region_insee,
    coalesce(
        i.latitude,
        bcp.latitude
    )                    as latitude,
    coalesce(
        i.longitude,
        bcp.longitude
    )                    as longitude,
    coalesce(
        i.code_insee,
        bcp.code_commune_insee
    )                    as code_commune_insee
from
    installations as i
left join cp_data as bcp
    on
        i.code_postal::integer = bcp.code_postal
left join {{ ref('code_geo_communes') }} as cgc on
    coalesce(
        i.code_insee,
        bcp.code_commune_insee
    ) = cgc.code_commune
    and cgc.type_commune = 'COM'

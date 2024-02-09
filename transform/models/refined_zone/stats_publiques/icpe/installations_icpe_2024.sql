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
        ir.quantite_totale as quantite_autorisee,
        ir.unite,
        ir.libelle_etat_site,
        i.latitude         as latitude,
        i.longitude        as longitude,
        i.adresse1         as adresse1,
        i.adresse2         as adresse2,
        i.code_postal      as code_postal,
        i.commune          as commune,
        i.code_insee       as code_insee,
        ir.rubrique
    from
        {{ ref('installations_rubriques_2024') }} as ir
    left join {{ ref('installations') }} as i on ir.code_aiot = i.code_aiot
    where
        (ir.libelle_etat_site not in ('Non construit', 'En construction', 'Projet abandonné', 'Sans titre', 'A l’arrêt') or ir.libelle_etat_site is null) -- noqa: LXR
        and (ir.etat_administratif_rubrique = 'En vigueur')
)

select
    i.code_aiot,
    i.raison_sociale,
    i.siret,
    i.rubrique,
    i.quantite_autorisee,
    i.unite,
    i.libelle_etat_site,
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

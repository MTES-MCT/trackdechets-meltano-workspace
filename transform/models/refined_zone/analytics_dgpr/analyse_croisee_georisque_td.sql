{{
  config(
    materialized = 'table',
    )
}}

with rubriques_data as (
    select
        ir.code_aiot,
        ir.siret,
        ir.quantite_totale,
        ir.etat_activite,
        ir.rubrique || coalesce(
            '-' || ir.alinea,
            ''
        ) as rubrique,
        coalesce(
            code_insee,
            se.code_commune_etablissement
        ) as "code_commune_insee"
    from
        {{ ref('installations_rubriques') }} as ir
    left join {{ ref('stock_etablissement') }} as se
        on
            ir.siret = se.siret
    where
        (
            rubrique = '2770'
            or (
                rubrique = '2760'
                and alinea = '1'
            )
        )
        and coalesce(
            code_insee,
            se.code_commune_etablissement
        ) is not null
),

wastes_data as (
    select
        b.destination_company_siret        as siret,
        mrco.rubrique,
        sum(b.quantity_received)           as quantite_traitee_2023_td,
        max(se.code_commune_etablissement) as code_commune_insee
    from
        {{ ref('bordereaux_enriched') }} as b
    left join {{ ref('referentiel_codes_operation_rubriques') }} as mrco
        on
            b.processing_operation = mrco.code_operation
    left join {{ ref('stock_etablissement') }} as se
        on
            b.destination_company_siret = se.siret
    where
        date_part(
            'year',
            b.processed_at
        ) = 2023
        and (
            b.waste_code ~* '.*\*$'
            or coalesce(
                b.waste_pop,
                false
            )
            or coalesce(
                b.waste_is_dangerous,
                false
            )
        )
        and mrco.rubrique in ('2760-1', '2770', '2790')
    group by
        b.destination_company_siret,
        mrco.rubrique
)

select
    r.code_aiot,
    r.siret                          as siret_georisque,
    w.siret                          as siret_td,
    cgc.code_region,
    cgc.code_departement,
    r.etat_activite                  as etat_activite_georisque,
    r.quantite_totale                as quantite_autorisee_georisque,
    w.quantite_traitee_2023_td,
    coalesce(r.rubrique, w.rubrique) as rubrique,
    case
        when
            (coalesce(r.siret, r.rubrique) is null) and (w.siret is not null)
            then 'Présent uniquement sur TD'
        when
            (coalesce(r.siret, r.rubrique) is not null) and (w.siret is null)
            then 'Présent uniquement sur Géorisques'
        when
            (coalesce(r.siret, r.rubrique) is null) and (w.siret is null)
            then 'Absent TD et Géorisque'
        when
            (coalesce(r.siret, r.rubrique) is not null)
            and (w.siret is not null)
            then 'Présent TD et Géorisque'
    end                              as "statut_donnee"
from
    rubriques_data as r
full outer join wastes_data as w
    on
        r.siret = w.siret
        and r.rubrique = w.rubrique
left join
    {{ ref('code_geo_communes') }} as cgc
    on coalesce(r.code_commune_insee, w.code_commune_insee) = cgc.code_commune

{{
  config(
    materialized = 'table',
    indexes = [{'columns':['siret'],'unique':true}]
    )
}}

with bordereaux_data as (
    select
        destination_company_siret,
        max(destination_company_name) as "name",
        sum(quantity_received) filter (
            where "_bs_type" = 'BSDD'
        ) as quantity_bsdd,
        sum(quantity_received) filter (
            where "_bs_type" = 'BSDA'
        ) as quantity_bsda,
        array_agg(id),
        array_agg(readable_id)
    from
        {{ ref('bordereaux_enriched') }}
    where
        processing_operation = 'D5'
        and (
            waste_code ~* '.*\*$'
            or waste_pop
            or waste_is_dangerous
        )
        and "_bs_type" in ('BSDD', 'BSDA')
    group by
        destination_company_siret
)

select
    destination_company_siret as siret,
    max(b.name) as "nom_etablissement",
    max(b.quantity_bsdd)      as quantite_bsdd_traitee_en_d5,
    max(b.quantity_bsda)      as quantite_bsda_traitee_en_d5,
    case
        when count(ir.siret) = 0 then 'Pas de données ICPE'
        else 'Données ICPE mais pas de rubrique 2760-1'
    end                       as statut_icpe
from
    bordereaux_data as b
left join {{ ref('installations_rubriques') }} as ir
    on
        ir.siret = b.destination_company_siret
where
    (
        ir.siret is null
        or (
            ir.rubrique != '2760'
            and ir.alinea != '1'
        )
    )
group by destination_company_siret

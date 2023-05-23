{{
  config(
    materialized = 'table',
    indexes = [{'columns': ['siret'] , 'unique': True },]
    )
}}


with icpe_data as (
    select
        siret_clean                   as siret,
        array_agg(distinct code_s3ic) as codes_s3ic,
        max(nom_etablissement_icpe)   as nom_etablissement,
        array_agg(
            distinct rubrique || coalesce('-' || alinea, '')
        )                             as rubriques_autorises
    from
        {{ ref('icpe_siretise') }}
    where
        (
            (rubrique in ('2770', '2790'))
            or (
                rubrique = '2760'
                and alinea = '1'
            )
        )
        and (
            en_vigueur is true
            or en_vigueur is null
        )
        and (etat_administratif_etablissement != 'F')
    group by
        siret
),

bsdd_data as (
    select
        b.recipient_company_siret                       as siret,
        max(b.recipient_company_name)                   as company_name,
        array_agg(distinct b.processing_operation_done) as code_operation,
        count(id)                                       as nombre_bordereaux,
        sum(quantity_received)                          as quantite
    from
        {{ ref('bsdd') }} as b
    where
        b.processing_operation_done not in (
            'D13',
            'D14',
            'D15',
            'R12',
            'R13'
        )
    group by
        siret
    order by
        siret
),

bsda_data as (
    select
        bsda.destination_company_siret                      as siret,
        max(bsda.destination_company_name)                  as company_name,
        array_agg(distinct bsda.destination_operation_code) as code_operation,
        count(
            id
        )                                                   as nombre_bordereaux,
        sum(destination_reception_weight)                   as quantite
    from
        {{ ref('bsda') }}
    where
        bsda.destination_operation_code in ('D5', 'R5')
    group by
        siret
    order by
        siret
),

bsff_data as (
    select
        bsff.destination_company_siret                      as siret,
        max(bsff.destination_company_name)                  as company_name,
        array_agg(distinct bsff.destination_operation_code) as code_operation,
        count(
            distinct bsff.id
        )                                                   as nombre_bordereaux,
        sum(pack.volume)                                    as packaging_volume,
        sum(pack.weight)                                    as packaging_weight,
        sum(
            pack.acceptation_weight
        )                                                   as packaging_acceptation_weight
    from
        {{ ref('bsff') }} as bsff
    inner join {{ ref('bsff_packaging') }} as pack on bsff.id = pack.bsff_id
    where
        pack.operation_code in ('R1', 'R2', 'R3', 'R5', 'D10')
    group by
        siret
    order by
        siret
)

select
    max(case
        when c.siret is not null then 1
        else 0
    end)::bool                         as "inscrit_sur_trackdechets",
    coalesce(
        icpe_data.siret,
        bsdd_data.siret,
        bsda_data.siret,
        bsff_data.siret,
        c.siret
    )                                  as siret,
    max(
        coalesce(
            icpe_data.nom_etablissement,
            c.name,
            bsdd_data.company_name,
            bsda_data.company_name,
            bsff_data.company_name
        )
    )                                  as nom_etablissement,
    max(icpe_data.codes_s3ic)          as codes_s3ic,
    max(icpe_data.rubriques_autorises) as rubriques_autorises,
    max(bsdd_data.nombre_bordereaux)   as "num_bsdd",
    max(bsdd_data.quantite)            as "tonnage_bsdd",
    max(bsdd_data.code_operation)      as "operations_effectues_bsdd",
    max(bsda_data.nombre_bordereaux)   as "num_bsda",
    max(bsda_data.quantite)            as "tonnage_bsda",
    max(bsda_data.code_operation)      as "operations_effectues_bsda",
    max(bsff_data.nombre_bordereaux)   as "num_bsff",
    max(bsff_data.packaging_volume)    as "volume_contenant_bsff",
    max(bsff_data.packaging_weight)    as "tonnage_contenant_bsff",
    max(
        bsff_data.packaging_acceptation_weight
    )                                  as "tonnage_accepte_contenant_bsff",
    max(bsff_data.code_operation)      as "operations_effectues_bsff"
from
    icpe_data
full outer join
    bsdd_data
    on
        icpe_data.siret = bsdd_data.siret
full outer join
    bsda_data
    on
        bsdd_data.siret = bsda_data.siret
full outer join
    bsff_data
    on
        bsff_data.siret = bsda_data.siret
left join {{ ref('company') }} as c
    on
        coalesce(icpe_data.siret, bsdd_data.siret, bsda_data.siret)
        = c.siret
group by
    coalesce(
        icpe_data.siret,
        bsdd_data.siret,
        bsda_data.siret,
        bsff_data.siret,
        c.siret
    )

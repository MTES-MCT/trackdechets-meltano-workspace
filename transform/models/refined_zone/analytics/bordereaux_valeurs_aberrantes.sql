{{
  config(
    materialized = 'table',
    indexes = [{'columns':['siret'],'unique':true}]
    )
}}

with bs_data as (
    select
        destination_company_siret,
        emitter_company_siret,
        eco_organisme_siret,
        worker_company_siret,
        readable_id,
        quantity_received,
        "_bs_type",
        received_at
    from
        {{ ref('bordereaux_enriched') }}
    where
        ((
            "_bs_type" in ('BSDD', 'BSDA')
            and quantity_received > 40
            and transport_mode = 'ROAD'
        )
        or (
            "_bs_type" = 'BSDASRI'
            and quantity_received > 20
            and transport_mode = 'ROAD'
        )
        or (
            "_bs_type" = 'BSVHU'
            and quantity_received > 40
        )
        or (
            "_bs_type" = 'BSFF'
            and quantity_received > 20
        ))
        and received_at BETWEEN '2023-01-01' AND '2023-06-30'
),

recipients_data as (
    select
        destination_company_siret as siret,
        'DESTINATAIRE'            as company_type,
        jsonb_agg(json_build_object(
            'readable_id',
            readable_id,
            'quantity',
            quantity_received,
            'bordereau_type',
            "_bs_type",
            'received_at',
            received_at
        ))                        as bs_data_json
    from
        bs_data
    group by
        destination_company_siret
),

emitters_data as (
    select
        emitter_company_siret as siret,
        'PRODUCTEUR'          as company_type,
        jsonb_agg(json_build_object(
            'readable_id',
            readable_id,
            'quantity',
            quantity_received,
            'bordereau_type',
            "_bs_type",
            'received_at',
            received_at
        ))                    as bs_data_json
    from
        bs_data
    group by
        emitter_company_siret
),

workers_company_data as (
    select
        worker_company_siret    as siret,
        'ENTREPRISE DE TRAVAUX' as company_type,
        jsonb_agg(json_build_object(
            'readable_id',
            readable_id,
            'quantity',
            quantity_received,
            'bordereau_type',
            "_bs_type",
            'received_at',
            received_at
        ))                      as bs_data_json
    from
        bs_data
    group by
        worker_company_siret
),

eo_data as (
    select
        eco_organisme_siret as siret,
        'ECO-ORGANISME'     as company_type,
        jsonb_agg(json_build_object(
            'readable_id',
            readable_id,
            'quantity',
            quantity_received,
            'bordereau_type',
            "_bs_type",
            'received_at',
            received_at
        ))                  as bs_data_json
    from
        bs_data
    group by
        eco_organisme_siret
),

merged_data as (
    select
        coalesce(
            r.siret,
            e.siret,
            w.siret,
            eo.siret
        )     as siret,
        array_remove(array[
            r.company_type,
            e.company_type,
            w.company_type,
            eo.company_type
        ],
        null) as company_types,
        (
            select jsonb_agg(distinct bd.bordereau)
            from
                jsonb_array_elements(
                    coalesce(
                        r.bs_data_json,
                        '{}'::jsonb
                    )
                    || coalesce(
                        e.bs_data_json,
                        '{}'::jsonb
                    )
                    || coalesce(
                        w.bs_data_json,
                        '{}'::jsonb
                    )
                    || coalesce(
                        eo.bs_data_json,
                        '{}'::jsonb
                    )
                ) as bd (bordereau)
            where
                bd.bordereau != '{}'::jsonb
        )     as bordereaux
    from
        recipients_data as r
    full outer join emitters_data as e
        on r.siret = e.siret
    full outer join workers_company_data as w
        on coalesce(r.siret, e.siret) = w.siret
    full outer join eo_data as eo
        on coalesce(r.siret, e.siret, w.siret) = eo.siret
),

admins as (
    select
        ca.company_siret as siret,
        ca.company_name,
        ca.user_email,
        row_number() over (
            partition by ca.company_siret
            order by
                ca.user_created_at asc
        )                as rn
    from
        {{ ref('companies_admins') }} as ca
)

select
    m.siret,
    m.company_types,
    m.bordereaux,
    admins.company_name,
    admins.user_email,
    jsonb_array_length(m.bordereaux) as num_bordereaux
from
    merged_data as m
left join admins
    on m.siret = admins.siret
where
    admins.rn = 1

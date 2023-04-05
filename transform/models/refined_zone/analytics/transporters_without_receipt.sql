{{
  config(
    materialized = 'table',
    )
}}

select distinct transporter_company_siret
from
    {{ ref('bordereaux_enriched') }} as be
inner join {{ ref('company') }} as c on be.transporter_company_siret = c.siret
where
    be.created_at >= now() - interval '3 months'
    and c.company_types ? 'TRANSPORTER'
    and transporter_receipt_id is null

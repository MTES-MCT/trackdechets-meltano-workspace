{{
  config(
    materialized = 'table',
    )
}}

select
  emitter_departement as departement_origine, 
  destination_departement as departement_destination,
  date_trunc('week', be.processed_at) as semaine,
  sum(
    case 
      when be.quantity_received > 60 then quantity_received/1000
      else quantity_received end 
  ) as quantite_traitee
from {{ ref('bordereaux_enriched') }} be
where {{ dangerous_waste_filter('bordereaux_enriched')}}
and date_trunc('week', be.processed_at) >= '2020-01-01'
and emitter_departement is not null
and destination_departement is not null
and quantity_received is not null
group by 1,2,3
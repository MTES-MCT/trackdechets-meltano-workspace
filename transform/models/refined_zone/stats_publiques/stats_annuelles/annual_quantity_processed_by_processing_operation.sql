select
    date_part('year', processed_at)::int as annee,
    processing_operation                 as code_operation_traitement,
    sum(case
        when quantity_received > 60 then quantity_received / 1000
        else quantity_received
    end)                                 as quantite_traitee
from
    {{ ref('bordereaux_enriched') }}
where
    /* Uniquement déchets dangereux */
    (
        waste_code ~* '.*\*$'
        or coalesce(waste_pop, false)
        or coalesce(waste_is_dangerous, false)
    )
    /* Pas de bouillons */
    and status != 'DRAFT'
    /* Uniquement les données jusqu'à la dernière semaine complète */
    and processed_at < date_trunc('week', now())
group by
    date_part('year', processed_at),
    processing_operation
order by annee desc, processing_operation asc

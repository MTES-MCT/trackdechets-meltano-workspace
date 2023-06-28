select
    _bs_type as type_bordereau,
    extract(
        'year' from date_trunc('year', processed_at)
    ) as annee_traitement,
    sum(
        case
            when quantity_received > 60 then quantity_received / 1000
            else quantity_received
        end
    ) as quantite_traitee
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
    /* Uniquement codes opérations finales */
    and processing_operation not in (
        'D9',
        'D13',
        'D14',
        'D15',
        'R12',
        'R13'
    )
    /* Filtrer les quantités aberrantes */
    and quantity_received < 100000
    /* Uniquement les données jusqu'à la dernière semaine complète */
    and processed_at < date_trunc('week', now())
group by
    date_trunc('year', processed_at),
    _bs_type
order by "year_of_processing" desc

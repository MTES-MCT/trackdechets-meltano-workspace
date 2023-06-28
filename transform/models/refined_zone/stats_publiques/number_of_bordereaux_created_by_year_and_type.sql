select
    _bs_type as bordereau_type,
    extract('year' from date_trunc('year', created_at)) as annee,
    count(id) as creations
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
    and created_at < date_trunc('week', now())
group by
    date_trunc('year', created_at),
    _bs_type
order by "year_of_creation" desc

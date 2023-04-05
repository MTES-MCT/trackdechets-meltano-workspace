select
    _bs_type,
    extract('year' from date_trunc('year', created_at)) as "year",
    count(id)
from
    {{ ref('bordereaux_enriched') }}
where
    (
        waste_code ~* '.*\*$'
        or coalesce(waste_pop, false)
        or coalesce(waste_is_dangerous, false)
    )
    and status != 'DRAFT'
group by
    date_trunc('year', created_at),
    _bs_type
order by "year" desc

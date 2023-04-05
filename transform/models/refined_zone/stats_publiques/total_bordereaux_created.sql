select
    extract ( 'year' from date_trunc('year', created_at)) as "year",
    _bs_type,
    count(id)
from
    refined_zone_enriched.bordereaux_enriched be
where
    (waste_code ~* '.*\*$'
    or coalesce (waste_pop, false)
    or coalesce (waste_is_dangerous, false))
    and status != 'DRAFT'
group by
    date_trunc('year', created_at),
    _bs_type
order by "year" desc
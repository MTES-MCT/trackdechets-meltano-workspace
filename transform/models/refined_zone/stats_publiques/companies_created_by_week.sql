select
    DATE_TRUNC('week', created_at) as "week_of_creation",
    COUNT(id)
from
    {{ ref('company') }}
where
    created_at < DATE_TRUNC('week', NOW())
group by
    DATE_TRUNC('week', created_at)
order by
    DATE_TRUNC('week', created_at) desc

select
    DATE_TRUNC('week', created_at) as "week_of_creation",
    COUNT(id)
from
    {{ ref('user') }}
where
    created_at < DATE_TRUNC('week', NOW())
    and is_active
group by
    DATE_TRUNC('week', created_at)
order by
    DATE_TRUNC('week', created_at) desc

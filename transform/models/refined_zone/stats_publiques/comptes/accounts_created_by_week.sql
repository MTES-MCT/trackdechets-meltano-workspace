select 
    coalesce (ucbw.semaine,
    ccbw.semaine) as semaine,
    ucbw.creations as comptes_utilisateurs,
    ccbw.creations as comptes_etablissements
from
    {{ ref('accounts_created_by_week') }} ucbw
full outer join {{ ref('companies_created_by_week') }} ccbw
        using (semaine)
order by
    coalesce (ucbw.semaine,
    ccbw.semaine) desc
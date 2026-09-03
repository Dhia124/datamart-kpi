-- Vue actionnable : ce qui relève de la production ferroviaire et ce qui n'en
-- relève pas, par mois et par liaison.
select
    c.mois_id,
    c.liaison_id,
    d.libelle_liaison,
    c.cause_famille,
    sum(c.nb_trains_attribues)                         as nb_trains_attribues,
    round(sum(c.pct_retard), 2)                        as pct_retard_famille
from {{ ref('stg_causes') }} c
join {{ source('mart', 'dim_liaison') }} d using (liaison_id)
group by 1, 2, 3, 4

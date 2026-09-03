select
    fc.mois_id,
    fc.liaison_id,
    fc.cause_id,
    c.libelle       as cause_libelle,
    c.famille       as cause_famille,
    fc.pct_retard,
    fc.nb_trains_attribues
from {{ source('mart', 'fait_retard_cause') }} fc
join {{ source('mart', 'dim_cause') }} c using (cause_id)

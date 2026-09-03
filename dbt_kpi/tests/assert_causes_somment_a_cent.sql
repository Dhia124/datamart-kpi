-- Règle métier : pour toute liaison-mois ayant au moins un train en retard,
-- la ventilation des causes doit sommer à 100 % à la tolérance près.
-- Les lignes qui violaient cette règle en amont sont en table de rejets ;
-- ce test garantit qu'aucune n'a franchi le filtre.
with somme as (
    select c.mois_id, c.liaison_id, sum(c.pct_retard) as total
    from {{ ref('stg_causes') }} c
    group by 1, 2
)
select s.*
from somme s
join {{ ref('stg_regularite') }} r
  on r.mois_id = s.mois_id and r.liaison_id = s.liaison_id
where r.nb_retard_arrivee > 0
  and abs(s.total - 100) > {{ var('ecart_causes_tolere_pct') }}

-- Intégrité référentielle du modèle en étoile : aucun fait ne doit pointer
-- vers une liaison absente de la dimension.
select f.mois_id, f.liaison_id
from {{ source('mart', 'fait_regularite') }} f
left join {{ source('mart', 'dim_liaison') }} d using (liaison_id)
where d.liaison_id is null

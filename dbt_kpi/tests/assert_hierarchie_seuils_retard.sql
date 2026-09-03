-- Règle métier : les seuils de retard sont cumulatifs. Un train en retard de
-- plus de 60 minutes l'est aussi de plus de 30, et de plus de 15.
-- Hypothèse validée sur les données : 8 691 lignes violent la lecture par
-- tranches disjointes contre 357 la lecture cumulative.
select mois_id, liaison_id, nb_retard_sup_15, nb_retard_sup_30, nb_retard_sup_60
from {{ ref('stg_regularite') }}
where nb_retard_sup_15 < nb_retard_sup_30
   or nb_retard_sup_30 < nb_retard_sup_60
   or nb_retard_sup_15 > nb_retard_arrivee

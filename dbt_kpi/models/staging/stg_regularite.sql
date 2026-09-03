-- Vue de préparation : typage explicite et exclusion de la période de
-- restriction sanitaire des comparaisons annuelles (le drapeau est conservé,
-- les lignes ne sont pas supprimées).
select
    f.mois_id,
    f.liaison_id,
    t.annee,
    t.trimestre,
    t.est_periode_covid,
    f.nb_prevu,
    f.nb_annule,
    f.nb_circule,
    f.nb_retard_arrivee,
    f.nb_retard_sup_15,
    f.nb_retard_sup_30,
    f.nb_retard_sup_60,
    f.retard_moyen_arrivee_tous,
    f.retard_moyen_arrivee_retardes,
    f.taux_regularite_pct,
    f.taux_annulation_pct,
    f.ecart_vs_national_pt,
    f.a_incident_documente
from {{ source('mart', 'fait_regularite') }} f
join {{ source('mart', 'dim_temps') }} t using (mois_id)

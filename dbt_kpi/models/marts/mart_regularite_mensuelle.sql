-- Agrégat de pilotage : une ligne par liaison et par mois, enrichie du
-- classement de la liaison sur le mois et du drapeau d'alerte métier.
select
    s.mois_id,
    s.annee,
    s.trimestre,
    d.libelle_liaison,
    d.gare_depart,
    d.gare_arrivee,
    d.service,
    d.est_international,
    d.duree_trajet_moyenne_min,
    s.nb_prevu,
    s.nb_annule,
    s.nb_circule,
    s.nb_retard_arrivee,
    s.taux_regularite_pct,
    s.taux_annulation_pct,
    s.ecart_vs_national_pt,
    s.retard_moyen_arrivee_retardes,
    s.a_incident_documente,
    rank() over (partition by s.mois_id order by s.taux_regularite_pct asc)
        as rang_mensuel_moins_regulier,
    -- Seuil versionné dans dbt_project.yml plutôt qu'en dur dans la requête.
    (s.taux_regularite_pct < {{ var('seuil_regularite_alerte') * 100 }})
        as est_sous_seuil_alerte
from {{ ref('stg_regularite') }} s
join {{ source('mart', 'dim_liaison') }} d using (liaison_id)

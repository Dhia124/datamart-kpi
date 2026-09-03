"""Étape 1 — Extraction : CSV source -> Parquet, avec schéma explicite et horodatage.

Aucune transformation ici : on fige la source telle quelle pour pouvoir
rejouer les étapes suivantes sans re-télécharger.
"""
from pathlib import Path

# Les chemins sont résolus depuis la racine du dépôt, pas depuis le dossier
# courant : les scripts fonctionnent quel que soit l'endroit d'où on les lance.
RACINE = Path(__file__).resolve().parents[1]
from datetime import datetime, timezone
import pandas as pd

RAW = RACINE / "data" / "raw"
OUT = RACINE / "data" / "out" / "extract"

RENAME = {
    "Date": "mois",
    "Service": "service",
    "Gare de départ": "gare_depart",
    "Gare d'arrivée": "gare_arrivee",
    "Durée moyenne du trajet": "duree_trajet_min",
    "Nombre de circulations prévues": "nb_prevu",
    "Nombre de trains annulés": "nb_annule",
    "Nombre de trains en retard au départ": "nb_retard_depart",
    "Retard moyen des trains en retard au départ": "retard_moyen_depart_retardes",
    "Retard moyen de tous les trains au départ": "retard_moyen_depart_tous",
    "Nombre de trains en retard à l'arrivée": "nb_retard_arrivee",
    "Retard moyen des trains en retard à l'arrivée": "retard_moyen_arrivee_retardes",
    "Retard moyen de tous les trains à l'arrivée": "retard_moyen_arrivee_tous",
    "Commentaire retards à l'arrivée": "commentaire_incidents",
    "Nombre trains en retard > 15min": "nb_retard_sup_15",
    "Nombre trains en retard > 30min": "nb_retard_sup_30",
    "Nombre trains en retard > 60min": "nb_retard_sup_60",
    "Prct retard pour causes externes": "pct_cause_externe",
    "Prct retard pour cause infrastructure": "pct_cause_infrastructure",
    "Prct retard pour cause gestion trafic": "pct_cause_gestion_trafic",
    "Prct retard pour cause matériel roulant": "pct_cause_materiel_roulant",
    "Prct retard pour cause gestion en gare et réutilisation de matériel": "pct_cause_gestion_gare",
    "Prct retard pour cause prise en compte voyageurs (affluence, gestions PSH, correspondances)": "pct_cause_voyageurs",
}

# Colonnes écartées, et pourquoi (documenté dans le README) :
#  - "Commentaire annulations", "Commentaire retards au départ" : 100 % vides sur 12 544 lignes.
#  - "Retard moyen trains en retard > 15 (si liaison concurrencée par vol)" : recopie du
#    retard moyen à l'arrivée sur 2 135 lignes, libellé non fiable -> exclue du modèle.
COLONNES_ECARTEES = [
    "Commentaire annulations",
    "Commentaire retards au départ",
    "Retard moyen trains en retard > 15 (si liaison concurrencée par vol)",
]


def extraire():
    OUT.mkdir(parents=True, exist_ok=True)
    horodatage = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    liaison = pd.read_csv(RAW / "regularite_liaison_mensuelle.csv", sep=";", low_memory=False)
    liaison.columns = [c.replace("﻿", "") for c in liaison.columns]
    liaison = liaison.drop(columns=[c for c in COLONNES_ECARTEES if c in liaison.columns])
    liaison = liaison.rename(columns=RENAME)

    national = pd.read_csv(RAW / "regularite_nationale_mensuelle.csv", sep=";")
    national.columns = [c.replace("﻿", "") for c in national.columns]
    national = national.rename(columns={
        "Date": "mois",
        "Régularité composite": "regularite_composite_nationale",
        "Ponctualité origine": "ponctualite_origine_nationale",
    })

    liaison.to_parquet(OUT / "liaison.parquet", index=False)
    national.to_parquet(OUT / "national.parquet", index=False)

    print(f"[extract {horodatage}] liaison  : {len(liaison):>6} lignes, {len(liaison.columns)} colonnes")
    print(f"[extract {horodatage}] national : {len(national):>6} lignes, {len(national.columns)} colonnes")
    print(f"[extract {horodatage}] colonnes écartées : {len(COLONNES_ECARTEES)}")


if __name__ == "__main__":
    extraire()

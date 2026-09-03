"""Étape 2 — Transformation : nettoyage, règles de qualité déclaratives, rejets motivés.

Principe : une ligne non conforme n'est jamais supprimée. Elle part en table de
rejets avec le motif de son exclusion, parce que la question qui suit un rejet
est toujours « pourquoi ».
"""
from pathlib import Path

# Les chemins sont résolus depuis la racine du dépôt, pas depuis le dossier
# courant : les scripts fonctionnent quel que soit l'endroit d'où on les lance.
RACINE = Path(__file__).resolve().parents[1]
import pandas as pd

SRC = RACINE / "data" / "out" / "extract"
OUT = RACINE / "data" / "out" / "transform"

# ---------------------------------------------------------------- règles
# Chaque règle : (code, libellé, prédicat renvoyant True quand la ligne est NON conforme)
REGLES = [
    ("R01_PREVU_NUL",
     "Aucune circulation prévue : le taux de régularité est indéfini",
     lambda d: d["nb_prevu"] == 0),

    ("R02_ANNULE_SUP_PREVU",
     "Trains annulés supérieurs aux circulations prévues : incohérent",
     lambda d: d["nb_annule"] > d["nb_prevu"]),

    ("R03_SEUILS_INCOHERENTS",
     "Hiérarchie des seuils de retard violée (>15 >= >30 >= >60 attendu)",
     lambda d: (d["nb_retard_sup_15"] < d["nb_retard_sup_30"])
             | (d["nb_retard_sup_30"] < d["nb_retard_sup_60"])),

    ("R04_RETARD_SUP_PREVU",
     "Trains en retard à l'arrivée supérieurs aux circulations prévues",
     lambda d: d["nb_retard_arrivee"] > d["nb_prevu"]),

    ("R07_SEUIL15_SUP_TOTAL",
     "Trains en retard de plus de 15 min supérieurs au total des trains en retard à l'arrivée",
     lambda d: d["nb_retard_sup_15"] > d["nb_retard_arrivee"]),

    ("R06_COMPTAGE_NEGATIF",
     "Valeur de comptage négative : impossible pour un nombre de trains",
     lambda d: (d["nb_prevu"] < 0) | (d["nb_annule"] < 0) | (d["nb_retard_depart"] < 0)
             | (d["nb_retard_arrivee"] < 0) | (d["nb_retard_sup_15"] < 0)
             | (d["nb_retard_sup_30"] < 0) | (d["nb_retard_sup_60"] < 0)),

    ("R05_CAUSES_INCOMPLETES",
     "Ventilation des causes absente ou incomplète alors que des retards sont constatés",
     lambda d: (d["nb_retard_arrivee"] > 0) & (~d["somme_causes"].between(99, 101))),
]

COLS_CAUSES = ["pct_cause_externe", "pct_cause_infrastructure", "pct_cause_gestion_trafic",
               "pct_cause_materiel_roulant", "pct_cause_gestion_gare", "pct_cause_voyageurs"]


def normaliser_gare(s: pd.Series) -> pd.Series:
    """Les libellés sources sont en majuscules non accentuées et parfois espacés
    irrégulièrement. On normalise pour que la dimension liaison soit stable."""
    return (s.astype(str).str.strip().str.upper()
             .str.replace(r"\s+", " ", regex=True))


def transformer():
    OUT.mkdir(parents=True, exist_ok=True)
    d = pd.read_parquet(SRC / "liaison.parquet")

    # --- nettoyage
    d["gare_depart"] = normaliser_gare(d["gare_depart"])
    d["gare_arrivee"] = normaliser_gare(d["gare_arrivee"])
    d["annee"] = d["mois"].str[:4].astype(int)
    d["numero_mois"] = d["mois"].str[5:7].astype(int)
    d["somme_causes"] = d[COLS_CAUSES].sum(axis=1)

    # --- clé métier et unicité du grain
    cle = ["mois", "service", "gare_depart", "gare_arrivee"]
    doublons = int(d.duplicated(cle).sum())
    if doublons:
        raise SystemExit(f"Grain non unique : {doublons} doublons sur {cle}")

    # --- application des règles
    d["motifs_rejet"] = ""
    for code, libelle, predicat in REGLES:
        masque = predicat(d)
        d.loc[masque, "motifs_rejet"] = (
            d.loc[masque, "motifs_rejet"] + ("|" if masque.any() else "") + code
        ).str.lstrip("|")
        print(f"  {code:24} {int(masque.sum()):>5} lignes  — {libelle}")

    conforme = d["motifs_rejet"] == ""
    valide, rejets = d[conforme].copy(), d[~conforme].copy()

    # motif lisible sur la table de rejets
    dico = {code: lib for code, lib, _ in REGLES}
    rejets["motifs_libelle"] = rejets["motifs_rejet"].map(
        lambda m: " ; ".join(dico[c] for c in m.split("|"))
    )

    valide.to_parquet(OUT / "liaison_valide.parquet", index=False)
    rejets.to_parquet(OUT / "liaison_rejets.parquet", index=False)

    print(f"\n  conformes : {len(valide):>6}  ({len(valide)/len(d):.2%})")
    print(f"  rejetées  : {len(rejets):>6}  ({len(rejets)/len(d):.2%})")
    print("\n  répartition des rejets par période :")
    print(rejets.groupby(rejets['annee'])['motifs_rejet'].count().to_string())


if __name__ == "__main__":
    transformer()

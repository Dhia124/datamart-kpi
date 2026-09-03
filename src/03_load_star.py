"""Étape 3 — Chargement du modèle en étoile (DuckDB + SQL).

GRAIN DE LA TABLE DE FAITS PRINCIPALE :
    une ligne par (mois, service, gare de départ, gare d'arrivée).

Justification : sur 12 544 lignes sources, cette clé ne produit aucun doublon.
Retirer « service » en produit 42 : une même liaison peut être desservie à la
fois en service National et International le même mois. Le service fait donc
partie de la clé, pas des attributs.
"""
from pathlib import Path

# Les chemins sont résolus depuis la racine du dépôt, pas depuis le dossier
# courant : les scripts fonctionnent quel que soit l'endroit d'où on les lance.
RACINE = Path(__file__).resolve().parents[1]
import duckdb

OUT = RACINE / "data" / "out"
DB = OUT / "kpi.duckdb"

# Regroupement éditorial des causes. Choix assumé et documenté : les causes
# « externes » et « prise en charge voyageurs » ne relèvent pas de la production
# ferroviaire, les quatre autres si. Cette distinction est ce qui rend le
# tableau de bord actionnable.
CAUSES = [
    ("externe",            "pct_cause_externe",            "Causes externes",                    "Hors production"),
    ("infrastructure",     "pct_cause_infrastructure",     "Infrastructure",                     "Production"),
    ("gestion_trafic",     "pct_cause_gestion_trafic",     "Gestion du trafic",                  "Production"),
    ("materiel_roulant",   "pct_cause_materiel_roulant",   "Matériel roulant",                   "Production"),
    ("gestion_gare",       "pct_cause_gestion_gare",       "Gestion en gare et réutilisation",   "Production"),
    ("voyageurs",          "pct_cause_voyageurs",          "Prise en charge des voyageurs",      "Hors production"),
]


def charger():
    OUT.mkdir(parents=True, exist_ok=True)
    if DB.exists():
        DB.unlink()
    con = duckdb.connect(str(DB))

    con.execute(f"CREATE VIEW src AS SELECT * FROM read_parquet('" + str(OUT / 'transform' / 'liaison_valide.parquet').replace(chr(92), '/') + "')")
    con.execute(f"CREATE VIEW rej AS SELECT * FROM read_parquet('" + str(OUT / 'transform' / 'liaison_rejets.parquet').replace(chr(92), '/') + "')")
    con.execute(f"CREATE VIEW nat AS SELECT * FROM read_parquet('" + str(OUT / 'extract' / 'national.parquet').replace(chr(92), '/') + "')")

    # ------------------------------------------------------------ dim_temps
    con.execute("""
    CREATE TABLE dim_temps AS
    SELECT
        mois                                             AS mois_id,
        CAST(substr(mois, 1, 4) AS INTEGER)              AS annee,
        CAST(substr(mois, 6, 2) AS INTEGER)              AS numero_mois,
        -- FLOOR et non CAST : (3-1)/3 vaut 0,67 et CAST arrondit à 1, ce qui
        -- placerait mars au deuxième trimestre. Même erreur pour juin, septembre
        -- et décembre. La division entière est explicite.
        CAST(FLOOR((CAST(substr(mois, 6, 2) AS INTEGER) - 1) / 3) + 1 AS INTEGER) AS trimestre,
        -- Vrai type DATE, et non une chaîne : les fonctions de temps de
        -- Power BI (DATEADD, SAMEPERIODLASTYEAR…) refusent une colonne texte.
        CAST(mois || '-01' AS DATE)                      AS premier_jour,
        -- Période de restriction sanitaire : sert à neutraliser 2020 dans les
        -- comparaisons annuelles plutôt qu'à exclure les lignes.
        (mois >= '2020-03' AND mois <= '2021-06')        AS est_periode_covid
    FROM (SELECT DISTINCT mois FROM src UNION SELECT DISTINCT mois FROM rej)
    ORDER BY mois_id
    """)

    # --------------------------------------------------------- dim_liaison
    con.execute("""
    CREATE TABLE dim_liaison AS
    SELECT
        md5(gare_depart || '>' || gare_arrivee || '|' || service) AS liaison_id,
        gare_depart,
        gare_arrivee,
        gare_depart || ' > ' || gare_arrivee              AS libelle_liaison,
        service,
        (service = 'International')                       AS est_international,
        ROUND(AVG(duree_trajet_min), 1)                   AS duree_trajet_moyenne_min,
        COUNT(*)                                          AS nb_mois_observes
    FROM src
    GROUP BY gare_depart, gare_arrivee, service
    """)

    # ----------------------------------------------------------- dim_cause
    con.execute("CREATE TABLE dim_cause (cause_id VARCHAR, libelle VARCHAR, famille VARCHAR)")
    for code, _col, libelle, famille in CAUSES:
        con.execute("INSERT INTO dim_cause VALUES (?, ?, ?)", [code, libelle, famille])

    # ----------------------------------------------------- fait_regularite
    # nb_circule = prévu - annulé : c'est le dénominateur du taux de régularité,
    # un train annulé n'étant ni à l'heure ni en retard.
    con.execute("""
    CREATE TABLE fait_regularite AS
    SELECT
        s.mois                                            AS mois_id,
        md5(s.gare_depart || '>' || s.gare_arrivee || '|' || s.service) AS liaison_id,
        s.nb_prevu,
        s.nb_annule,
        s.nb_prevu - s.nb_annule                          AS nb_circule,
        s.nb_retard_arrivee,
        s.nb_retard_sup_15,
        s.nb_retard_sup_30,
        s.nb_retard_sup_60,
        s.retard_moyen_arrivee_tous,
        s.retard_moyen_arrivee_retardes,
        s.duree_trajet_min,
        CASE WHEN s.nb_prevu - s.nb_annule > 0
             THEN ROUND(100.0 * (s.nb_prevu - s.nb_annule - s.nb_retard_arrivee)
                        / (s.nb_prevu - s.nb_annule), 4) END           AS taux_regularite_pct,
        CASE WHEN s.nb_prevu > 0
             THEN ROUND(100.0 * s.nb_annule / s.nb_prevu, 4) END       AS taux_annulation_pct,
        n.regularite_composite_nationale,
        CASE WHEN s.nb_prevu - s.nb_annule > 0 AND n.regularite_composite_nationale IS NOT NULL
             THEN ROUND(100.0 * (s.nb_prevu - s.nb_annule - s.nb_retard_arrivee)
                        / (s.nb_prevu - s.nb_annule) - n.regularite_composite_nationale, 4) END
                                                          AS ecart_vs_national_pt,
        (s.commentaire_incidents IS NOT NULL)             AS a_incident_documente
    FROM src s
    LEFT JOIN nat n ON n.mois = s.mois
    """)

    # --------------------------------------------------- fait_retard_cause
    # Dépivotage : les six causes passent de colonnes à lignes, et le
    # pourcentage est converti en nombre de trains attribués.
    union = "\nUNION ALL\n".join(
        f"""SELECT mois AS mois_id,
                   md5(gare_depart || '>' || gare_arrivee || '|' || service) AS liaison_id,
                   '{code}' AS cause_id,
                   {col} AS pct_retard,
                   ROUND({col} / 100.0 * nb_retard_arrivee, 2) AS nb_trains_attribues
            FROM src WHERE {col} > 0"""
        for code, col, _l, _f in CAUSES)
    con.execute(f"CREATE TABLE fait_retard_cause AS {union}")

    # ------------------------------------------------------------- rejets
    con.execute("""
    CREATE TABLE rejets AS
    SELECT mois AS mois_id, service, gare_depart, gare_arrivee,
           motifs_rejet, motifs_libelle,
           nb_prevu, nb_annule, nb_retard_arrivee,
           nb_retard_sup_15, nb_retard_sup_30, nb_retard_sup_60
    FROM rej
    """)

    for t in ["dim_temps", "dim_liaison", "dim_cause", "fait_regularite", "fait_retard_cause", "rejets"]:
        n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        print(f"  {t:20} {n:>7} lignes")
        con.execute(f"COPY {t} TO '{str(OUT / (t + '.parquet')).replace(chr(92), '/')}' (FORMAT PARQUET)")

    # ------------------------------------------- contrôles post-chargement
    print("\n  contrôles d'intégrité :")
    orph = con.execute("""SELECT count(*) FROM fait_regularite f
                          LEFT JOIN dim_liaison d USING (liaison_id) WHERE d.liaison_id IS NULL""").fetchone()[0]
    print(f"    faits sans liaison correspondante : {orph}")
    orph2 = con.execute("""SELECT count(*) FROM fait_regularite f
                           LEFT JOIN dim_temps t USING (mois_id) WHERE t.mois_id IS NULL""").fetchone()[0]
    print(f"    faits sans mois correspondant     : {orph2}")
    dup = con.execute("""SELECT count(*) FROM (SELECT mois_id, liaison_id FROM fait_regularite
                         GROUP BY 1,2 HAVING count(*) > 1)""").fetchone()[0]
    print(f"    grain non unique                  : {dup}")
    hors = con.execute("""SELECT count(*) FROM fait_regularite
                          WHERE taux_regularite_pct < 0 OR taux_regularite_pct > 100""").fetchone()[0]
    print(f"    taux de régularité hors [0,100]   : {hors}")
    con.close()


if __name__ == "__main__":
    charger()

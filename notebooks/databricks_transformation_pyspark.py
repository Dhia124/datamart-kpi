# Databricks notebook source
# MAGIC %md
# MAGIC # Data mart régularité — transformation PySpark
# MAGIC
# MAGIC Portage de l'étape 2 du pipeline (`src/02_transform.py`) sur Databricks.
# MAGIC Même règles, même table de rejets motivés, exécution distribuée.
# MAGIC
# MAGIC **Grain de sortie** : une ligne par (mois, service, gare de départ, gare d'arrivée).
# MAGIC
# MAGIC Prérequis : déposer `regularite_liaison_mensuelle.csv` et
# MAGIC `regularite_nationale_mensuelle.csv` dans un volume ou DBFS, puis
# MAGIC ajuster `CHEMIN_SOURCE` ci-dessous.

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Détection du catalogue : il s'appelle « workspace » sur Databricks Free
# Edition et « main » sur la plupart des workspaces Unity Catalog. On ne le
# code pas en dur, sinon le notebook ne tourne que sur un seul environnement.
catalogues = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
CATALOGUE = next((c for c in ("workspace", "main") if c in catalogues), catalogues[0])
SCHEMA = "datamart_kpi"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGUE}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGUE}.{SCHEMA}.donnees")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGUE}.{SCHEMA}.sortie")

CHEMIN_SOURCE = f"/Volumes/{CATALOGUE}/{SCHEMA}/donnees/"
CHEMIN_SORTIE = f"/Volumes/{CATALOGUE}/{SCHEMA}/sortie/"

print(f"catalogues disponibles : {catalogues}")
print(f"catalogue retenu       : {CATALOGUE}")
print(f"source                 : {CHEMIN_SOURCE}")
print(f"sortie                 : {CHEMIN_SORTIE}")
print()
print("Déposer les deux CSV dans le volume source via Catalog > "
      f"{CATALOGUE} > {SCHEMA} > donnees > Upload to this volume :")
print("  regularite_liaison_mensuelle.csv")
print("  regularite_nationale_mensuelle.csv")

# COMMAND ----------
# MAGIC %md ## 1. Lecture avec schéma explicite
# MAGIC Le schéma est déclaré plutôt qu'inféré : une inférence sur CSV lit tout
# MAGIC le fichier et laisse le typage varier d'une exécution à l'autre.

# COMMAND ----------

schema = StructType([
    StructField("Date", StringType(), False),
    StructField("Service", StringType(), False),
    StructField("Gare de départ", StringType(), False),
    StructField("Gare d'arrivée", StringType(), False),
    StructField("Durée moyenne du trajet", IntegerType(), True),
    StructField("Nombre de circulations prévues", IntegerType(), True),
    StructField("Nombre de trains annulés", IntegerType(), True),
    StructField("Commentaire annulations", StringType(), True),
    StructField("Nombre de trains en retard au départ", IntegerType(), True),
    StructField("Retard moyen des trains en retard au départ", DoubleType(), True),
    StructField("Retard moyen de tous les trains au départ", DoubleType(), True),
    StructField("Commentaire retards au départ", StringType(), True),
    StructField("Nombre de trains en retard à l'arrivée", IntegerType(), True),
    StructField("Retard moyen des trains en retard à l'arrivée", DoubleType(), True),
    StructField("Retard moyen de tous les trains à l'arrivée", DoubleType(), True),
    StructField("Commentaire retards à l'arrivée", StringType(), True),
    StructField("Nombre trains en retard > 15min", IntegerType(), True),
    StructField("Retard moyen trains en retard > 15 (si liaison concurrencée par vol)", DoubleType(), True),
    StructField("Nombre trains en retard > 30min", IntegerType(), True),
    StructField("Nombre trains en retard > 60min", IntegerType(), True),
    StructField("Prct retard pour causes externes", DoubleType(), True),
    StructField("Prct retard pour cause infrastructure", DoubleType(), True),
    StructField("Prct retard pour cause gestion trafic", DoubleType(), True),
    StructField("Prct retard pour cause matériel roulant", DoubleType(), True),
    StructField("Prct retard pour cause gestion en gare et réutilisation de matériel", DoubleType(), True),
    StructField("Prct retard pour cause prise en compte voyageurs (affluence, gestions PSH, correspondances)", DoubleType(), True),
])

fichiers = [f.name for f in dbutils.fs.ls(CHEMIN_SOURCE)]
assert "regularite_liaison_mensuelle.csv" in fichiers, (
    f"CSV absent du volume. Contenu actuel : {fichiers}")
print("fichiers trouvés :", fichiers)

# COMMAND ----------

brut = (spark.read
        .option("header", True).option("sep", ";").option("encoding", "UTF-8")
        # Le champ « Commentaire retards à l'arrivée » contient des sauts de ligne
        # entre guillemets : 15 062 lignes physiques pour 12 544 enregistrements.
        # Sans multiLine, Spark coupe l'enregistrement au premier saut de ligne et
        # perd toutes les colonnes suivantes — dont les six causes de retard.
        .option("multiLine", True).option("quote", '"').option("escape", '"')
        .schema(schema)
        .csv(CHEMIN_SOURCE + "regularite_liaison_mensuelle.csv"))

# COMMAND ----------
# MAGIC %md ## 2. Renommage et colonnes écartées
# MAGIC Deux colonnes de commentaires sont vides sur 100 % des lignes, et la
# MAGIC colonne « retard moyen > 15 (si liaison concurrencée par vol) » recopie le
# MAGIC retard moyen à l'arrivée sur 2 135 lignes : libellé non fiable, exclue.

# COMMAND ----------

d = (brut
     .withColumnRenamed("Date", "mois")
     .withColumnRenamed("Service", "service")
     .withColumnRenamed("Gare de départ", "gare_depart")
     .withColumnRenamed("Gare d'arrivée", "gare_arrivee")
     .withColumnRenamed("Durée moyenne du trajet", "duree_trajet_min")
     .withColumnRenamed("Nombre de circulations prévues", "nb_prevu")
     .withColumnRenamed("Nombre de trains annulés", "nb_annule")
     .withColumnRenamed("Nombre de trains en retard au départ", "nb_retard_depart")
     .withColumnRenamed("Retard moyen des trains en retard au départ", "retard_moyen_depart_retardes")
     .withColumnRenamed("Retard moyen de tous les trains au départ", "retard_moyen_depart_tous")
     .withColumnRenamed("Nombre de trains en retard à l'arrivée", "nb_retard_arrivee")
     .withColumnRenamed("Retard moyen des trains en retard à l'arrivée", "retard_moyen_arrivee_retardes")
     .withColumnRenamed("Retard moyen de tous les trains à l'arrivée", "retard_moyen_arrivee_tous")
     .withColumnRenamed("Commentaire retards à l'arrivée", "commentaire_incidents")
     .withColumnRenamed("Nombre trains en retard > 15min", "nb_retard_sup_15")
     .withColumnRenamed("Nombre trains en retard > 30min", "nb_retard_sup_30")
     .withColumnRenamed("Nombre trains en retard > 60min", "nb_retard_sup_60")
     .withColumnRenamed("Prct retard pour causes externes", "pct_cause_externe")
     .withColumnRenamed("Prct retard pour cause infrastructure", "pct_cause_infrastructure")
     .withColumnRenamed("Prct retard pour cause gestion trafic", "pct_cause_gestion_trafic")
     .withColumnRenamed("Prct retard pour cause matériel roulant", "pct_cause_materiel_roulant")
     .withColumnRenamed("Prct retard pour cause gestion en gare et réutilisation de matériel", "pct_cause_gestion_gare")
     .withColumnRenamed("Prct retard pour cause prise en compte voyageurs (affluence, gestions PSH, correspondances)", "pct_cause_voyageurs")
     .drop("Commentaire annulations", "Commentaire retards au départ",
           "Retard moyen trains en retard > 15 (si liaison concurrencée par vol)"))

# Normalisation des libellés de gare : la dimension liaison doit être stable.
for c in ["gare_depart", "gare_arrivee"]:
    d = d.withColumn(c, F.regexp_replace(F.upper(F.trim(F.col(c))), r"\s+", " "))

# Garde-fou : Delta refuse espaces, accents et ponctuation dans les noms de
# colonnes (DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES). Plutôt que d'attendre
# l'échec à l'écriture, on vérifie ici et on signale la colonne fautive.
INTERDITS = set(" ,;{}()\n\t=")
restantes = [c for c in d.columns if set(c) & INTERDITS]
assert not restantes, (
    "Colonnes non renommées, incompatibles avec Delta : " + str(restantes))
print(f"{len(d.columns)} colonnes, toutes compatibles Delta")

CAUSES = ["pct_cause_externe", "pct_cause_infrastructure", "pct_cause_gestion_trafic",
          "pct_cause_materiel_roulant", "pct_cause_gestion_gare", "pct_cause_voyageurs"]
d = d.withColumn("somme_causes", sum(F.coalesce(F.col(c), F.lit(0.0)) for c in CAUSES))

# COMMAND ----------
# MAGIC %md ## 3. Vérification du grain
# MAGIC Le service fait partie de la clé : sans lui, 42 liaisons-mois apparaissent
# MAGIC en double (une même liaison desservie en National et en International).

# COMMAND ----------

cle = ["mois", "service", "gare_depart", "gare_arrivee"]
n = d.count()
assert n == 12544, f"Volume inattendu après lecture : {n} au lieu de 12544"
doublons = d.groupBy(*cle).count().filter(F.col("count") > 1).count()
assert doublons == 0, f"Grain non unique : {doublons} doublons sur {cle}"
print(f"Grain vérifié sur {cle} — {n} lignes, aucun doublon")

# COMMAND ----------
# MAGIC %md ## 4. Règles de qualité déclaratives et quarantaine motivée
# MAGIC Une ligne non conforme n'est pas supprimée : elle part en table de rejets
# MAGIC avec le motif, parce que la question qui suit un rejet est toujours « pourquoi ».

# COMMAND ----------

REGLES = [
    ("R01_PREVU_NUL", "Aucune circulation prévue : taux de régularité indéfini",
     F.col("nb_prevu") == 0),
    ("R02_ANNULE_SUP_PREVU", "Trains annulés supérieurs aux circulations prévues",
     F.col("nb_annule") > F.col("nb_prevu")),
    ("R03_SEUILS_INCOHERENTS", "Hiérarchie des seuils de retard violée",
     (F.col("nb_retard_sup_15") < F.col("nb_retard_sup_30")) |
     (F.col("nb_retard_sup_30") < F.col("nb_retard_sup_60"))),
    ("R04_RETARD_SUP_PREVU", "Trains en retard supérieurs aux circulations prévues",
     F.col("nb_retard_arrivee") > F.col("nb_prevu")),
    ("R07_SEUIL15_SUP_TOTAL", "Trains en retard > 15 min supérieurs au total des retards",
     F.col("nb_retard_sup_15") > F.col("nb_retard_arrivee")),
    ("R06_COMPTAGE_NEGATIF", "Valeur de comptage négative : impossible",
     F.least(*[F.col(c) for c in ["nb_prevu", "nb_annule", "nb_retard_depart",
                                  "nb_retard_arrivee", "nb_retard_sup_15",
                                  "nb_retard_sup_30", "nb_retard_sup_60"]]) < 0),
    ("R05_CAUSES_INCOMPLETES", "Ventilation des causes absente malgré des retards",
     (F.col("nb_retard_arrivee") > 0) & (~F.col("somme_causes").between(99, 101))),
]

# ATTENTION — piège Spark : `array_remove(tableau, None)` renvoie NULL, et non
# le tableau nettoyé, car l'élément à retirer est NULL. `size(NULL)` n'est alors
# ni 0 ni > 0, et les deux filtres suivants ne retiennent aucune ligne : le
# pipeline « réussit » en produisant zéro enregistrement.
# `concat_ws` ignore nativement les NULL : c'est la construction sûre.
motifs = F.concat_ws("|", *[
    F.when(cond, F.lit(code)) for code, _lib, cond in REGLES
])

d = d.withColumn("motifs_rejet", motifs)

# Garde-fou : un contrôle qui ne vérifie pas le volume ne vérifie rien.
# Un `assert` sur les doublons passe toujours sur une table vide.
total = d.count()
assert total > 0, "Aucune ligne après application des règles — pipeline vide"

valide = d.filter(F.col("motifs_rejet") == "").drop("motifs_rejet")
rejets = d.filter(F.col("motifs_rejet") != "")

n_valide, n_rejets = valide.count(), rejets.count()
print(f"total     : {total}")
print(f"conformes : {n_valide}   ({n_valide / total:.2%})")
print(f"rejetées  : {n_rejets}   ({n_rejets / total:.2%})")
assert n_valide + n_rejets == total, "Des lignes se sont perdues entre les deux filtres"

print("\nrépartition des motifs :")
(rejets.groupBy("motifs_rejet").count().orderBy(F.desc("count"))).show(10, truncate=False)

# COMMAND ----------
# MAGIC %md ## 5. Dépivotage des causes
# MAGIC Les six causes passent de colonnes à lignes, et le pourcentage devient un
# MAGIC nombre de trains attribués — la mesure que le tableau de bord additionne.

# COMMAND ----------

paires = ", ".join(f"'{c.replace('pct_cause_', '')}', {c}" for c in CAUSES)
faits_causes = (valide
    .selectExpr("mois", "service", "gare_depart", "gare_arrivee", "nb_retard_arrivee",
                f"stack({len(CAUSES)}, {paires}) as (cause_id, pct_retard)")
    .filter(F.col("pct_retard") > 0)
    .withColumn("nb_trains_attribues",
                F.round(F.col("pct_retard") / 100.0 * F.col("nb_retard_arrivee"), 2)))

# COMMAND ----------
# MAGIC %md ## 6. Écriture en Delta

# COMMAND ----------

# Tables gérées par Unity Catalog : interrogeables en SQL depuis le workspace,
# plutôt que des fichiers Delta à un chemin qu'il faut retenir.
for df, nom in [(valide, "liaison_valide"),
                (rejets, "liaison_rejets"),
                (faits_causes, "fait_retard_cause")]:
    (df.write.mode("overwrite").format("delta")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOGUE}.{SCHEMA}.{nom}"))
    print(f"écrit : {CATALOGUE}.{SCHEMA}.{nom}")

# COMMAND ----------
# MAGIC %md ## 7. Vérification en SQL

# COMMAND ----------

resultat = spark.sql(f"""
    SELECT substr(mois, 1, 4) AS annee,
           COUNT(*)                                   AS liaisons_mois,
           ROUND(100.0 * SUM(nb_prevu - nb_annule - nb_retard_arrivee)
                       / SUM(nb_prevu - nb_annule), 2) AS taux_regularite_pct
    FROM {CATALOGUE}.{SCHEMA}.liaison_valide
    GROUP BY 1 ORDER BY 1
""")

# `display` n'existe que dans le notebook Databricks. En exécution via
# Databricks Connect depuis un IDE, elle n'est pas injectée : on retombe sur
# `show`, pour que le même fichier tourne dans les deux contextes.
try:
    display(resultat)          # noqa: F821 — fourni par le runtime notebook
except NameError:
    resultat.show(20, truncate=False)

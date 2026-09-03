# Operational KPI Data Mart & Analytics Pipeline

Data mart analytique en étoile pour le suivi d'indicateurs opérationnels, construit
sur les données ouvertes de régularité mensuelle des TGV (source : SNCF / AQST).

**Chaîne complète** : extraction → règles de qualité avec quarantaine motivée →
modèle en étoile → couche dbt testée → restitution Power BI.

| | |
|---|---|
| Période couverte | 2018-01 → 2026-06 (102 mois) |
| Lignes sources | 12 544 |
| Lignes conformes | 12 002 (95,7 %) |
| Lignes en quarantaine | 542 (4,3 %), avec motif |
| Liaisons | 146 (59 gares, 2 services) |
| Faits de causes après dépivotage | 60 568 |
| Tests dbt | 32, tous au vert |

---

## 1. Le problème

Suivre la performance opérationnelle d'un réseau ferroviaire suppose de répondre à
quatre questions qui ne se posent pas au même niveau :

- La régularité s'améliore-t-elle ou se dégrade-t-elle dans le temps ?
- Quelles liaisons décrochent, et de combien par rapport à la moyenne nationale ?
- Les retards viennent-ils de la production ferroviaire ou de causes qui lui échappent ?
- Peut-on faire confiance au chiffre affiché ?

Une table à plat ne répond qu'à la première. Les trois autres demandent un modèle
dimensionnel — et la quatrième demande de savoir ce qui a été écarté, et pourquoi.

## 2. Le modèle

**Grain de la table de faits principale : une ligne par (mois, service, gare de
départ, gare d'arrivée).**

Cette clé ne produit aucun doublon sur les 12 544 lignes sources. La retirer du
service en produit **42** : une même liaison peut être desservie le même mois en
service National et en service International. Le service appartient donc à la clé,
pas aux attributs — c'est la première décision du modèle, et elle est vérifiée par
les données plutôt que supposée.

```
                    dim_temps                dim_cause
                  (102 mois)               (6 causes,
                        |                2 familles)
                        |                     |
   dim_liaison ── fait_regularite      fait_retard_cause ── dim_liaison
   (146 liaisons)   (12 002 faits)        (60 568 faits)
```

Deux tables de faits, à deux grains distincts :

- **`fait_regularite`** — mois × liaison. Mesures additives (circulations prévues,
  annulées, en retard, par seuil) et taux calculés.
- **`fait_retard_cause`** — mois × liaison × cause, obtenue en dépivotant les six
  colonnes de pourcentage. Le pourcentage y devient un **nombre de trains attribués**,
  seule forme additive et donc sommable dans un tableau de bord.

### Décisions de modélisation

**Le dénominateur du taux de régularité exclut les trains annulés.**
`taux = (circulés − en retard à l'arrivée) / circulés`, avec `circulés = prévus − annulés`.
Un train annulé n'est ni à l'heure ni en retard ; le compter au dénominateur ferait
baisser la régularité pour une raison qui n'est pas un retard. Le taux d'annulation
est suivi séparément.

**Les six causes sont regroupées en deux familles.** « Causes externes » et « prise
en charge des voyageurs » relèvent de l'exploitation au sens large ; infrastructure,
gestion du trafic, matériel roulant et gestion en gare relèvent de la production
ferroviaire. C'est un choix éditorial assumé — il transforme six parts de camembert
en une question actionnable : **70,1 %** des retards attribués relèvent de la production.

**Les seuils de retard sont cumulatifs, pas disjoints.** Hypothèse testée plutôt que
supposée : la lecture par tranches disjointes (15-30, 30-60, > 60) est violée sur
8 691 lignes, la lecture cumulative sur 357. Le modèle retient la cumulative, et un
test dbt garde la règle.

**Le référentiel national est joint comme mesure de comparaison.** Un second flux
apporte la régularité composite nationale mensuelle, ce qui donne
`ecart_vs_national_pt` : une liaison n'est pas jugée dans l'absolu mais par rapport
au réseau, le même mois.

## 3. Qualité : sept règles, et une quarantaine motivée

Une ligne non conforme n'est jamais supprimée. Elle part en table de rejets **avec
le motif de son exclusion**, parce que la question qui suit un rejet est toujours
« pourquoi ».

| Règle | Lignes | Ce qu'elle attrape |
|---|---|---|
| `R01_PREVU_NUL` | 73 | Aucune circulation prévue → taux indéfini |
| `R02_ANNULE_SUP_PREVU` | 63 | Plus d'annulations que de circulations prévues |
| `R03_SEUILS_INCOHERENTS` | 357 | Hiérarchie > 15 ≥ > 30 ≥ > 60 violée |
| `R04_RETARD_SUP_PREVU` | 0 | Retards supérieurs aux circulations |
| `R05_CAUSES_INCOMPLETES` | 82 | Ventilation absente malgré des retards |
| `R06_COMPTAGE_NEGATIF` | 43 | Nombre de trains négatif |
| `R07_SEUIL15_SUP_TOTAL` | 31 | Retards > 15 min dépassant le total des retards |

### Ce que les règles ont révélé

**Le premier trimestre 2025 est corrompu.** Les 357 violations de hiérarchie ne sont
pas dispersées : **355 tombent sur janvier, février et mars 2025**, et touchent
quasiment toutes les liaisons de ces trois mois (118, 117, 120). Zéro ailleurs, y
compris en 2026. La colonne « > 30 min » y contient des zéros et **43 valeurs
négatives**, jusqu'à −44. Permuter les colonnes > 30 et > 60 rétablirait la hiérarchie
sur 361 lignes sur 363 — mais les valeurs négatives excluent la simple inversion.
Conclusion retenue : incident de publication sur un trimestre, pas erreur ponctuelle.

**Le printemps 2020 se lit dans les données.** Les 63 lignes « annulés > prévus » et
les 73 lignes « aucune circulation prévue » sont toutes situées entre avril et
décembre 2020 : pendant le confinement, les circulations programmées ont été remises
à zéro alors que les annulations continuaient d'être enregistrées. La règle attrape
un artefact réel, pas un bug.

**Un test dbt a rattrapé une règle manquante.** À la première exécution,
`assert_hierarchie_seuils_retard` a échoué sur 30 lignes qui avaient franchi le filtre
amont : les règles vérifiaient la hiérarchie interne des seuils mais pas la borne
supérieure. D'où la règle `R07`, ajoutée en amont plutôt qu'un assouplissement du
test. C'est exactement le rôle qu'on attend d'une couche de tests.

**Trois colonnes ont été écartées de la source**, et c'est documenté : deux colonnes
de commentaires vides sur 100 % des lignes, et « retard moyen > 15 min (si liaison
concurrencée par vol) », qui recopie le retard moyen à l'arrivée sur 2 135 lignes —
libellé non fiable.

## 4. La couche dbt

dbt n'orchestre pas le chargement : il **documente, teste et expose** au-dessus du
modèle en étoile. 32 tests, tous au vert.

- Tests génériques **écrits sur mesure** (`macros/tests_generiques.sql`) plutôt
  qu'importés : `intervalle_accepte` et `combinaison_unique`. Le projet n'a aucune
  dépendance externe et les règles restent lisibles dans le dépôt.
- Tests d'intégrité référentielle entre faits et dimensions.
- Trois tests singuliers portant les règles métier : somme des causes à 100 %,
  hiérarchie des seuils, absence de fait orphelin.
- Seuils métier en `vars` du `dbt_project.yml`, versionnés en Git plutôt qu'écrits
  en dur dans les requêtes.

## 5. Ce que le data mart montre

- **La régularité de 2026 est revenue au niveau de 2018** : 82,14 % contre 82,34 %,
  après un pic à 88,7 % en 2021 — année de trafic réduit.
- **Les liaisons les moins régulières sont structurellement les mêmes** sur 98 mois :
  Chambéry–Paris Lyon (73,0 %), Lyon Part-Dieu–Lille (73,8 %), Mâcon–Paris Lyon (74,0 %).
- **70,1 % des retards attribués relèvent de la production ferroviaire**, le reste
  de causes externes et de la prise en charge des voyageurs.

## 6. Exécution

```bash
pip install -r requirements.txt
python src/01_extract.py      # CSV -> Parquet, schéma explicite
python src/02_transform.py    # règles de qualité, quarantaine motivée
python src/03_load_star.py    # modèle en étoile (DuckDB + SQL)
cd dbt_kpi && dbt build       # 32 tests
```

La transformation existe aussi en **PySpark pour Databricks**
(`notebooks/databricks_transformation_pyspark.py`) : mêmes règles, même table de
rejets, exécution distribuée et écriture en Delta.

## 7. Restitution

Les tables du modèle sont exportées en Parquet dans `data/out/`, prêtes à charger
dans Power BI. Voir `docs/powerbi.md` pour le modèle de données, les relations et
les mesures DAX.

![Tableau de bord — pilotage](docs/dashboard_pilotage.png)
![Tableau de bord — qualité des données](docs/dashboard_qualite.png)
---

### Stack

Python (pandas, pyarrow) · SQL · DuckDB · PySpark / Databricks · dbt · Power BI

### Source des données

[SNCF Open Data — Régularité mensuelle TGV (AQST)](https://ressources.data.sncf.com)
et régularité mensuelle nationale. Licence ouverte.

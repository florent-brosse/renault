# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Create Genie Space
# MAGIC
# MAGIC Provisions an AI/BI Genie space for natural language car sales analytics.
# MAGIC
# MAGIC **Run after** the pipeline has completed at least one run.

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("catalog", "renault_demo", "Catalog name")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "car_sales"

w = WorkspaceClient()

# ─── Find a SQL warehouse ───
warehouse_id = None
for wh in w.warehouses.list():
    if wh.warehouse_type and "PRO" in str(wh.warehouse_type).upper():
        warehouse_id = wh.id
        print(f"Using warehouse: {wh.name} ({wh.id})")
        break

if not warehouse_id:
    all_wh = list(w.warehouses.list())
    if all_wh:
        warehouse_id = all_wh[0].id
        print(f"Fallback warehouse: {all_wh[0].name} ({warehouse_id})")
    else:
        raise Exception("No SQL warehouse found. Create one first.")

# COMMAND ----------

serialized_space = {
    "version": 2,
    "config": {
        "sample_questions": [
            {"id": "a1000000000000000000000000000001", "question": ["Quel est le top 10 des concessions par chiffre d'affaires ?"]},
            {"id": "a1000000000000000000000000000002", "question": ["Quelle est la part de véhicules électriques par groupe de concessions ?"]},
            {"id": "a1000000000000000000000000000003", "question": ["Quel est le modèle le plus vendu ce mois-ci ?"]},
            {"id": "a1000000000000000000000000000004", "question": ["Classement des groupes par CA et part de neuf"]},
            {"id": "a1000000000000000000000000000005", "question": ["Quelle est l'évolution des ventes EV par mois ?"]},
            {"id": "a1000000000000000000000000000006", "question": ["Prix moyen par modèle Renault en occasion ?"]},
            {"id": "a1000000000000000000000000000007", "question": ["Quelles concessions ont le plus de véhicules neufs ?"]},
        ]
    },
    "data_sources": {
        "tables": [
            {
                "identifier": f"{CATALOG}.{SCHEMA}.listings_detail",
                "description": [
                    "Détail de chaque annonce de véhicule avec toutes les dimensions jointes. ",
                    "Une ligne par véhicule listé. Contient : concession, groupe, marque, modèle, version, ",
                    "segment, année d'immatriculation, kilométrage, énergie, état, prix, nombre de photos. ",
                    "Table principale pour les questions détaillées sur les véhicules."
                ],
                "column_configs": [
                    {"column_name": "brand", "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "concession_name", "enable_entity_matching": True},
                    {"column_name": "energy", "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "etat", "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "group_name", "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "model", "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "region", "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "segment", "enable_format_assistance": True, "enable_entity_matching": True},
                ]
            },
            {
                "identifier": f"{CATALOG}.{SCHEMA}.concession_daily_kpis",
                "description": [
                    "KPIs agrégés par concession et par jour. ",
                    "Une ligne par concession x jour ouvert. Contient : nombre d'annonces, CA, prix moyen, ",
                    "mix neuf/occasion, mix énergie (électrique, hybride, thermique), profil véhicule moyen. ",
                    "Utiliser pour les tendances temporelles et comparaisons entre concessions."
                ],
                "column_configs": [
                    {"column_name": "concession_name", "enable_entity_matching": True},
                    {"column_name": "group_name", "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "region", "enable_format_assistance": True, "enable_entity_matching": True},
                ]
            },
            {
                "identifier": f"{CATALOG}.{SCHEMA}.group_scorecard",
                "description": [
                    "Scorecard par groupe de concessions et par mois. ",
                    "Contient : classement par CA, volume, part EV. ",
                    "rank_revenue, rank_volume, rank_ev_share sont les classements mensuels."
                ],
                "column_configs": [
                    {"column_name": "group_name", "enable_format_assistance": True, "enable_entity_matching": True},
                ]
            },
            {
                "identifier": f"{CATALOG}.{SCHEMA}.model_performance",
                "description": [
                    "Performance par modèle et par mois. ",
                    "Une ligne par marque x modèle x version x mois. ",
                    "Contient : volume, CA, prix moyen/médian, part EV, part neuf."
                ],
                "column_configs": [
                    {"column_name": "brand", "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "model", "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "segment", "enable_format_assistance": True, "enable_entity_matching": True},
                ]
            }
        ]
    },
    "instructions": {
        "text_instructions": [
            {
                "id": "01f0b37c378e1c9100000000000000a1",
                "content": [
                    "Tu es un assistant analytics pour le réseau de concessions Renault en France. ",
                    "DONNÉES : ventes de véhicules neufs et d'occasion (Renault, Dacia, Alpine). ",
                    "TABLES DISPONIBLES : ",
                    "1) listings_detail : détail annonce (véhicule, prix, km, état, énergie, concession, groupe). ",
                    "2) concession_daily_kpis : KPIs par concession x jour (volume, CA, mix énergie/neuf). ",
                    "3) group_scorecard : classement mensuel des groupes de concessions. ",
                    "4) model_performance : performance par modèle x mois. ",
                    "SYNONYMES : 'CA' = chiffre d'affaires = total_revenue. 'VE' = 'EV' = véhicule électrique. ",
                    "'VO' = véhicule d'occasion = etat != 'Neuf'. 'VN' = véhicule neuf = etat = 'Neuf'. ",
                    "'concessionnaire' = concession. 'groupe' = group_name. ",
                    "SEGMENTS : B = citadine, C = compacte, D = berline, SUV = SUV, EV = électrique, VU = utilitaire. ",
                    "MARQUES : Renault (majoritaire), Dacia (entrée de gamme), Alpine (sport/premium). ",
                    "GROUPES : Bernard, Gueudet, Jean Lain, Bodemer, Dubreuil, Mary, Parot, Claro. ",
                    "Réponds toujours en français."
                ]
            }
        ],
        "example_question_sqls": [
            {
                "id": "01f0821116d912db00000000000001b1",
                "question": ["Top 10 concessions par CA"],
                "sql": [
                    f"SELECT concession_name, region, group_name, ",
                    f"  SUM(total_revenue) AS ca, ",
                    f"  SUM(nb_listings) AS nb_annonces, ",
                    f"  ROUND(SUM(total_revenue) / NULLIF(SUM(nb_listings), 0), 0) AS prix_moyen ",
                    f"FROM {CATALOG}.{SCHEMA}.concession_daily_kpis ",
                    f"GROUP BY concession_name, region, group_name ",
                    f"ORDER BY ca DESC LIMIT 10"
                ]
            },
            {
                "id": "01f0821116d912db00000000000002b2",
                "question": ["Part de véhicules électriques par groupe"],
                "sql": [
                    f"SELECT group_name, ",
                    f"  SUM(nb_listings) AS total, ",
                    f"  SUM(nb_ev) AS nb_ev, ",
                    f"  ROUND(SUM(nb_ev) * 100.0 / NULLIF(SUM(nb_listings), 0), 1) AS part_ev_pct ",
                    f"FROM {CATALOG}.{SCHEMA}.group_scorecard ",
                    f"GROUP BY group_name ",
                    f"ORDER BY part_ev_pct DESC"
                ]
            },
            {
                "id": "01f0821116d912db00000000000003b3",
                "question": ["Modèles les plus vendus"],
                "sql": [
                    f"SELECT brand, model, version, segment, ",
                    f"  SUM(nb_listings) AS nb_annonces, ",
                    f"  SUM(total_revenue) AS ca, ",
                    f"  ROUND(AVG(avg_price), 0) AS prix_moyen ",
                    f"FROM {CATALOG}.{SCHEMA}.model_performance ",
                    f"GROUP BY brand, model, version, segment ",
                    f"ORDER BY nb_annonces DESC LIMIT 10"
                ]
            },
            {
                "id": "01f0821116d912db00000000000004b4",
                "question": ["Prix moyen des Clio d'occasion par année d'immatriculation"],
                "sql": [
                    f"SELECT year_immat, ",
                    f"  COUNT(*) AS nb, ",
                    f"  ROUND(AVG(price), 0) AS prix_moyen, ",
                    f"  ROUND(AVG(km), 0) AS km_moyen ",
                    f"FROM {CATALOG}.{SCHEMA}.listings_detail ",
                    f"WHERE model = 'Clio' AND etat != 'Neuf' ",
                    f"GROUP BY year_immat ",
                    f"ORDER BY year_immat DESC"
                ]
            },
            {
                "id": "01f0821116d912db00000000000005b5",
                "question": ["Évolution mensuelle des ventes EV"],
                "sql": [
                    f"SELECT year, month, ",
                    f"  SUM(nb_electrique) AS nb_ev, ",
                    f"  SUM(nb_listings) AS total, ",
                    f"  ROUND(SUM(nb_electrique) * 100.0 / NULLIF(SUM(nb_listings), 0), 1) AS part_ev_pct ",
                    f"FROM {CATALOG}.{SCHEMA}.concession_daily_kpis ",
                    f"GROUP BY year, month ",
                    f"ORDER BY year, month"
                ]
            }
        ],
        "sql_snippets": {
            "filters": [
                {"id": "01f09972e66d100000000000000001d1", "sql": ["etat = 'Neuf'"], "display_name": "véhicules neufs uniquement", "synonyms": ["VN", "neuf", "neufs"]},
                {"id": "01f09972e66d100000000000000002d2", "sql": ["etat != 'Neuf'"], "display_name": "occasions uniquement", "synonyms": ["VO", "occasion", "occasions"]},
                {"id": "01f09972e66d100000000000000003d3", "sql": ["energy = 'Électrique'"], "display_name": "électriques uniquement", "synonyms": ["EV", "VE", "électrique"]},
                {"id": "01f09972e66d100000000000000004d4", "sql": ["brand = 'Renault'"], "display_name": "Renault uniquement", "synonyms": ["Renault"]},
                {"id": "01f09972e66d100000000000000005d5", "sql": ["brand = 'Dacia'"], "display_name": "Dacia uniquement", "synonyms": ["Dacia"]},
            ],
            "measures": [
                {"id": "01f09972611f100000000000000001f1", "alias": "ca_total", "sql": ["SUM(total_revenue)"], "display_name": "CA total", "synonyms": ["chiffre d'affaires", "CA", "revenue"]},
                {"id": "01f09972611f100000000000000002f2", "alias": "prix_moyen", "sql": ["ROUND(AVG(price), 0)"], "display_name": "prix moyen", "synonyms": ["prix moyen", "average price"]},
            ]
        }
    },
    "benchmarks": {
        "questions": [
            {
                "id": "01f0d0b4e81510000000000000000ae1",
                "question": ["Quel groupe a la meilleure part EV ?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT group_name, ",
                    f"  ROUND(SUM(nb_ev) * 100.0 / NULLIF(SUM(nb_listings), 0), 1) AS part_ev_pct ",
                    f"FROM {CATALOG}.{SCHEMA}.group_scorecard ",
                    f"GROUP BY group_name ",
                    f"ORDER BY part_ev_pct DESC LIMIT 1"
                ]}]
            },
            {
                "id": "01f0d0b4e81510000000000000000ae2",
                "question": ["Top 3 modèles par CA"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT brand, model, version, SUM(total_revenue) AS ca ",
                    f"FROM {CATALOG}.{SCHEMA}.model_performance ",
                    f"GROUP BY brand, model, version ",
                    f"ORDER BY ca DESC LIMIT 3"
                ]}]
            }
        ]
    }
}

# Sort per Databricks validation rules
def sort_genie_space(space):
    if "config" in space and "sample_questions" in space["config"]:
        space["config"]["sample_questions"].sort(key=lambda x: x["id"])
    if "data_sources" in space:
        if "tables" in space["data_sources"]:
            space["data_sources"]["tables"].sort(key=lambda x: x["identifier"])
            for table in space["data_sources"]["tables"]:
                if "column_configs" in table:
                    table["column_configs"].sort(key=lambda x: x["column_name"])
    if "instructions" in space:
        instr = space["instructions"]
        for key in ["text_instructions", "example_question_sqls"]:
            if key in instr:
                instr[key].sort(key=lambda x: x["id"])
        if "sql_snippets" in instr:
            for key in ["filters", "measures"]:
                if key in instr["sql_snippets"]:
                    instr["sql_snippets"][key].sort(key=lambda x: x["id"])
    if "benchmarks" in space and "questions" in space["benchmarks"]:
        space["benchmarks"]["questions"].sort(key=lambda x: x["id"])
    return space

serialized_space = sort_genie_space(serialized_space)

# COMMAND ----------

try:
    space = w.genie.create_space(
        warehouse_id=warehouse_id,
        title="Renault - Ventes Automobiles",
        description="Analytics ventes automobiles du réseau Renault/Dacia/Alpine. Volumes, CA, mix énergie, performance modèles, classement groupes de concessions.",
        serialized_space=json.dumps(serialized_space),
        parent_path=f"/Workspace/Users/{spark.sql('SELECT current_user()').collect()[0][0]}"
    )
    print(f"Genie space created!")
    print(f"  Space ID: {space.space_id}")
    print(f"  Title: {space.title}")
    print(f"  URL: Open your workspace and navigate to AI/BI Genie -> '{space.title}'")
except Exception as e:
    print(f"Genie space creation failed:\n   {e}")
    print(f"\nSerialized space JSON (for manual creation):")
    print(json.dumps(serialized_space, indent=2, ensure_ascii=False))

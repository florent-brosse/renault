# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Lakebase Setup (Synced Tables)
# MAGIC
# MAGIC The Lakebase project is created by the DAB (`databricks.yml`).
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Enables CDF on Gold tables
# MAGIC 2. Creates **synced tables** to replicate Gold Delta → Lakebase Postgres (for pgrest)
# MAGIC 3. Triggers initial sync
# MAGIC
# MAGIC Synced tables are created in the **source catalog/schema** and auto-appear in
# MAGIC the Lakebase Postgres database. No UC catalog registration needed.

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../generators/config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Enable CDF on Gold tables (required for Triggered/Continuous sync)

# COMMAND ----------

gold_tables = [
    "listings_detail",
    "concession_daily_kpis",
    "model_performance",
    "group_scorecard",
]

for table in gold_tables:
    fqn = f"{CATALOG}.car_sales.{table}"
    try:
        spark.sql(f"ALTER TABLE {fqn} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
        print(f"CDF enabled on {fqn}")
    except Exception as e:
        print(f"CDF on {fqn}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create synced tables (Delta → Lakebase)
# MAGIC
# MAGIC Synced tables are created in the same catalog/schema as the source.
# MAGIC They auto-appear in the Lakebase Postgres database as `"car_sales"."table_name_synced"`.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import SyncedDatabaseTable, SyncedTableSpec

w = WorkspaceClient()

LAKEBASE_PROJECT = "renault-lakebase"

# Primary keys for each Gold table
SYNC_CONFIG = [
    {"table": "listings_detail", "pk": ["listing_id"]},
    {"table": "concession_daily_kpis", "pk": ["concession_id", "sale_date"]},
    {"table": "model_performance", "pk": ["brand", "model", "version", "year", "month"]},
    {"table": "group_scorecard", "pk": ["group_id", "year", "month"]},
]

for cfg in SYNC_CONFIG:
    source = f"{CATALOG}.car_sales.{cfg['table']}"
    # Synced table lives in the same catalog/schema as source
    dest = f"{CATALOG}.car_sales.{cfg['table']}_synced"
    try:
        synced = w.database.create_synced_database_table(
            SyncedDatabaseTable(
                name=dest,
                spec=SyncedTableSpec(
                    source_table_full_name=source,
                    primary_key_columns=cfg["pk"],
                    scheduling_policy="TRIGGERED"
                )
            )
        )
        print(f"Synced table created: {source} → Lakebase")
        print(f"  UC table: {dest}")
        print(f"  Postgres: \"car_sales\".\"{cfg['table']}_synced\"")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"Already exists: {dest}")
        else:
            print(f"Sync {cfg['table']} failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Trigger initial sync

# COMMAND ----------

for cfg in SYNC_CONFIG:
    dest = f"{CATALOG}.car_sales.{cfg['table']}_synced"
    try:
        table_info = w.database.get_synced_database_table(name=dest)
        pipeline_id = table_info.data_synchronization_status.pipeline_id
        w.pipelines.start_update(pipeline_id=pipeline_id)
        print(f"Sync triggered: {dest} (pipeline: {pipeline_id})")
    except Exception as e:
        print(f"Trigger {cfg['table']}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Summary
# MAGIC
# MAGIC Once synced, query data in Postgres via pgrest:
# MAGIC ```sql
# MAGIC SELECT * FROM "car_sales"."listings_detail_synced" WHERE group_id = 'GRP-01';
# MAGIC ```

# COMMAND ----------

print("=" * 60)
print("  LAKEBASE SYNCED TABLES SETUP COMPLETE")
print("=" * 60)
print(f"\n  Lakebase project: {LAKEBASE_PROJECT}")
print(f"\n  Synced tables (Delta → Lakebase Postgres):")
for cfg in SYNC_CONFIG:
    print(f"    {CATALOG}.car_sales.{cfg['table']} → \"car_sales\".\"{cfg['table']}_synced\"")
print(f"\n  Sync mode: TRIGGERED")
print(f"\n  Postgres access: connect to Lakebase endpoint, query \"car_sales\".\"*_synced\" tables")

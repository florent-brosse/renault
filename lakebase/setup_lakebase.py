# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Lakebase Setup (Synced Tables)
# MAGIC
# MAGIC The Lakebase project is created by the DAB (`databricks.yml`).
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Creates **synced tables** to replicate Gold Delta tables → Lakebase (for pgrest)
# MAGIC 2. Creates reference tables directly in Lakebase (for CRUD via App)
# MAGIC
# MAGIC **Prerequisite**: Lakebase project must be deployed (`databricks bundle deploy`)
# MAGIC and registered as a UC catalog via: Catalog Explorer → + → Create catalog → Lakebase Postgres

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../generators/config

# COMMAND ----------

try:
    dbutils.widgets.text("lakebase_catalog", "renault_lakebase", "Lakebase catalog name")
    LAKEBASE_CATALOG = dbutils.widgets.get("lakebase_catalog")
except Exception:
    LAKEBASE_CATALOG = "renault_lakebase"

print(f"Lakebase catalog: {LAKEBASE_CATALOG}")
print(f"Delta catalog: {CATALOG}")

# Check if Lakebase catalog exists
try:
    spark.sql(f"DESCRIBE CATALOG {LAKEBASE_CATALOG}")
    print(f"Lakebase catalog '{LAKEBASE_CATALOG}' found — proceeding with setup")
except Exception:
    print(f"Lakebase catalog '{LAKEBASE_CATALOG}' not found. Skipping.")
    print(f"  Register it via: Catalog Explorer → + → Create catalog → Lakebase Postgres")
    dbutils.notebook.exit("SKIPPED: Lakebase catalog not found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Enable CDF on Gold tables (required for synced tables)

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
# MAGIC Synced tables automatically replicate Gold data into Lakebase Postgres.
# MAGIC Uses TRIGGERED mode — sync on demand or at intervals.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import SyncedDatabaseTable, SyncedTableSpec

w = WorkspaceClient()

# Primary keys for each Gold table
SYNC_CONFIG = [
    {"table": "listings_detail", "pk": ["listing_id"]},
    {"table": "concession_daily_kpis", "pk": ["concession_id", "sale_date"]},
    {"table": "model_performance", "pk": ["brand", "model", "version", "year", "month"]},
    {"table": "group_scorecard", "pk": ["group_id", "year", "month"]},
]

for cfg in SYNC_CONFIG:
    source = f"{CATALOG}.car_sales.{cfg['table']}"
    dest = f"{LAKEBASE_CATALOG}.public.{cfg['table']}"
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
        print(f"Synced table created: {source} → {dest}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"Synced table already exists: {dest}")
        else:
            print(f"Sync {cfg['table']} failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Trigger initial sync

# COMMAND ----------

for cfg in SYNC_CONFIG:
    dest = f"{LAKEBASE_CATALOG}.public.{cfg['table']}"
    try:
        table_info = w.database.get_synced_database_table(name=dest)
        pipeline_id = table_info.data_synchronization_status.pipeline_id
        w.pipelines.start_update(pipeline_id=pipeline_id)
        print(f"Sync triggered for {dest} (pipeline: {pipeline_id})")
    except Exception as e:
        print(f"Trigger sync {cfg['table']}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create reference tables in Lakebase (CRUD for App)

# COMMAND ----------

# Reference tables created directly in Lakebase via SQL through the catalog
try:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LAKEBASE_CATALOG}.public.ref_concession_groups (
      group_id VARCHAR(10),
      group_name VARCHAR(100),
      regions STRING,
      contact_email VARCHAR(200),
      is_active BOOLEAN
    )
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LAKEBASE_CATALOG}.public.ref_price_adjustments (
      model_id VARCHAR(10),
      segment VARCHAR(20),
      adjustment_pct DECIMAL(5,2),
      reason VARCHAR(200),
      valid_from DATE,
      valid_to DATE,
      created_by VARCHAR(200)
    )
    """)
    print("Reference tables created in Lakebase")
except Exception as e:
    print(f"Ref tables: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Seed reference tables

# COMMAND ----------

try:
    seed_data = [(g["group_id"], g["group_name"], ", ".join(g["regions"]), None, True) for g in CONCESSION_GROUPS]
    df = spark.createDataFrame(seed_data, ["group_id", "group_name", "regions", "contact_email", "is_active"])
    df.write.mode("overwrite").saveAsTable(f"{LAKEBASE_CATALOG}.public.ref_concession_groups")
    print(f"Seeded {len(CONCESSION_GROUPS)} concession groups")
except Exception as e:
    print(f"Seed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary

# COMMAND ----------

print("=" * 60)
print("  LAKEBASE SETUP COMPLETE")
print("=" * 60)
print(f"\n  Lakebase catalog: {LAKEBASE_CATALOG}")
print(f"\n  Synced tables (Delta → Lakebase, auto-replicated):")
for cfg in SYNC_CONFIG:
    print(f"    {CATALOG}.car_sales.{cfg['table']} → {LAKEBASE_CATALOG}.public.{cfg['table']}")
print(f"\n  Reference tables (CRUD via App):")
print(f"    {LAKEBASE_CATALOG}.public.ref_concession_groups")
print(f"    {LAKEBASE_CATALOG}.public.ref_price_adjustments")
print(f"\n  Sync mode: TRIGGERED (run on demand or at intervals)")

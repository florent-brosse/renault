# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Lakebase Setup
# MAGIC
# MAGIC Creates a Lakebase (managed Postgres) instance and sets up:
# MAGIC 1. **Mirror tables** from Gold Delta tables → Lakebase (auto-synced)
# MAGIC 2. **Reference tables** in Lakebase for CRUD via REST API
# MAGIC 3. **Sync back** from Lakebase to Delta for ref table updates
# MAGIC
# MAGIC ## Architecture
# MAGIC ```
# MAGIC Gold Delta Tables ──mirror──▶ Lakebase (Postgres) ──REST API──▶ Apps / External
# MAGIC                                     │
# MAGIC                               Ref tables (CRUD)
# MAGIC                                     │
# MAGIC                              sync back to Delta
# MAGIC ```

# COMMAND ----------

# MAGIC %run ../generators/config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Lakebase instance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the Lakebase database (managed Postgres)
# MAGIC -- This creates a serverless Postgres instance in Unity Catalog
# MAGIC CREATE DATABASE IF NOT EXISTS ${renault_catalog}.car_sales_lakebase
# MAGIC USING LAKEBASE;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create mirror tables (Gold → Lakebase)
# MAGIC
# MAGIC Mirror tables automatically sync Delta table data into Lakebase.
# MAGIC They are read-only in Lakebase but queryable via SQL/REST.

# COMMAND ----------

# Mirror the key Gold tables into Lakebase for REST API access
mirror_tables = [
    "listings_detail",
    "concession_daily_kpis",
    "model_performance",
    "group_scorecard",
]

for table in mirror_tables:
    try:
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.car_sales_lakebase.{table}
        USING MIRROR
        TBLPROPERTIES ('source' = '{CATALOG}.car_sales.{table}')
        """)
        print(f"Mirror created: {CATALOG}.car_sales_lakebase.{table}")
    except Exception as e:
        print(f"Mirror {table}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create reference tables in Lakebase (CRUD-enabled)
# MAGIC
# MAGIC These tables live natively in Lakebase and can be updated via REST/SQL.
# MAGIC A Databricks App or external tool can CRUD these.

# COMMAND ----------

# Reference tables for the optional Databricks App to manage
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.car_sales_lakebase.ref_concession_groups (
  group_id VARCHAR(10) PRIMARY KEY,
  group_name VARCHAR(100) NOT NULL,
  regions TEXT,
  contact_email VARCHAR(200),
  is_active BOOLEAN DEFAULT TRUE,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.car_sales_lakebase.ref_price_adjustments (
  adjustment_id SERIAL PRIMARY KEY,
  model_id VARCHAR(10) NOT NULL,
  segment VARCHAR(20),
  adjustment_pct DECIMAL(5,2) COMMENT 'Price adjustment percentage (-10 = 10% discount)',
  reason VARCHAR(200),
  valid_from DATE NOT NULL,
  valid_to DATE,
  created_by VARCHAR(200),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

print("Reference tables created in Lakebase")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Seed reference tables with initial data

# COMMAND ----------

# Seed concession groups into Lakebase
for g in CONCESSION_GROUPS:
    regions_str = ", ".join(g["regions"])
    spark.sql(f"""
    INSERT INTO {CATALOG}.car_sales_lakebase.ref_concession_groups (group_id, group_name, regions)
    VALUES ('{g["group_id"]}', '{g["group_name"]}', '{regions_str}')
    ON CONFLICT (group_id) DO UPDATE SET group_name = EXCLUDED.group_name, regions = EXCLUDED.regions
    """)

print(f"Seeded {len(CONCESSION_GROUPS)} concession groups into Lakebase")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Set up sync back to Delta (Lakebase → Delta)
# MAGIC
# MAGIC For reference tables edited in Lakebase/App, we sync changes back
# MAGIC to Delta tables so they're available in the Lakehouse.

# COMMAND ----------

# Create Delta tables that mirror the Lakebase ref tables back
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.car_sales.ref_concession_groups_synced
USING MIRROR
TBLPROPERTIES ('source' = '{CATALOG}.car_sales_lakebase.ref_concession_groups')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.car_sales.ref_price_adjustments_synced
USING MIRROR
TBLPROPERTIES ('source' = '{CATALOG}.car_sales_lakebase.ref_price_adjustments')
""")

print("Sync-back mirrors created (Lakebase → Delta)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary

# COMMAND ----------

print("=" * 60)
print("  LAKEBASE SETUP COMPLETE")
print("=" * 60)
print(f"\n  Lakebase database: {CATALOG}.car_sales_lakebase")
print(f"\n  Mirror tables (Gold → Lakebase, read-only):")
for t in mirror_tables:
    print(f"    {CATALOG}.car_sales_lakebase.{t}")
print(f"\n  Reference tables (CRUD via REST/App):")
print(f"    {CATALOG}.car_sales_lakebase.ref_concession_groups")
print(f"    {CATALOG}.car_sales_lakebase.ref_price_adjustments")
print(f"\n  Sync-back tables (Lakebase → Delta):")
print(f"    {CATALOG}.car_sales.ref_concession_groups_synced")
print(f"    {CATALOG}.car_sales.ref_price_adjustments_synced")
print(f"\n  REST API endpoint: Use Lakebase connection string from UC")
print(f"  See: https://docs.databricks.com/en/database-objects/lakebase.html")

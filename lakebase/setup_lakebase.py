# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Lakebase Setup
# MAGIC
# MAGIC The Lakebase project + branch + endpoint are created by the DAB (`databricks.yml`).
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Registers the Lakebase project as a Unity Catalog catalog (if not already done)
# MAGIC 2. Creates reference tables in Lakebase
# MAGIC 3. Seeds them with concession group data
# MAGIC 4. Shows cross-catalog queries (Lakebase + Delta)
# MAGIC
# MAGIC **Prerequisite**: The Lakebase project must be deployed first (`databricks bundle deploy`).
# MAGIC If the catalog doesn't exist yet, register it via:
# MAGIC Catalog Explorer → + → Create catalog → Lakebase Postgres → select `renault-lakebase` project

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

# Check if Lakebase catalog exists — skip gracefully if not
try:
    spark.sql(f"DESCRIBE CATALOG {LAKEBASE_CATALOG}")
    print(f"Lakebase catalog '{LAKEBASE_CATALOG}' found — proceeding with setup")
except Exception as e:
    print(f"Lakebase catalog '{LAKEBASE_CATALOG}' not found. Skipping Lakebase setup.")
    print(f"  Register it via: Catalog Explorer → + → Create catalog → Lakebase Postgres → select 'renault-lakebase'")
    dbutils.notebook.exit("SKIPPED: Lakebase catalog not found — register it in Catalog Explorer first")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create reference tables in Lakebase (CRUD-enabled)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {LAKEBASE_CATALOG}.public.ref_concession_groups (
  group_id VARCHAR(10) PRIMARY KEY,
  group_name VARCHAR(100) NOT NULL,
  regions STRING,
  contact_email VARCHAR(200),
  is_active BOOLEAN DEFAULT TRUE
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {LAKEBASE_CATALOG}.public.ref_price_adjustments (
  adjustment_id SERIAL PRIMARY KEY,
  model_id VARCHAR(10) NOT NULL,
  segment VARCHAR(20),
  adjustment_pct DECIMAL(5,2),
  reason VARCHAR(200),
  valid_from DATE NOT NULL,
  valid_to DATE,
  created_by VARCHAR(200)
)
""")

print("Reference tables created in Lakebase")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Seed reference tables

# COMMAND ----------

for g in CONCESSION_GROUPS:
    regions_str = ", ".join(g["regions"])
    spark.sql(f"""
    INSERT INTO {LAKEBASE_CATALOG}.public.ref_concession_groups (group_id, group_name, regions)
    VALUES ('{g["group_id"]}', '{g["group_name"]}', '{regions_str}')
    ON CONFLICT (group_id) DO UPDATE SET group_name = EXCLUDED.group_name, regions = EXCLUDED.regions
    """)

print(f"Seeded {len(CONCESSION_GROUPS)} concession groups into Lakebase")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cross-catalog query (Lakebase + Delta)

# COMMAND ----------

spark.sql(f"""
SELECT
  g.group_name,
  g.regions,
  g.is_active,
  COUNT(DISTINCT l.concession_id) AS nb_concessions,
  COUNT(*) AS nb_listings,
  SUM(l.price) AS total_revenue
FROM {LAKEBASE_CATALOG}.public.ref_concession_groups g
JOIN {CATALOG}.car_sales.listings_detail l
  ON g.group_id = l.group_id
GROUP BY g.group_name, g.regions, g.is_active
ORDER BY total_revenue DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Summary

# COMMAND ----------

print("=" * 60)
print("  LAKEBASE SETUP COMPLETE")
print("=" * 60)
print(f"\n  Lakebase catalog: {LAKEBASE_CATALOG}")
print(f"\n  Reference tables (CRUD via SQL/App):")
print(f"    {LAKEBASE_CATALOG}.public.ref_concession_groups")
print(f"    {LAKEBASE_CATALOG}.public.ref_price_adjustments")
print(f"\n  Cross-catalog query: Lakebase + Delta in single SQL")

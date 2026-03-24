# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Lakebase Setup (pgrest exposure)
# MAGIC
# MAGIC The Lakebase project is created by the DAB (`databricks.yml`).
# MAGIC
# MAGIC This notebook connects directly to the Lakebase Postgres endpoint and:
# MAGIC 1. Creates reference tables
# MAGIC 2. Syncs Gold data from Delta into Lakebase for REST API exposure
# MAGIC 3. Seeds reference tables with concession group data
# MAGIC
# MAGIC No `CREATE CATALOG` needed — we write directly to Postgres.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../generators/config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Get Lakebase connection credentials

# COMMAND ----------

import json, subprocess

PROJECT_ID = "renault-lakebase"

# Get the production branch ID
result = subprocess.run(
    ["databricks", "postgres", "list-branches", f"projects/{PROJECT_ID}", "--output", "json"],
    capture_output=True, text=True
)
branches = json.loads(result.stdout)
branch_id = None
for b in branches.get("branches", []):
    if "production" in b.get("name", ""):
        branch_id = b["name"].split("/")[-1]
        break

if not branch_id:
    branch_id = branches["branches"][0]["name"].split("/")[-1] if branches.get("branches") else None

if not branch_id:
    print("No Lakebase branch found. Skipping.")
    dbutils.notebook.exit("SKIPPED: No Lakebase branch found")

print(f"Branch: {branch_id}")

# Get endpoint
result = subprocess.run(
    ["databricks", "postgres", "list-endpoints", f"projects/{PROJECT_ID}/branches/{branch_id}", "--output", "json"],
    capture_output=True, text=True
)
endpoints = json.loads(result.stdout)
endpoint_id = None
for ep in endpoints.get("endpoints", []):
    endpoint_id = ep["name"].split("/")[-1]
    dns = ep.get("status", {}).get("read_write_dns", "")
    break

if not endpoint_id:
    print("No Lakebase endpoint found. Skipping.")
    dbutils.notebook.exit("SKIPPED: No Lakebase endpoint found")

print(f"Endpoint: {endpoint_id}")
print(f"DNS: {dns}")

# Generate credentials
result = subprocess.run(
    ["databricks", "postgres", "generate-database-credential",
     f"projects/{PROJECT_ID}/branches/{branch_id}/endpoints/{endpoint_id}",
     "--output", "json"],
    capture_output=True, text=True
)
creds = json.loads(result.stdout)
pg_user = creds.get("username", "")
pg_password = creds.get("password", "")
pg_host = dns
pg_port = 5432
pg_database = "databricks_postgres"

print(f"Connected as: {pg_user}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create tables in Lakebase

# COMMAND ----------

import psycopg2

conn = psycopg2.connect(
    host=pg_host, port=pg_port, dbname=pg_database,
    user=pg_user, password=pg_password,
    sslmode="require"
)
conn.autocommit = True
cur = conn.cursor()

# Reference tables (CRUD by App)
cur.execute("""
CREATE TABLE IF NOT EXISTS public.ref_concession_groups (
    group_id VARCHAR(10) PRIMARY KEY,
    group_name VARCHAR(100) NOT NULL,
    regions TEXT,
    contact_email VARCHAR(200),
    is_active BOOLEAN DEFAULT TRUE
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS public.ref_price_adjustments (
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

# Gold data tables for pgrest exposure
cur.execute("""
CREATE TABLE IF NOT EXISTS public.listings_detail (
    listing_id VARCHAR(30) PRIMARY KEY,
    sale_date DATE,
    concession_id VARCHAR(10),
    concession_name VARCHAR(100),
    city VARCHAR(50),
    region VARCHAR(50),
    group_id VARCHAR(10),
    group_name VARCHAR(100),
    model_id VARCHAR(10),
    brand VARCHAR(20),
    model VARCHAR(30),
    version VARCHAR(10),
    segment VARCHAR(20),
    year_immat INTEGER,
    age INTEGER,
    km INTEGER,
    energy VARCHAR(20),
    etat VARCHAR(30),
    price INTEGER,
    nb_photos INTEGER
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS public.concession_daily_kpis (
    concession_id VARCHAR(10),
    sale_date DATE,
    concession_name VARCHAR(100),
    region VARCHAR(50),
    group_name VARCHAR(100),
    nb_listings INTEGER,
    total_revenue BIGINT,
    avg_price INTEGER,
    nb_electrique INTEGER,
    nb_hybride INTEGER,
    nb_thermique INTEGER,
    PRIMARY KEY (concession_id, sale_date)
)
""")

print("Tables created in Lakebase")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Seed reference tables

# COMMAND ----------

for g in CONCESSION_GROUPS:
    regions_str = ", ".join(g["regions"])
    cur.execute("""
        INSERT INTO public.ref_concession_groups (group_id, group_name, regions)
        VALUES (%s, %s, %s)
        ON CONFLICT (group_id) DO UPDATE SET group_name = EXCLUDED.group_name, regions = EXCLUDED.regions
    """, (g["group_id"], g["group_name"], regions_str))

print(f"Seeded {len(CONCESSION_GROUPS)} concession groups")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sync Gold data from Delta → Lakebase

# COMMAND ----------

# Read Gold data from Delta
df_listings = spark.sql(f"""
SELECT listing_id, sale_date, concession_id, concession_name, city, region,
       group_id, group_name, model_id, brand, model, version, segment,
       year_immat, age, km, energy, etat, price, nb_photos
FROM {CATALOG}.car_sales.listings_detail
""")

df_kpis = spark.sql(f"""
SELECT concession_id, sale_date, concession_name, region, group_name,
       nb_listings, total_revenue, CAST(avg_price AS INTEGER) AS avg_price,
       nb_electrique, nb_hybride, nb_thermique
FROM {CATALOG}.car_sales.concession_daily_kpis
""")

# Write to Lakebase via JDBC
jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}?sslmode=require"
jdbc_props = {"user": pg_user, "password": pg_password, "driver": "org.postgresql.Driver"}

df_listings.write.mode("overwrite").jdbc(jdbc_url, "public.listings_detail", properties=jdbc_props)
print(f"Synced {df_listings.count()} listings to Lakebase")

df_kpis.write.mode("overwrite").jdbc(jdbc_url, "public.concession_daily_kpis", properties=jdbc_props)
print(f"Synced {df_kpis.count()} KPI rows to Lakebase")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary

# COMMAND ----------

cur.close()
conn.close()

print("=" * 60)
print("  LAKEBASE SETUP COMPLETE")
print("=" * 60)
print(f"\n  Project: {PROJECT_ID}")
print(f"  Host: {pg_host}")
print(f"\n  Tables exposed via pgrest:")
print(f"    public.listings_detail")
print(f"    public.concession_daily_kpis")
print(f"\n  Reference tables (CRUD):")
print(f"    public.ref_concession_groups")
print(f"    public.ref_price_adjustments")

# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Car Sales — Declarative Pipeline
# MAGIC
# MAGIC Full Bronze → Silver → Gold pipeline:
# MAGIC - **Bronze**: Auto Loader ingestion from CSV landing zone (streaming tables)
# MAGIC - **Silver**: Cleansing, type casting, data quality expectations (streaming tables)
# MAGIC - **Gold**: Business aggregations as materialized views (sales KPIs, model mix, concession scorecards)
# MAGIC
# MAGIC ## Pipeline Configuration
# MAGIC - **Target catalog**: set via `renault_catalog`
# MAGIC - **Target schema**: `car_sales`

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *

CATALOG = spark.conf.get("renault_catalog", "renault_demo")
VOLUME_PATH = f"/Volumes/{CATALOG}/landing/car_sales_landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # BRONZE LAYER — Raw ingestion via Auto Loader
# MAGIC ---

# COMMAND ----------

@dp.table(
    name="bronze_car_listings",
    comment="Raw car listings ingested from CSV landing zone via Auto Loader",
    table_properties={"quality": "bronze"},
)
@dp.expect("valid_concession_id", "concession_id IS NOT NULL")
@dp.expect("valid_model_id", "model_id IS NOT NULL")
@dp.expect_or_drop("valid_price", "CAST(price AS INT) > 0")
def bronze_car_listings():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints",
                "price INT, km INT, nb_photos INT, year_immat INT")
        .load(f"{VOLUME_PATH}/sales/")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # SILVER LAYER — Cleansed, typed, validated
# MAGIC ---

# COMMAND ----------

@dp.table(
    name="silver_car_listings",
    comment="Cleansed car listings with type casting and domain validation",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop({
    "valid_listing_id": "listing_id IS NOT NULL",
    "valid_sale_date": "sale_date IS NOT NULL",
    "positive_price": "price > 0",
    "non_negative_km": "km >= 0",
})
@dp.expect_all({
    "valid_etat": "etat IN ('Neuf', 'Occasion - Excellent', 'Occasion - Bon', 'Occasion - Correct')",
    "valid_energy": "energy IN ('Essence', 'Diesel', 'Hybride', 'Électrique', 'GPL', 'Essence + GPL')",
    "reasonable_price": "price < 200000",
    "valid_year": "year_immat BETWEEN 2000 AND 2026",
    "valid_photos": "nb_photos BETWEEN 1 AND 30",
})
def silver_car_listings():
    return (
        spark.readStream.table(f"{CATALOG}.car_sales.bronze_car_listings")
        .withColumn("sale_date", F.to_date("sale_date"))
        .withColumn("price", F.col("price").cast("int"))
        .withColumn("km", F.col("km").cast("int"))
        .withColumn("nb_photos", F.col("nb_photos").cast("int"))
        .withColumn("year_immat", F.col("year_immat").cast("int"))
        .withColumn("age", F.year(F.col("sale_date")) - F.col("year_immat"))
        .withColumn("_ingestion_ts", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # GOLD LAYER — Business aggregations (materialized views)
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Concession Daily KPIs
# MAGIC
# MAGIC One row per concession per day — core sales metrics.

# COMMAND ----------

@dp.materialized_view(
    name="concession_daily_kpis",
    comment="Concession x day KPIs: volume, revenue, avg price, model mix. Grain: one row per concession per open day.",
    table_properties={"quality": "gold"},
    cluster_by=["concession_id", "sale_date"],
)
def gold_concession_daily_kpis():
    return spark.sql(f"""
    SELECT
      s.concession_id,
      c.concession_name,
      c.city,
      c.region,
      c.group_id,
      c.group_name,
      s.sale_date,

      -- Volume
      COUNT(*)                                                    AS nb_listings,
      COUNT(CASE WHEN s.etat = 'Neuf' THEN 1 END)              AS nb_neuf,
      COUNT(CASE WHEN s.etat != 'Neuf' THEN 1 END)             AS nb_occasion,

      -- Revenue
      SUM(s.price)                                               AS total_revenue,
      AVG(s.price)                                               AS avg_price,
      SUM(CASE WHEN s.etat = 'Neuf' THEN s.price ELSE 0 END)  AS revenue_neuf,
      SUM(CASE WHEN s.etat != 'Neuf' THEN s.price ELSE 0 END) AS revenue_occasion,

      -- Vehicle profile
      AVG(s.age)                                                 AS avg_age,
      AVG(s.km)                                                  AS avg_km,
      AVG(s.nb_photos)                                           AS avg_photos,

      -- Energy mix
      COUNT(CASE WHEN s.energy = 'Électrique' THEN 1 END)      AS nb_electrique,
      COUNT(CASE WHEN s.energy = 'Hybride' THEN 1 END)         AS nb_hybride,
      COUNT(CASE WHEN s.energy IN ('Essence', 'Diesel', 'GPL', 'Essence + GPL') THEN 1 END) AS nb_thermique,

      -- Calendar
      dt.day_name,
      dt.is_weekend,
      dt.week_iso,
      dt.month,
      dt.quarter,
      dt.year

    FROM {CATALOG}.car_sales.silver_car_listings s
    JOIN {CATALOG}.car_sales.dim_concession c
      ON s.concession_id = c.concession_id
    LEFT JOIN {CATALOG}.car_sales.dim_date dt
      ON s.sale_date = dt.date_key
    GROUP BY ALL
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Model Performance

# COMMAND ----------

@dp.materialized_view(
    name="model_performance",
    comment="Model x month performance: volume, revenue, avg price, energy mix. Grain: model x month.",
    table_properties={"quality": "gold"},
    cluster_by=["brand", "model"],
)
def gold_model_performance():
    return spark.sql(f"""
    SELECT
      s.brand,
      s.model,
      s.version,
      s.segment,
      YEAR(s.sale_date)                                           AS year,
      MONTH(s.sale_date)                                          AS month,

      COUNT(*)                                                    AS nb_listings,
      SUM(s.price)                                               AS total_revenue,
      AVG(s.price)                                               AS avg_price,
      PERCENTILE(s.price, 0.5)                                   AS median_price,

      AVG(s.age)                                                 AS avg_age,
      AVG(s.km)                                                  AS avg_km,
      AVG(s.nb_photos)                                           AS avg_photos,

      COUNT(CASE WHEN s.etat = 'Neuf' THEN 1 END)              AS nb_neuf,
      COUNT(CASE WHEN s.energy = 'Électrique' THEN 1 END)      AS nb_electrique,
      COUNT(CASE WHEN s.energy = 'Hybride' THEN 1 END)         AS nb_hybride

    FROM {CATALOG}.car_sales.silver_car_listings s
    GROUP BY ALL
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Group Scorecard
# MAGIC
# MAGIC Concession group-level rankings.

# COMMAND ----------

@dp.materialized_view(
    name="group_scorecard",
    comment="Concession group scorecard: rankings by revenue, volume, EV share. Grain: group x month.",
    table_properties={"quality": "gold"},
    cluster_by=["group_id"],
)
def gold_group_scorecard():
    return spark.sql(f"""
    WITH monthly AS (
      SELECT
        s.group_id,
        c.group_name,
        YEAR(s.sale_date) AS year,
        MONTH(s.sale_date) AS month,
        COUNT(*) AS nb_listings,
        SUM(s.price) AS total_revenue,
        AVG(s.price) AS avg_price,
        COUNT(CASE WHEN s.energy = 'Électrique' THEN 1 END) AS nb_ev,
        COUNT(CASE WHEN s.energy = 'Électrique' THEN 1 END) * 100.0 / COUNT(*) AS ev_share_pct,
        COUNT(CASE WHEN s.etat = 'Neuf' THEN 1 END) * 100.0 / COUNT(*) AS neuf_share_pct,
        COUNT(DISTINCT s.concession_id) AS nb_concessions
      FROM {CATALOG}.car_sales.silver_car_listings s
      JOIN {CATALOG}.car_sales.dim_concession c
        ON s.concession_id = c.concession_id
      GROUP BY ALL
    )
    SELECT
      *,
      RANK() OVER (PARTITION BY year, month ORDER BY total_revenue DESC) AS rank_revenue,
      RANK() OVER (PARTITION BY year, month ORDER BY nb_listings DESC)   AS rank_volume,
      RANK() OVER (PARTITION BY year, month ORDER BY ev_share_pct DESC)  AS rank_ev_share
    FROM monthly
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Listings Detail (for AI/BI + Genie)
# MAGIC
# MAGIC Enriched listing-level view with all dimensions joined.

# COMMAND ----------

@dp.materialized_view(
    name="listings_detail",
    comment="Enriched car listings with concession, model, and calendar context. For AI/BI dashboards and Genie.",
    table_properties={"quality": "gold"},
    cluster_by=["concession_id", "sale_date"],
)
def gold_listings_detail():
    return spark.sql(f"""
    SELECT
      s.listing_id,
      s.sale_date,
      s.concession_id,
      c.concession_name,
      c.city,
      c.region,
      s.group_id,
      c.group_name,
      s.model_id,
      s.brand,
      s.model,
      s.version,
      s.segment,
      s.year_immat,
      s.age,
      s.km,
      s.energy,
      s.etat,
      s.price,
      s.nb_photos,
      dt.day_name,
      dt.is_weekend,
      dt.week_iso,
      dt.month,
      dt.quarter,
      dt.year
    FROM {CATALOG}.car_sales.silver_car_listings s
    JOIN {CATALOG}.car_sales.dim_concession c
      ON s.concession_id = c.concession_id
    LEFT JOIN {CATALOG}.car_sales.dim_date dt
      ON s.sale_date = dt.date_key
    """)

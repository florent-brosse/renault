# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — History Generator
# MAGIC
# MAGIC Generates 2 years of historical car sales data as CSV files in a UC Volume.
# MAGIC Simulates the export from a dealership management system (DMS).
# MAGIC
# MAGIC **Run once** before starting the pipeline.
# MAGIC
# MAGIC Output structure:
# MAGIC ```
# MAGIC /Volumes/{catalog}/landing/car_sales_landing/
# MAGIC   sales/YYYY-MM-DD/sales_CON-XXX_YYYY-MM-DD.csv
# MAGIC   dimensions/dim_concession.csv
# MAGIC   dimensions/dim_model.csv
# MAGIC   dimensions/dim_concession_group.csv
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Load shared config

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create catalog, schemas, and volume

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception as e:
    print(f"Catalog already exists or cannot be created (OK): {e}")

for schema in [SCHEMA_LANDING, SCHEMA_CAR_SALES]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_LANDING}.{VOLUME_NAME}")

print(f"Volume path: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate and write dimension tables

# COMMAND ----------

concessions = generate_concessions(seed=42)
car_models = generate_car_models()
groups = generate_concession_groups()
dim_date_rows = generate_dim_date(date.fromisoformat(DATE_START), date.fromisoformat(DATE_END))

DIM_TBLPROPS = {"delta.enableChangeDataFeed": "true", "delta.enableRowTracking": "true"}

for name, data, label in [
    ("dim_concession", concessions, f"{len(concessions)} concessions"),
    ("dim_model", car_models, f"{len(car_models)} models"),
    ("dim_concession_group", groups, f"{len(groups)} groups"),
    ("dim_date", dim_date_rows, f"{len(dim_date_rows)} days"),
]:
    fqn = f"{CATALOG}.{SCHEMA_CAR_SALES}.{name}"
    df = spark.createDataFrame(data)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqn)
    for k, v in DIM_TBLPROPS.items():
        spark.sql(f"ALTER TABLE {fqn} SET TBLPROPERTIES ('{k}' = '{v}')")
    print(f"{name}: {label} -> {fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate car sales data — Spark-based

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# ─── Date x Concession spine ───
df_dates = spark.sql(f"""
  SELECT explode(sequence(
    DATE('{DATE_START}'), DATE('{DATE_END}'), INTERVAL 1 DAY
  )) AS sale_date
""")

df_dates = df_dates.withColumn("dow", F.dayofweek("sale_date"))  # 1=Sun, 7=Sat
df_dates = df_dates.withColumn("month", F.month("sale_date"))
df_dates = df_dates.withColumn("is_open", F.when(F.col("dow") == 1, False).otherwise(True))  # closed Sunday

# Seasonality
df_dates = df_dates.withColumn("seasonality",
    F.when(F.col("month") == 1, 1.3)
     .when(F.col("month") == 3, 1.2)
     .when(F.col("month") == 6, 1.1)
     .when(F.col("month") == 8, 0.5)
     .when(F.col("month") == 9, 1.25)
     .when(F.col("month") == 12, 0.9)
     .otherwise(1.0)
)

# Day-of-week multiplier
df_dates = df_dates.withColumn("dow_mult",
    F.when(F.col("dow") == 7, 1.8)   # Saturday
     .when(F.col("dow") == 6, 1.2)   # Friday
     .when(F.col("dow") == 2, 0.8)   # Monday
     .otherwise(1.0)
)

# Cross join with concessions
df_conc = spark.createDataFrame(concessions)
size_mult = {"S": 0.6, "M": 1.0, "L": 1.5}
df_conc_sized = df_conc.withColumn("size_mult",
    F.when(F.col("size") == "S", 0.6)
     .when(F.col("size") == "M", 1.0)
     .otherwise(1.5)
)

df_spine = df_dates.crossJoin(
    df_conc_sized.select("concession_id", "size", "size_mult", "group_id")
)

base_sales = (SALES_MIN + SALES_MAX) / 2

df_spine = df_spine.withColumn("nb_sales",
    F.when(~F.col("is_open"), 0)
     .otherwise(
        F.greatest(
            F.lit(SALES_MIN),
            F.least(
                F.lit(SALES_MAX),
                (F.lit(base_sales) * F.col("size_mult") * F.col("seasonality") * F.col("dow_mult")
                 * (F.lit(0.8) + F.rand(seed=42) * F.lit(0.4))
                ).cast("int")
            )
        )
     )
)

df_spine = df_spine.filter(F.col("nb_sales") > 0)

total_spine = df_spine.count()
total_sales = df_spine.agg(F.sum("nb_sales")).collect()[0][0]
print(f"Concession x day spine: {total_spine:,} open concession-days")
print(f"Total car listings to generate: {total_sales:,.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Explode into individual car listings

# COMMAND ----------

num_models = len(car_models)

# Explode into individual sales
df_sales = df_spine.withColumn("sale_seq", F.explode(F.sequence(F.lit(1), F.col("nb_sales"))))

# Unique listing ID
df_sales = df_sales.withColumn("listing_id",
    F.concat(
        F.col("concession_id"), F.lit("-"),
        F.date_format("sale_date", "yyyyMMdd"), F.lit("-"),
        F.lpad(F.col("sale_seq").cast("string"), 4, "0")
    )
)

# Assign random model
df_models = spark.createDataFrame([{**m, "model_idx": i} for i, m in enumerate(car_models)])

df_sales = df_sales.withColumn("model_hash",
    F.abs(F.hash(F.col("listing_id"))) % num_models
)

df_sales = df_sales.join(
    F.broadcast(df_models.select(
        "model_idx", "model_id", "brand", "model", "version", "segment",
        "price_min", "price_max", "year_min", "year_max",
    )),
    df_sales["model_hash"] == df_models["model_idx"],
    "inner"
)

# Year of immatriculation (weighted toward recent)
df_sales = df_sales.withColumn("year_immat",
    F.least(
        F.col("year_max"),
        F.greatest(
            F.col("year_min"),
            (F.col("year_max") - F.floor(F.pow(F.rand(seed=101) * 3, 2)).cast("int"))
        )
    )
)

# Age-based depreciation
df_sales = df_sales.withColumn("age", F.year(F.col("sale_date")) - F.col("year_immat"))
df_sales = df_sales.withColumn("depreciation",
    F.when(F.col("age") <= 0, 1.0)
     .when(F.col("age") == 1, 0.80)
     .when(F.col("age") == 2, 0.70)
     .when(F.col("age") == 3, 0.60)
     .when(F.col("age") == 4, 0.52)
     .when(F.col("age") == 5, 0.45)
     .when(F.col("age") == 6, 0.38)
     .when(F.col("age") == 7, 0.32)
     .when(F.col("age") == 8, 0.27)
     .when(F.col("age") == 9, 0.23)
     .otherwise(0.20)
)

# Price = base price with depreciation + noise
df_sales = df_sales.withColumn("base_price",
    F.col("price_min") + (F.col("price_max") - F.col("price_min")) * F.rand(seed=202)
)
df_sales = df_sales.withColumn("price",
    F.round(F.col("base_price") * F.col("depreciation") * (F.lit(0.9) + F.rand(seed=303) * F.lit(0.2)), 0).cast("int")
)

# État (condition) — newer cars are in better condition
df_sales = df_sales.withColumn("etat_rng", F.rand(seed=404))
df_sales = df_sales.withColumn("etat",
    F.when(F.col("age") <= 0, "Neuf")
     .when((F.col("age") <= 2) & (F.col("etat_rng") < 0.7), "Occasion - Excellent")
     .when((F.col("age") <= 2), "Occasion - Bon")
     .when((F.col("age") <= 5) & (F.col("etat_rng") < 0.4), "Occasion - Excellent")
     .when((F.col("age") <= 5) & (F.col("etat_rng") < 0.8), "Occasion - Bon")
     .when(F.col("age") <= 5, "Occasion - Correct")
     .when(F.col("etat_rng") < 0.2, "Occasion - Bon")
     .when(F.col("etat_rng") < 0.6, "Occasion - Correct")
     .otherwise("Occasion - Correct")
)

# Number of photos (newer/pricier cars get more photos)
df_sales = df_sales.withColumn("nb_photos",
    F.greatest(
        F.lit(1),
        F.least(
            F.lit(25),
            (F.lit(8) + F.lit(5) * F.col("depreciation") + F.rand(seed=505) * F.lit(10)).cast("int")
        )
    )
)

# Km
df_sales = df_sales.withColumn("km",
    F.when(F.col("etat") == "Neuf", F.lit(0))
     .otherwise(
        F.greatest(F.lit(1000),
            (F.col("age") * (F.lit(12000) + F.rand(seed=606) * F.lit(8000))).cast("int")
        )
     )
)

# Energy type
df_sales = df_sales.withColumn("energy",
    F.when(F.col("segment").endswith("-EV"), "Électrique")
     .when(F.rand(seed=707) < 0.35, "Essence")
     .when(F.rand(seed=808) < 0.50, "Diesel")
     .when(F.rand(seed=909) < 0.70, "Hybride")
     .otherwise("Essence")
)

# Select final columns
df_fact_sales = df_sales.select(
    "listing_id", "sale_date", "concession_id", "group_id",
    "model_id", "brand", "model", "version", "segment",
    "year_immat", "km", "energy", "etat", "price", "nb_photos"
)

print(f"Car listings generated (writing to CSV...)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write as CSV partitioned by sale_date

# COMMAND ----------

df_fact_sales.write.mode("overwrite") \
    .option("header", "true") \
    .partitionBy("sale_date") \
    .csv(f"{VOLUME_PATH}/sales")

total_listings = spark.read.option("header", "true").csv(f"{VOLUME_PATH}/sales").count()
print(f"Car listings: {total_listings:,.0f} written to {VOLUME_PATH}/sales/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary

# COMMAND ----------

print("=" * 60)
print("  HISTORY GENERATION COMPLETE")
print("=" * 60)
print(f"\n  Volume: {VOLUME_PATH}")
print(f"\n  Delta tables (dimensions):")
print(f"    {CATALOG}.{SCHEMA_CAR_SALES}.dim_concession       : {len(concessions):>6} concessions")
print(f"    {CATALOG}.{SCHEMA_CAR_SALES}.dim_concession_group  : {len(groups):>6} groups")
print(f"    {CATALOG}.{SCHEMA_CAR_SALES}.dim_model             : {len(car_models):>6} models")
print(f"    {CATALOG}.{SCHEMA_CAR_SALES}.dim_date              : {len(dim_date_rows):>6} days")
print(f"\n  CSV files:")
print(f"    sales/ (partitioned by date) : {total_listings:>10,} listings")
print(f"\n  Next step: Create and start the pipeline")

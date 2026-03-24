# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Daily Generator
# MAGIC
# MAGIC Generates **one new day** of car sales data in the UC Volume.
# MAGIC Run during the demo to show live incremental ingestion.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Detect last generated date

# COMMAND ----------

from datetime import date, timedelta

try:
    partitions = [
        d.name.split("=")[1]
        for d in dbutils.fs.ls(f"{VOLUME_PATH}/sales/")
        if d.name.startswith("sale_date=")
    ]
    last_date_str = max(partitions)
    last_date = date.fromisoformat(last_date_str)
    print(f"Last generated date: {last_date} (from {len(partitions)} partitions)")
except Exception:
    last_date = date.fromisoformat(DATE_START) - timedelta(days=1)
    print(f"No existing data found. Will start from {DATE_START}")

next_date = last_date + timedelta(days=1)
while not is_open_day(next_date):
    print(f"  Skipping {next_date} (closed)")
    next_date += timedelta(days=1)

print(f"Generating data for: {next_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate car listings for the day

# COMMAND ----------

import random

concessions = generate_concessions(seed=42)
car_models = generate_car_models()
date_seed = int(next_date.strftime("%Y%m%d"))
rng = random.Random(date_seed)

month = next_date.month
dow_iso = next_date.isoweekday()
seasonality = get_seasonality(month)
dow_mult = get_dow_multiplier(dow_iso)
base_sales = (SALES_MIN + SALES_MAX) / 2

all_rows = []

for conc in concessions:
    size_mult = {"S": 0.6, "M": 1.0, "L": 1.5}[conc["size"]]
    noise = 0.8 + rng.random() * 0.4
    nb_sales = max(SALES_MIN, min(SALES_MAX, int(base_sales * size_mult * seasonality * dow_mult * noise)))

    for seq in range(1, nb_sales + 1):
        listing_id = f"{conc['concession_id']}-{next_date.strftime('%Y%m%d')}-{seq:04d}"

        # Pick model
        model_idx = abs(hash(listing_id)) % len(car_models)
        m = car_models[model_idx]

        # Year immat (weighted toward recent)
        year_max = m["year_range"][1]
        year_min = m["year_range"][0]
        year_immat = min(year_max, max(year_min, year_max - int(rng.random() * 3) ** 2))

        age = next_date.year - year_immat
        dep = DEPRECIATION.get(min(age, 10), 0.20)

        # Price
        base_price = m["base_price_range"][0] + rng.random() * (m["base_price_range"][1] - m["base_price_range"][0])
        price = int(round(base_price * dep * (0.9 + rng.random() * 0.2), 0))

        # Condition
        if age <= 0:
            etat = "Neuf"
        elif age <= 2:
            etat = rng.choice(["Occasion - Excellent"] * 7 + ["Occasion - Bon"] * 3)
        elif age <= 5:
            etat = rng.choice(["Occasion - Excellent"] * 3 + ["Occasion - Bon"] * 4 + ["Occasion - Correct"] * 3)
        else:
            etat = rng.choice(["Occasion - Bon"] * 2 + ["Occasion - Correct"] * 8)

        nb_photos = max(1, min(25, int(8 + 5 * dep + rng.random() * 10)))
        km = 0 if etat == "Neuf" else max(1000, int(age * (12000 + rng.random() * 8000)))

        energy = "Électrique" if m["segment"].endswith("-EV") else rng.choice(
            ["Essence"] * 35 + ["Diesel"] * 25 + ["Hybride"] * 25 + ["GPL"] * 5 + ["Essence + GPL"] * 10
        )

        all_rows.append({
            "listing_id": listing_id,
            "sale_date": str(next_date),
            "concession_id": conc["concession_id"],
            "group_id": conc["group_id"],
            "model_id": m["model_id"],
            "brand": m["brand"],
            "model": m["model"],
            "version": m["version"],
            "segment": m["segment"],
            "year_immat": year_immat,
            "km": km,
            "energy": energy,
            "etat": etat,
            "price": price,
            "nb_photos": nb_photos,
        })

print(f"Generated {len(all_rows):,.0f} car listings across {len(concessions)} concessions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write to landing zone

# COMMAND ----------

date_str = str(next_date)
df_sales = spark.createDataFrame(all_rows)
df_sales.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv(f"{VOLUME_PATH}/sales/sale_date={date_str}")

print(f"\nFiles written: {VOLUME_PATH}/sales/sale_date={date_str}/")
print(f"Listings: {len(all_rows):,.0f}")
print(f"\nThe pipeline will pick up these files automatically on next trigger.")

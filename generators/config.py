# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Car Sales Demo — Shared Configuration
# MAGIC
# MAGIC Shared constants and generation functions for car sales data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog & Schema

# COMMAND ----------

try:
    dbutils.widgets.text("catalog", "renault_demo", "Catalog name")
    CATALOG = dbutils.widgets.get("catalog")
except Exception:
    CATALOG = "renault_demo"

SCHEMA_LANDING = "landing"
SCHEMA_CAR_SALES = "car_sales"

VOLUME_NAME = "car_sales_landing"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA_LANDING}/{VOLUME_NAME}"

# ─── Data generation parameters ───
NUM_CONCESSIONS = 50
NUM_CONCESSION_GROUPS = 8
NUM_MODELS = 30
DATE_START = "2024-01-01"
DATE_END = "2025-12-31"

# Sales per concession per day (used cars listed/sold)
SALES_MIN = 2
SALES_MAX = 15

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concession Groups (for RLS)

# COMMAND ----------

CONCESSION_GROUPS = [
    {"group_id": "GRP-01", "group_name": "Groupe Bernard", "regions": ["Auvergne-Rhône-Alpes", "Bourgogne-Franche-Comté"]},
    {"group_id": "GRP-02", "group_name": "Groupe Gueudet", "regions": ["Hauts-de-France", "Normandie"]},
    {"group_id": "GRP-03", "group_name": "Groupe Jean Lain", "regions": ["Auvergne-Rhône-Alpes"]},
    {"group_id": "GRP-04", "group_name": "Groupe Bodemer", "regions": ["Bretagne", "Pays de la Loire"]},
    {"group_id": "GRP-05", "group_name": "Groupe Dubreuil", "regions": ["Nouvelle-Aquitaine", "Pays de la Loire"]},
    {"group_id": "GRP-06", "group_name": "Groupe Mary", "regions": ["Normandie", "Île-de-France"]},
    {"group_id": "GRP-07", "group_name": "Groupe Parot", "regions": ["Nouvelle-Aquitaine", "Occitanie"]},
    {"group_id": "GRP-08", "group_name": "Groupe Claro", "regions": ["Provence-Alpes-Côte d'Azur", "Occitanie"]},
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Car Models (Renault + Dacia)

# COMMAND ----------

CAR_MODELS = [
    # Renault models
    {"model_id": "MOD-01", "brand": "Renault", "model": "Clio", "version": "V", "segment": "B", "base_price_range": (12000, 22000), "year_range": (2019, 2025)},
    {"model_id": "MOD-02", "brand": "Renault", "model": "Clio", "version": "IV", "segment": "B", "base_price_range": (6000, 14000), "year_range": (2012, 2019)},
    {"model_id": "MOD-03", "brand": "Renault", "model": "Captur", "version": "II", "segment": "B-SUV", "base_price_range": (15000, 28000), "year_range": (2019, 2025)},
    {"model_id": "MOD-04", "brand": "Renault", "model": "Captur", "version": "I", "segment": "B-SUV", "base_price_range": (8000, 16000), "year_range": (2013, 2019)},
    {"model_id": "MOD-05", "brand": "Renault", "model": "Mégane", "version": "IV", "segment": "C", "base_price_range": (12000, 25000), "year_range": (2016, 2023)},
    {"model_id": "MOD-06", "brand": "Renault", "model": "Mégane E-Tech", "version": "I", "segment": "C-EV", "base_price_range": (28000, 42000), "year_range": (2022, 2025)},
    {"model_id": "MOD-07", "brand": "Renault", "model": "Austral", "version": "I", "segment": "C-SUV", "base_price_range": (28000, 42000), "year_range": (2022, 2025)},
    {"model_id": "MOD-08", "brand": "Renault", "model": "Arkana", "version": "I", "segment": "C-SUV", "base_price_range": (25000, 38000), "year_range": (2021, 2025)},
    {"model_id": "MOD-09", "brand": "Renault", "model": "Scenic E-Tech", "version": "V", "segment": "C-EV", "base_price_range": (35000, 48000), "year_range": (2024, 2025)},
    {"model_id": "MOD-10", "brand": "Renault", "model": "Espace", "version": "VI", "segment": "D-SUV", "base_price_range": (38000, 52000), "year_range": (2023, 2025)},
    {"model_id": "MOD-11", "brand": "Renault", "model": "Kangoo", "version": "III", "segment": "Ludospace", "base_price_range": (20000, 32000), "year_range": (2021, 2025)},
    {"model_id": "MOD-12", "brand": "Renault", "model": "Twingo", "version": "III", "segment": "A", "base_price_range": (8000, 16000), "year_range": (2014, 2024)},
    {"model_id": "MOD-13", "brand": "Renault", "model": "Twingo E-Tech", "version": "III", "segment": "A-EV", "base_price_range": (18000, 26000), "year_range": (2020, 2024)},
    {"model_id": "MOD-14", "brand": "Renault", "model": "Zoe", "version": "II", "segment": "B-EV", "base_price_range": (15000, 28000), "year_range": (2019, 2024)},
    {"model_id": "MOD-15", "brand": "Renault", "model": "R5 E-Tech", "version": "I", "segment": "B-EV", "base_price_range": (25000, 35000), "year_range": (2024, 2025)},
    {"model_id": "MOD-16", "brand": "Renault", "model": "Trafic", "version": "III", "segment": "VU", "base_price_range": (25000, 42000), "year_range": (2014, 2025)},
    {"model_id": "MOD-17", "brand": "Renault", "model": "Master", "version": "III", "segment": "VU", "base_price_range": (30000, 50000), "year_range": (2014, 2025)},
    # Dacia models
    {"model_id": "MOD-18", "brand": "Dacia", "model": "Sandero", "version": "III", "segment": "B", "base_price_range": (9000, 16000), "year_range": (2020, 2025)},
    {"model_id": "MOD-19", "brand": "Dacia", "model": "Sandero", "version": "II", "segment": "B", "base_price_range": (5000, 11000), "year_range": (2016, 2020)},
    {"model_id": "MOD-20", "brand": "Dacia", "model": "Duster", "version": "III", "segment": "B-SUV", "base_price_range": (18000, 28000), "year_range": (2024, 2025)},
    {"model_id": "MOD-21", "brand": "Dacia", "model": "Duster", "version": "II", "segment": "B-SUV", "base_price_range": (12000, 22000), "year_range": (2018, 2024)},
    {"model_id": "MOD-22", "brand": "Dacia", "model": "Jogger", "version": "I", "segment": "C-MPV", "base_price_range": (15000, 24000), "year_range": (2022, 2025)},
    {"model_id": "MOD-23", "brand": "Dacia", "model": "Spring", "version": "I", "segment": "A-EV", "base_price_range": (12000, 20000), "year_range": (2021, 2025)},
    # Alpine
    {"model_id": "MOD-24", "brand": "Alpine", "model": "A110", "version": "II", "segment": "Sport", "base_price_range": (50000, 80000), "year_range": (2017, 2025)},
    {"model_id": "MOD-25", "brand": "Alpine", "model": "A290", "version": "I", "segment": "B-EV", "base_price_range": (35000, 45000), "year_range": (2025, 2025)},
    # Older Renault (occasion only)
    {"model_id": "MOD-26", "brand": "Renault", "model": "Scénic", "version": "IV", "segment": "C-MPV", "base_price_range": (10000, 22000), "year_range": (2016, 2022)},
    {"model_id": "MOD-27", "brand": "Renault", "model": "Kadjar", "version": "I", "segment": "C-SUV", "base_price_range": (10000, 22000), "year_range": (2015, 2022)},
    {"model_id": "MOD-28", "brand": "Renault", "model": "Talisman", "version": "I", "segment": "D", "base_price_range": (12000, 25000), "year_range": (2015, 2022)},
    {"model_id": "MOD-29", "brand": "Renault", "model": "Koleos", "version": "II", "segment": "D-SUV", "base_price_range": (15000, 30000), "year_range": (2017, 2023)},
    {"model_id": "MOD-30", "brand": "Renault", "model": "Clio", "version": "III", "segment": "B", "base_price_range": (3000, 8000), "year_range": (2005, 2012)},
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concessions (Dealerships)

# COMMAND ----------

GEO_CONCESSIONS = {
    "Île-de-France": ["Paris 17e", "Boulogne-Billancourt", "Saint-Denis", "Créteil", "Versailles", "Évry"],
    "Auvergne-Rhône-Alpes": ["Lyon Gerland", "Grenoble", "Saint-Étienne", "Annecy", "Clermont-Ferrand"],
    "Nouvelle-Aquitaine": ["Bordeaux Lac", "Bayonne", "La Rochelle", "Limoges", "Périgueux"],
    "Occitanie": ["Toulouse Balma", "Montpellier", "Nîmes", "Perpignan"],
    "Provence-Alpes-Côte d'Azur": ["Marseille La Valentine", "Nice", "Toulon", "Avignon"],
    "Grand Est": ["Strasbourg", "Mulhouse", "Metz", "Nancy"],
    "Hauts-de-France": ["Lille", "Amiens", "Beauvais", "Calais"],
    "Bretagne": ["Rennes", "Brest", "Lorient", "Saint-Brieuc"],
    "Pays de la Loire": ["Nantes", "Angers", "Le Mans"],
    "Normandie": ["Rouen", "Caen", "Le Havre"],
}

# Vehicle condition states
ETATS = ["Neuf", "Occasion - Excellent", "Occasion - Bon", "Occasion - Correct"]
ETAT_WEIGHTS = [0.25, 0.30, 0.30, 0.15]

# Depreciation by age (year offset from current)
DEPRECIATION = {
    0: 1.0,    # new / current year
    1: 0.80,
    2: 0.70,
    3: 0.60,
    4: 0.52,
    5: 0.45,
    6: 0.38,
    7: 0.32,
    8: 0.27,
    9: 0.23,
    10: 0.20,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fuel / Energy types

# COMMAND ----------

ENERGY_TYPES = {
    "default": [("Essence", 0.35), ("Diesel", 0.25), ("Hybride", 0.25), ("GPL", 0.05), ("Essence + GPL", 0.10)],
    "EV": [("Électrique", 1.0)],
}

def get_energy_type(segment, seed_val):
    """Return energy type based on segment."""
    import random
    rng = random.Random(seed_val)
    if segment.endswith("-EV"):
        return "Électrique"
    choices, weights = zip(*ENERGY_TYPES["default"])
    return rng.choices(choices, weights=weights)[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: Generate dimensions

# COMMAND ----------

import random
from datetime import date, timedelta

def generate_concessions(seed=42):
    """Generate list of concession dicts with group assignment."""
    rng = random.Random(seed)
    concessions = []
    idx = 0
    regions = list(GEO_CONCESSIONS.keys())

    for region, cities in GEO_CONCESSIONS.items():
        # Find which group covers this region
        matching_groups = [g for g in CONCESSION_GROUPS if region in g["regions"]]

        for city in cities:
            group = rng.choice(matching_groups) if matching_groups else rng.choice(CONCESSION_GROUPS)
            concessions.append({
                "concession_id": f"CON-{idx:03d}",
                "concession_name": f"Renault {city}",
                "city": city,
                "region": region,
                "country": "France",
                "group_id": group["group_id"],
                "group_name": group["group_name"],
                "size": rng.choice(["S", "M", "L"]),
            })
            idx += 1

    return concessions[:NUM_CONCESSIONS]


def generate_car_models():
    """Return the car models list as-is (already defined)."""
    return CAR_MODELS


def generate_concession_groups():
    """Return the concession groups list."""
    return CONCESSION_GROUPS


def generate_dim_date(start_date, end_date):
    """Generate list of date dimension dicts."""
    date_rows = []
    d = start_date
    while d <= end_date:
        date_rows.append({
            "date_key": str(d),
            "day_of_week": d.isoweekday(),
            "day_name": ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"][d.weekday()],
            "is_weekend": d.weekday() >= 5,
            "week_iso": d.isocalendar()[1],
            "month": d.month,
            "quarter": (d.month - 1) // 3 + 1,
            "year": d.year,
        })
        d += timedelta(days=1)
    return date_rows


def is_open_day(d):
    """Concessions are closed on Sundays."""
    return d.weekday() != 6


def get_seasonality(month):
    """Car sales seasonality — peaks in Jan (new year), Mar, Sep (rentrée), low in Aug."""
    mapping = {1: 1.3, 3: 1.2, 6: 1.1, 8: 0.5, 9: 1.25, 12: 0.9}
    return mapping.get(month, 1.0)


def get_dow_multiplier(day_of_week_iso):
    """Saturday is the big day for car shopping."""
    mapping = {6: 1.8, 5: 1.2, 1: 0.8}  # Sat, Fri, Mon
    return mapping.get(day_of_week_iso, 1.0)

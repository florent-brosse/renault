# Renault Car Sales Demo — Databricks Lakehouse

A complete car sales analytics demo showcasing how **1 Databricks workspace** replaces the complex GCP architecture (6 projects + NiFi + Cloud SQL + K8s).

## Architecture

```
  CSV Generators              Declarative Pipeline (DP)
 ┌─────────────────┐    ┌─────────────────────────────────────────────────────┐
 │ generate_history │───▶│  BRONZE            SILVER              GOLD        │
 │  2 yrs of CSVs  │    │  ┌──────────┐    ┌──────────────┐   ┌───────────┐ │
 │                  │    │  │Auto Loader│──▶ │Typed, cleaned│──▶│KPIs       │ │
 │ generate_daily   │───▶│  │cloudFiles │    │DQ validated  │   │Scorecards │ │
 │  1 new day CSVs  │    │  └──────────┘    └──────────────┘   │Model mix  │ │
 └─────────────────┘    └───────────────────────────────────┴───────────┘   │
                                                                 │          │
  UC Volume              ┌─── RLS by concession group ──────────┤          │
  (landing zone)         │                                       ▼          │
                         │   AI/BI Dashboard + Genie Space  ◀───Gold       │
                         │                                                  │
                         │   Lakebase (Postgres REST API)   ◀──────────────┘
                         │      ↕ sync back to Delta
                         │   Databricks App (ref tables CRUD)
                         │
                         └─── BQ Federation (future) ───────── BigQuery
```

## Features showcased

| Feature | Where |
|---------|-------|
| Unity Catalog Volumes | Landing zone for CSV files |
| Auto Loader (cloudFiles) | Bronze layer streaming ingestion |
| Declarative Pipelines (DP) | Full Bronze → Silver → Gold pipeline |
| Data Quality Expectations | Bronze + Silver layers |
| Streaming Tables | Bronze + Silver |
| Materialized Views | Gold layer with incremental maintenance |
| Liquid Clustering | Gold tables |
| Row-Level Security | Gold tables filtered by concession group |
| AI/BI Dashboard | Sales monitoring (no extra license, SSO) |
| AI/BI Genie | Natural language car sales analytics |
| Lakebase | Postgres REST API for Gold + CRUD ref tables |
| Lakebase ↔ Delta sync | Mirror tables both directions |
| Databricks App (optional) | UI for managing reference tables |
| BQ Federation (future) | Zero-copy query of BigQuery |
| Databricks Asset Bundles | Infrastructure-as-code deployment |

## Data model

### Dimensions

| Table | Rows | Key columns |
|-------|------|-------------|
| dim_concession | 50 | concession_id, concession_name, city, region, group_id, group_name |
| dim_concession_group | 8 | group_id, group_name, regions |
| dim_model | 30 | model_id, brand, model, version, segment |
| dim_date | 731 | date_key, day_name, is_weekend, month, quarter, year |

### Facts

| Table | Grain | Key columns |
|-------|-------|-------------|
| car_listings | concession × day × listing | listing_id, sale_date, concession_id, model_id, brand, model, version, year_immat, km, energy, etat, price, nb_photos |

### Gold layer

| Table | Grain | Purpose |
|-------|-------|---------|
| concession_daily_kpis | concession × day | Volume, revenue, avg price, energy mix |
| model_performance | model × month | Model rankings, price trends, EV adoption |
| group_scorecard | group × month | Group rankings by revenue, volume, EV share |
| listings_detail | listing | Enriched listing-level for AI/BI + Genie |

### RLS groups

| Group | Name | Regions |
|-------|------|---------|
| GRP-01 | Groupe Bernard | ARA, BFC |
| GRP-02 | Groupe Gueudet | HdF, Normandie |
| GRP-03 | Groupe Jean Lain | ARA |
| GRP-04 | Groupe Bodemer | Bretagne, PdL |
| GRP-05 | Groupe Dubreuil | NA, PdL |
| GRP-06 | Groupe Mary | Normandie, IdF |
| GRP-07 | Groupe Parot | NA, Occitanie |
| GRP-08 | Groupe Claro | PACA, Occitanie |

## Quick start

### Deploy with Databricks Asset Bundles

```bash
databricks bundle validate
databricks bundle deploy

# Initial setup: generate history + run pipeline + RLS + Lakebase
databricks bundle run renault_setup

# During demo: generate one new day
databricks bundle run renault_daily
```

## Project structure

```
renault/
├── databricks.yml                     # DAB configuration
├── generators/
│   ├── config.py                      # Shared constants & generation functions
│   ├── generate_history.py            # Batch: 2 years of CSV files
│   └── generate_daily.py             # Incremental: 1 new day of CSVs
├── pipeline/
│   └── car_sales_pipeline.py         # Declarative Pipeline (Bronze + Silver + Gold)
├── rls/
│   └── setup_rls.py                  # Row-Level Security by concession group
├── lakebase/
│   └── setup_lakebase.py            # Lakebase setup: mirrors + ref tables + sync
├── dashboards/                        # AI/BI Lakeview dashboard queries
├── genie/                            # Genie space provisioning
└── app/                              # Optional Databricks App for ref table CRUD
```

## Synthetic data details

- **50 concessions** across 10 French régions, assigned to 8 dealership groups
- **30 car models**: Renault (17), Dacia (6), Alpine (2), older models (5)
- **Segments**: A, B, C, D, SUV, EV, VU, Sport, Ludospace
- **Seasonality**: January +30% (new year), September +25% (rentrée), August -50%
- **Day-of-week**: Saturday +80%, Friday +20%, Monday -20%, closed Sundays
- **Pricing**: Realistic depreciation curve (80% at 1yr, down to 20% at 10yr+)
- **Vehicle condition**: Correlated to age (newer = better condition)
- **Energy mix**: Essence 35%, Diesel 25%, Hybride 25%, GPL 5%, EV for electric models

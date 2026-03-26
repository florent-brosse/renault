# Renault Car Sales Demo — Databricks Lakehouse

A complete car sales analytics demo showcasing how **1 Databricks workspace** replaces the complex GCP architecture (6 projects + NiFi + Cloud SQL + K8s).

## Architecture

```
  CSV Generators              Declarative Pipeline (DP)
 ┌─────────────────┐    ┌─────────────────────────────────────────────────────┐
 │ generate_history │───▶│  BRONZE            SILVER              GOLD (MVs) │
 │  3 months CSVs   │    │  ┌──────────┐    ┌──────────────┐   ┌───────────┐ │
 │                  │    │  │Auto Loader│──▶ │Typed, cleaned│──▶│KPIs       │ │
 │ generate_daily   │───▶│  │cloudFiles │    │DQ validated  │   │Scorecards │ │
 │  1 new day CSVs  │    │  └──────────┘    └──────────────┘   │Model mix  │ │
 └─────────────────┘    └───────────────────────────────────┴───────────┘   │
                                                                 │          │
  UC Volume                                                      ▼          │
  (landing zone)      ┌── Dynamic Views (RLS) ──▶ Dashboards / Genie / DBSQL│
                      │   v_listings_detail                                  │
                      │   v_concession_daily_kpis                            │
                      │   v_group_scorecard                                  │
                      │                                                      │
                      │── Synced Tables ──▶ Lakebase Postgres ──▶ Data API  │
                      │   (no RLS on MVs)    (Postgres views for RLS)       │
                      │                                                      │
                      └── BQ Federation (future) ────────────── BigQuery    │
```

### RLS Architecture (two layers)

Gold MVs can't have both RLS and synced tables simultaneously. Workaround:

```
Gold MVs (no RLS) ──→ Synced tables → Postgres → Data API (Postgres views for RLS)
       │
       └──→ Dynamic views (WHERE rls_filter()) → Dashboards / Genie / DBSQL
```

- **DBSQL / Dashboards / Genie**: query `v_*` dynamic views with `WHERE rls_filter(group_id)`
- **Data API (PostgREST)**: query Postgres views filtered by SP identity
- **Gold MVs**: stay clean (no row filters) so synced tables work

## Features showcased

| Feature | Where |
|---------|-------|
| Unity Catalog Volumes | Landing zone for CSV files |
| Auto Loader (cloudFiles) | Bronze layer streaming ingestion |
| Declarative Pipelines (DP) | Full Bronze → Silver → Gold pipeline |
| Data Quality Expectations | Bronze + Silver layers |
| Streaming Tables | Bronze + Silver |
| Materialized Views | Gold layer with incremental maintenance |
| Dynamic Views (RLS) | Row-level security by concession group |
| AI/BI Dashboard | Sales monitoring (no extra license, SSO) |
| AI/BI Genie | Natural language car sales analytics |
| Lakebase (synced tables) | Delta → Postgres automatic sync |
| Lakebase (Data API) | PostgREST REST API with Postgres RLS |
| Cost tagging | All resources tagged `project=renault-demo` |
| Databricks Asset Bundles | Infrastructure-as-code deployment |

## Data model

### Dimensions

| Table | Rows | Key columns |
|-------|------|-------------|
| dim_concession | ~42 | concession_id, concession_name, city, region, group_id |
| dim_concession_group | 8 | group_id, group_name, regions |
| dim_model | 30 | model_id, brand, model, version, segment |
| dim_date | ~92 | date_key, day_name, is_weekend, month, quarter, year |

### Gold layer (materialized views)

| MV | Grain | Purpose |
|----|-------|---------|
| concession_daily_kpis | concession × day | Volume, revenue, avg price, energy mix |
| model_performance | model × month | Model rankings, price trends, EV adoption |
| group_scorecard | group × month | Group rankings by revenue, volume, EV share |
| listings_detail | listing | Enriched listing-level for AI/BI + Genie |

### Dynamic views (RLS)

| View | Source MV | RLS |
|------|-----------|-----|
| v_concession_daily_kpis | concession_daily_kpis | `WHERE rls_filter(group_id)` |
| v_group_scorecard | group_scorecard | `WHERE rls_filter(group_id)` |
| v_listings_detail | listings_detail | `WHERE rls_filter(group_id)` |
| v_model_performance | model_performance | pass-through (no group_id) |

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

### Prerequisites

1. **Create a budget policy** in the workspace UI (Settings → Compute → Budget Policies)
   - Add a custom tag: `project = <your-policy-name>`
   - Copy the policy ID (UUID from the URL)
2. **Set the policy ID** in `databricks.yml`:
   ```yaml
   variables:
     budget_policy_id:
       default: <your-policy-id>
   ```

### Deploy with Databricks Asset Bundles

```bash
databricks bundle validate
databricks bundle deploy

# Initial setup: generate history + pipeline + RLS + Lakebase + Genie + Dashboard
databricks bundle run renault_setup

# During demo: generate one new day
databricks bundle run renault_daily
```

### Manual steps (one-time, after first deploy)

1. **Enable Data API**: Lakebase UI → select `renault-lakebase` → Data API → Enable
2. **Expose schema**: Data API → Advanced settings → add `car_sales`

> Data API enablement via REST API coming ~April 2026 (`PATCH /api/2.0/postgres/.../data-api`)

### Job DAG

```
generate_history → run_pipeline → setup_rls → create_genie_space
                                            → deploy_dashboard
                                → setup_lakebase (parallel, independent)
```

## Project structure

```
renault/
├── databricks.yml                     # DAB: pipeline, jobs, warehouse, Lakebase project
├── generators/
│   ├── config.py                      # Shared constants & generation functions
│   ├── generate_history.py            # Batch: 3 months of CSV files
│   └── generate_daily.py             # Incremental: 1 new day of CSVs
├── pipeline/
│   └── car_sales_pipeline.py         # Declarative Pipeline (Bronze + Silver + Gold)
├── rls/
│   └── setup_rls.py                  # Dynamic views with RLS + verification tests
├── lakebase/
│   └── setup_lakebase.py            # Synced tables + SPs + Postgres views + Data API demo
├── dashboards/
│   ├── renault_car_sales_dashboard.json  # AI/BI Lakeview dashboard (4 pages, French)
│   └── deploy_dashboard.py              # Deploy/update dashboard via API
├── genie/
│   └── create_genie_space.py        # Genie space with French instructions
└── cost/
    └── check_cost.py                # Cost analysis via system.billing.usage
```

## Teardown (delete everything)

`databricks bundle destroy` removes DAB-managed resources but NOT synced table pipelines, service principals, Genie spaces, or dashboards. Full cleanup:

```bash
# 1. Destroy DAB resources (jobs, pipeline, warehouse, Lakebase project, workspace files)
databricks bundle destroy --auto-approve

# 2. Delete Genie spaces and dashboards
databricks api get "/api/2.0/genie/spaces" \
  | python3 -c "import sys,json; [print(s['space_id']) for s in json.load(sys.stdin).get('spaces',[]) if 'Renault' in s.get('title','')]" \
  | while read sid; do databricks api delete "/api/2.0/genie/spaces/$sid"; done

databricks api get "/api/2.0/lakeview/dashboards" \
  | python3 -c "import sys,json; [print(d['dashboard_id']) for d in json.load(sys.stdin).get('dashboards',[]) if 'Renault' in d.get('display_name','')]" \
  | while read did; do databricks api delete "/api/2.0/lakeview/dashboards/$did"; done

# 3. Delete synced table pipelines (created by Lakebase setup, not DAB-managed)
databricks api get "/api/2.0/pipelines?filter=name+LIKE+'%25synced%25car_sales%25'&max_results=10" \
  | python3 -c "import sys,json; [print(p['pipeline_id']) for p in json.load(sys.stdin).get('statuses',[])]" \
  | while read pid; do databricks api delete "/api/2.0/pipelines/$pid"; done

# 4. Delete service principals
for sp_name in "renault-groupe-bernard" "renault-groupe-gueudet"; do
  sp_id=$(databricks service-principals list | grep "$sp_name" | awk '{print $1}')
  [ -n "$sp_id" ] && databricks service-principals delete "$sp_id"
done

# 5. Delete secret scope
databricks api post /api/2.0/secrets/scopes/delete --json '{"scope": "renault-demo"}'

# 6. Drop UC schemas (if not already dropped by pipeline destroy)
databricks api post /api/2.0/sql/statements --json '{
  "warehouse_id": "<warehouse_id>",
  "statement": "DROP SCHEMA IF EXISTS <catalog>.car_sales CASCADE",
  "wait_timeout": "50s"
}'
databricks api post /api/2.0/sql/statements --json '{
  "warehouse_id": "<warehouse_id>",
  "statement": "DROP SCHEMA IF EXISTS <catalog>.landing CASCADE",
  "wait_timeout": "50s"
}'
```

## Cost tracking

All resources are tagged with `project=renault-demo` and `customer=renault`.

```sql
-- Total cost (jobs + warehouse via tags, pipeline via ID)
SELECT sku_name, ROUND(SUM(usage_quantity), 2) AS dbus
FROM system.billing.usage
WHERE custom_tags['project'] = 'renault-demo'
   OR usage_metadata.dlt_pipeline_id IN (
     SELECT pipeline_id FROM system.lakeflow.pipelines WHERE name LIKE '%Renault%'
   )
GROUP BY sku_name ORDER BY dbus DESC
```

See `cost/check_cost.py` for detailed breakdown.

## Synthetic data details

- **~42 concessions** across 10 French regions, assigned to 8 dealership groups
- **30 car models**: Renault (17), Dacia (6), Alpine (2), older models (5)
- **Segments**: A, B, C, D, SUV, EV, VU, Sport, Ludospace
- **Seasonality**: January +30% (new year), September +25% (rentrée), August -50%
- **Day-of-week**: Saturday +80%, Friday +20%, Monday -20%, closed Sundays
- **Pricing**: Realistic depreciation curve (80% at 1yr, down to 20% at 10yr+)
- **Vehicle condition**: Correlated to age (newer = better condition)
- **Energy mix**: Essence 35%, Diesel 25%, Hybride 25%, GPL 5%, EV for electric models

## Known limitations

| Limitation | Workaround |
|------------|------------|
| RLS + synced tables can't coexist on same MV | Dynamic views for DBSQL/BI, Postgres views for Data API |
| Data API enablement is UI-only | API coming ~April 2026 |
| `bundle destroy` doesn't clean synced pipelines/SPs/Genie | See teardown section |
| ABAC `CREATE POLICY` also blocks synced tables | Use dynamic views instead |
| Governed tags creation is UI or API, not SQL | `POST /api/2.0/tag-policies` |

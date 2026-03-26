# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Lakebase Setup (Synced Tables + Data API)
# MAGIC
# MAGIC Fully automated — works on a fresh workspace.
# MAGIC
# MAGIC 1. Enables CDF on Gold tables
# MAGIC 2. Creates synced tables (Delta Gold → Lakebase Postgres)
# MAGIC 3. Creates 2 service principals (one per concession group)
# MAGIC 4. Sets up Postgres roles for Data API access
# MAGIC 5. Creates **Postgres views with RLS** (workaround: synced tables are owned by pipeline, can't ALTER)
# MAGIC 6. Demos the Data API: each SP sees only their group's data
# MAGIC
# MAGIC **Manual steps** (one-time):
# MAGIC - Enable Data API: Lakebase UI → Data API → Enable
# MAGIC - Expose `car_sales` schema: Data API → Advanced settings → Exposed schemas
# MAGIC
# MAGIC ### RLS Architecture
# MAGIC ```
# MAGIC UC Row Filters ──── Spark SQL / DBSQL / Dashboards / Genie
# MAGIC                     (ALTER TABLE SET ROW FILTER)
# MAGIC
# MAGIC Postgres Views ──── Data API / PostgREST / External Apps
# MAGIC with RLS            (CREATE VIEW + GRANT per SP)
# MAGIC ```
# MAGIC **Why two layers?** Synced tables are owned by the sync pipeline — we can't
# MAGIC `ALTER TABLE ENABLE ROW LEVEL SECURITY` on them. The workaround is to create
# MAGIC Postgres views that filter by `current_user` and grant per-SP access.

# COMMAND ----------

# MAGIC %run ../generators/config

# COMMAND ----------

import json, time, requests, psycopg2
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import SyncedTableSpec, SyncedTableSchedulingPolicy

w = WorkspaceClient()
workspace_url = w.config.host.rstrip("/")
workspace_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceId().get()

LAKEBASE_PROJECT = "renault-lakebase"
dbutils.widgets.text("budget_policy_id", "", "Budget Policy ID")
BUDGET_POLICY_ID = dbutils.widgets.get("budget_policy_id")
assert BUDGET_POLICY_ID, "budget_policy_id parameter is required"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Enable CDF on Gold tables

# COMMAND ----------

# Gold tables are materialized views — CDF can't be enabled on them.
# Synced tables use SNAPSHOT mode which doesn't require CDF.
# CDF is only needed for TRIGGERED/CONTINUOUS sync modes.
gold_tables = ["listings_detail", "concession_daily_kpis", "model_performance", "group_scorecard"]
print("Skipping CDF — Gold tables are materialized views, using SNAPSHOT sync mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Get Lakebase project IDs

# COMMAND ----------

projects = list(w.postgres.list_projects())
project_uid = None
for p in projects:
    if LAKEBASE_PROJECT in p.name:
        project_uid = p.uid
        break

assert project_uid, f"Lakebase project '{LAKEBASE_PROJECT}' not found. Run 'databricks bundle deploy' first."

# Tag the Lakebase project for cost tracking
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
requests.patch(
    f"{workspace_url}/api/2.0/postgres/projects/{LAKEBASE_PROJECT}",
    headers={"Authorization": f"Bearer {token}"},
    params={"update_mask": "spec.custom_tags"},
    json={"spec": {"custom_tags": [
        {"key": "project", "value": "renault-demo"},
        {"key": "customer", "value": "renault"}
    ]}}
).raise_for_status()
print("Lakebase project tagged: project=renault-demo, customer=renault")

# Wait for branch to be READY
print("Waiting for Lakebase branch to be READY...")
for attempt in range(30):
    branches = list(w.postgres.list_branches(parent=f"projects/{LAKEBASE_PROJECT}"))
    if branches and branches[0].status.current_state.value == "READY":
        print(f"  Branch ready: {branches[0].uid}")
        break
    time.sleep(10)
else:
    raise Exception("Lakebase branch not ready after 5 min")

branch = branches[0]
branch_uid = branch.uid
branch_name = branch.name.split("/")[-1]

endpoints = list(w.postgres.list_endpoints(parent=f"projects/{LAKEBASE_PROJECT}/branches/{branch_name}"))
endpoint_host = endpoints[0].status.hosts.host
endpoint_name = endpoints[0].name.split("/")[-1]

print(f"Project UID: {project_uid}")
print(f"Branch: {branch_uid} ({branch_name})")
print(f"Endpoint: {endpoint_host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create synced tables (Delta → Lakebase)

# COMMAND ----------

SYNC_CONFIG = [
    {"table": "listings_detail", "pk": ["listing_id"]},
    {"table": "concession_daily_kpis", "pk": ["concession_id", "sale_date"]},
    {"table": "model_performance", "pk": ["brand", "model", "version", "year", "month"]},
    {"table": "group_scorecard", "pk": ["group_id", "year", "month"]},
]

for cfg in SYNC_CONFIG:
    source = f"{CATALOG}.car_sales.{cfg['table']}"
    dest = f"{CATALOG}.car_sales.{cfg['table']}_synced"

    # Skip if already exists
    try:
        existing = w.database.get_synced_database_table(name=dest)
        state = existing.data_synchronization_status.detailed_state.value if existing.data_synchronization_status else "?"
        print(f"Already exists: {dest} ({state})")
        continue
    except Exception:
        pass

    try:
        w.database.create_synced_database_table(
            name=dest,
            database_project_id=project_uid,
            database_branch_id=branch_uid,
            logical_database_name="databricks_postgres",
            spec=SyncedTableSpec(
                source_table_full_name=source,
                primary_key_columns=cfg["pk"],
                scheduling_policy=SyncedTableSchedulingPolicy.SNAPSHOT,
            ),
        )
        print(f"Created: {dest}")
    except Exception as e:
        if "409" in str(e) or "already exists" in str(e).lower():
            print(f"Already registered: {dest} (conflict)")
        else:
            print(f"ERROR creating {dest}: {e}")
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Wait for sync to complete

# COMMAND ----------

DEMO_TAG = "renault-demo"

# Tag the sync pipelines for cost tracking + budget policy
print("Tagging sync pipelines...\n")
for cfg in SYNC_CONFIG:
    dest = f"{CATALOG}.car_sales.{cfg['table']}_synced"
    table = w.database.get_synced_database_table(name=dest)
    pipeline_id = table.data_synchronization_status.pipeline_id
    if pipeline_id:
        pipe = w.pipelines.get(pipeline_id=pipeline_id)
        spec = pipe.spec
        current_tags = dict(spec.tags) if spec.tags else {}
        current_tags["project"] = DEMO_TAG
        current_tags["customer"] = "renault"
        # PUT replaces full spec — must send existing spec with updated fields
        spec.tags = current_tags
        spec.budget_policy_id = BUDGET_POLICY_ID
        w.pipelines.update(pipeline_id=pipeline_id, **spec.as_dict())
        print(f"  Tagged pipeline {pipeline_id} for {cfg['table']}_synced")

print("\nWaiting for synced tables to come online...\n")
for cfg in SYNC_CONFIG:
    dest = f"{CATALOG}.car_sales.{cfg['table']}_synced"
    for attempt in range(60):
        table = w.database.get_synced_database_table(name=dest)
        state = table.data_synchronization_status.detailed_state.value if table.data_synchronization_status else "UNKNOWN"
        if "ONLINE" in state:
            print(f"  {cfg['table']}_synced: {state}")
            break
        elif "FAILED" in state:
            raise Exception(f"Sync failed for {cfg['table']}_synced: {state}")
        time.sleep(5)
    else:
        raise Exception(f"Sync timeout for {cfg['table']}_synced (last state: {state})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create 2 Service Principals (one per concession group)

# COMMAND ----------

SP_CONFIGS = [
    {"name": "renault-groupe-bernard", "group_id": "GRP-01", "label": "Groupe Bernard"},
    {"name": "renault-groupe-gueudet", "group_id": "GRP-02", "label": "Groupe Gueudet"},
]

# Create secret scope if needed
scopes = [s.name for s in w.secrets.list_scopes()]
if "renault-demo" not in scopes:
    w.secrets.create_scope(scope="renault-demo")

sp_credentials = []

for sp_cfg in SP_CONFIGS:
    # Check if SP already exists
    existing_sp = None
    for sp in w.service_principals.list():
        if sp.display_name == sp_cfg["name"]:
            existing_sp = sp
            break

    if existing_sp:
        app_id = existing_sp.application_id
        sp_num_id = existing_sp.id
        print(f"SP '{sp_cfg['name']}' exists: {app_id}")
        # Check if secret is stored
        stored_keys = [s.key for s in w.secrets.list_secrets(scope="renault-demo")]
        if f"{sp_cfg['name']}-secret" in stored_keys:
            secret = dbutils.secrets.get("renault-demo", f"{sp_cfg['name']}-secret")
            sp_credentials.append({"app_id": app_id, "secret": secret, **sp_cfg})
            print(f"  Using stored secret")
            continue
        print(f"  No stored secret — creating new one")
    else:
        new_sp = w.service_principals.create(display_name=sp_cfg["name"], active=True)
        app_id = new_sp.application_id
        sp_num_id = new_sp.id
        print(f"Created SP '{sp_cfg['name']}': {app_id}")

    # Create OAuth secret (no SDK method — use API)
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    secret_resp = requests.post(
        f"{workspace_url}/api/2.0/accounts/servicePrincipals/{sp_num_id}/credentials/secrets",
        headers={"Authorization": f"Bearer {token}"}
    )
    secret_resp.raise_for_status()
    sp_secret = secret_resp.json()["secret"]

    # Store in secrets
    w.secrets.put_secret(scope="renault-demo", key=f"{sp_cfg['name']}-id", string_value=app_id)
    w.secrets.put_secret(scope="renault-demo", key=f"{sp_cfg['name']}-secret", string_value=sp_secret)

    # Grant CAN_USE on Lakebase project
    w.permissions.update(
        request_object_type="database-projects",
        request_object_id=project_uid,
        access_control_list=[{"service_principal_name": app_id, "permission_level": "CAN_USE"}]
    )

    sp_credentials.append({"app_id": app_id, "secret": sp_secret, **sp_cfg})
    print(f"  Secret stored, project access granted")

print(f"\n{len(sp_credentials)} SPs ready")

# Also insert SPs into the UC user_group_mapping table (used by dynamic views for RLS)
# This table is created by setup_rls, but we create it here if it doesn't exist
# so the same SPs are used for both UC RLS and Postgres RLS
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.car_sales.user_group_mapping (
  user_or_group STRING,
  group_id STRING,
  group_name STRING,
  is_admin BOOLEAN
) USING DELTA
""")

for sp in sp_credentials:
    spark.sql(f"""
    MERGE INTO {CATALOG}.car_sales.user_group_mapping t
    USING (SELECT '{sp["app_id"]}' AS user_or_group, '{sp["group_id"]}' AS group_id, '{sp["label"]}' AS group_name, FALSE AS is_admin) s
    ON t.user_or_group = s.user_or_group
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
print(f"SPs added to UC user_group_mapping (for dynamic views RLS)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Set up Postgres roles + RLS views
# MAGIC
# MAGIC **Why views?** Synced tables are owned by `databricks_writer_*` (the sync pipeline).
# MAGIC We can't `ALTER TABLE ENABLE ROW LEVEL SECURITY` on them.
# MAGIC
# MAGIC **Workaround:** Create views that filter by a mapping of `current_user → group_id`.
# MAGIC Each SP is granted SELECT only on the views, not on the underlying synced tables.

# COMMAND ----------

# Generate Postgres credential for Autoscaling project
pg_cred = w.postgres.generate_database_credential(
    endpoint=f"projects/{LAKEBASE_PROJECT}/branches/{branch_name}/endpoints/{endpoint_name}"
)
email = w.current_user.me().user_name

conn = psycopg2.connect(
    host=endpoint_host, port=5432, dbname="databricks_postgres",
    user=email, password=pg_cred.token, sslmode="require"
)
conn.autocommit = True
cur = conn.cursor()

cur.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth")

# Create Postgres roles for each SP
for sp in sp_credentials:
    cur.execute(f"""
        DO $$ BEGIN
            PERFORM databricks_create_role('{sp["app_id"]}', 'SERVICE_PRINCIPAL');
        EXCEPTION WHEN OTHERS THEN
            IF SQLERRM LIKE '%%already exists%%' THEN NULL;
            ELSE RAISE;
            END IF;
        END $$
    """)
    cur.execute(f'GRANT "{sp["app_id"]}" TO authenticator')
    print(f"Postgres role ready: {sp['name']} ({sp['app_id'][:12]}...)")

# Create group mapping table (SP app_id → group_id)
cur.execute("DROP TABLE IF EXISTS car_sales.sp_group_mapping CASCADE")
cur.execute("""
    CREATE TABLE car_sales.sp_group_mapping (
        sp_role TEXT PRIMARY KEY,
        group_id TEXT NOT NULL
    )
""")
for sp in sp_credentials:
    cur.execute(f"INSERT INTO car_sales.sp_group_mapping VALUES ('{sp['app_id']}', '{sp['group_id']}')")
print(f"Mapping table: {len(sp_credentials)} entries")

# Create filtered views per table (RLS via current_user lookup)
TABLES_WITH_GROUP_ID = ["listings_detail", "concession_daily_kpis", "group_scorecard"]

for table in TABLES_WITH_GROUP_ID:
    view_name = f"car_sales.v_{table}"
    cur.execute(f"DROP VIEW IF EXISTS {view_name}")
    cur.execute(f"""
        CREATE VIEW {view_name} AS
        SELECT t.*
        FROM car_sales.{table}_synced t
        JOIN car_sales.sp_group_mapping m ON m.group_id = t.group_id
        WHERE m.sp_role = current_user
    """)
    # Grant view access to SPs, revoke direct table access
    for sp in sp_credentials:
        cur.execute(f'GRANT SELECT ON {view_name} TO "{sp["app_id"]}"')
        cur.execute(f'REVOKE SELECT ON car_sales.{table}_synced FROM "{sp["app_id"]}"')
    print(f"View created: {view_name}")

# model_performance has no group_id — grant direct access (no RLS needed)
for sp in sp_credentials:
    cur.execute(f'GRANT SELECT ON car_sales.model_performance_synced TO "{sp["app_id"]}"')
print("model_performance_synced: direct access (no group_id)")

# Grant schema usage
for sp in sp_credentials:
    cur.execute(f'GRANT USAGE ON SCHEMA car_sales TO "{sp["app_id"]}"')

cur.execute("NOTIFY pgrst, 'reload schema'")
conn.close()
print("\nPostgres roles + RLS views configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Demo: Data API with RLS
# MAGIC
# MAGIC Each SP queries the same view endpoint.
# MAGIC **Different results** — each sees only their concession group's data.

# COMMAND ----------

DATA_API_BASE = f"https://{endpoint_host}/api/2.0/workspace/{workspace_id}/rest/databricks_postgres/car_sales"

for sp in sp_credentials:
    token_resp = requests.post(
        f"{workspace_url}/oidc/v1/token",
        data={
            "grant_type": "client_credentials",
            "client_id": sp["app_id"],
            "client_secret": sp["secret"],
            "scope": "all-apis",
        },
    )
    token_resp.raise_for_status()
    sp_token = token_resp.json()["access_token"]

    print(f"\n{'='*60}")
    print(f"  {sp['label']} ({sp['name']})")
    print(f"  Sees: {sp['group_id']} only")
    print(f"{'='*60}")

    # Query filtered view
    resp = requests.get(
        f"{DATA_API_BASE}/v_listings_detail",
        headers={"Authorization": f"Bearer {sp_token}"},
        params={"select": "listing_id,brand,model,price,etat,concession_name,group_id", "limit": "5"}
    )
    if resp.status_code == 200:
        data = resp.json()
        groups_seen = set(r.get("group_id") for r in data)
        print(f"\n  Rows returned: {len(data)} (groups visible: {groups_seen})")
        if data:
            display(spark.createDataFrame(pd.DataFrame(data)))
    else:
        print(f"  Error: {resp.status_code} — {resp.text[:200]}")

    # Count total visible via view
    resp = requests.get(
        f"{DATA_API_BASE}/v_listings_detail",
        headers={"Authorization": f"Bearer {sp_token}", "Prefer": "count=exact", "Range-Unit": "items", "Range": "0-0"}
    )
    total = resp.headers.get("Content-Range", "?")
    print(f"  Total visible: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Summary

# COMMAND ----------

print("=" * 60)
print("  LAKEBASE + DATA API + RLS COMPLETE")
print("=" * 60)
print(f"\n  Synced tables (Delta → Postgres, all data):")
for cfg in SYNC_CONFIG:
    print(f"    car_sales.{cfg['table']}_synced")
print(f"\n  Filtered views (RLS via current_user → group_id mapping):")
for t in TABLES_WITH_GROUP_ID:
    print(f"    car_sales.v_{t}")
print(f"\n  Service Principals:")
for sp in sp_credentials:
    print(f"    {sp['name']} → {sp['group_id']} ({sp['label']})")
print(f"\n  Data API: {DATA_API_BASE}")
print(f"\n  Manual steps (one-time):")
print(f"    1. Enable Data API: Lakebase UI → Data API → Enable")
print(f"    2. Expose 'car_sales' schema: Data API → Advanced settings")

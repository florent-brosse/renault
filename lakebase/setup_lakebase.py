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

# MAGIC %pip install --upgrade databricks-sdk psycopg2-binary --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../generators/config

# COMMAND ----------

import json, time, requests, psycopg2
import pandas as pd
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
workspace_url = w.config.host.rstrip("/")
workspace_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceId().get()
workspace_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {workspace_token}"}

LAKEBASE_PROJECT = "renault-lakebase"

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

projects_resp = requests.get(f"{workspace_url}/api/2.0/postgres/projects", headers=headers)
projects_resp.raise_for_status()
projects = projects_resp.json() if isinstance(projects_resp.json(), list) else projects_resp.json().get("projects", [])

project_uid = None
for p in projects:
    if LAKEBASE_PROJECT in p.get("name", ""):
        project_uid = p["uid"]
        break

assert project_uid, f"Lakebase project '{LAKEBASE_PROJECT}' not found. Run 'databricks bundle deploy' first."

# Wait for branch to be READY
print("Waiting for Lakebase branch to be READY...")
for attempt in range(30):
    branches_resp = requests.get(f"{workspace_url}/api/2.0/postgres/projects/{LAKEBASE_PROJECT}/branches", headers=headers)
    branches_resp.raise_for_status()
    branches = branches_resp.json() if isinstance(branches_resp.json(), list) else branches_resp.json().get("branches", [])
    if branches and branches[0].get("status", {}).get("current_state") == "READY":
        print(f"  Branch ready: {branches[0]['uid']}")
        break
    time.sleep(10)
else:
    raise Exception(f"Lakebase branch not ready after 5 min")

branches_resp = requests.get(f"{workspace_url}/api/2.0/postgres/projects/{LAKEBASE_PROJECT}/branches", headers=headers)
branches_resp.raise_for_status()
branches = branches_resp.json() if isinstance(branches_resp.json(), list) else branches_resp.json().get("branches", [])
branch_uid = branches[0]["uid"]
branch_name = branches[0]["name"].split("/")[-1]

endpoints_resp = requests.get(f"{workspace_url}/api/2.0/postgres/projects/{LAKEBASE_PROJECT}/branches/{branch_name}/endpoints", headers=headers)
endpoints_resp.raise_for_status()
endpoints_data = endpoints_resp.json() if isinstance(endpoints_resp.json(), list) else endpoints_resp.json().get("endpoints", [])
endpoint_host = endpoints_data[0]["status"]["hosts"]["host"]
endpoint_name = endpoints_data[0]["name"].split("/")[-1]

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
    check = requests.get(f"{workspace_url}/api/2.0/database/synced_tables/{dest}", headers=headers)
    if check.status_code == 200:
        state = check.json().get("data_synchronization_status", {}).get("detailed_state", "?")
        print(f"Already exists: {dest} ({state})")
        continue

    resp = requests.post(
        f"{workspace_url}/api/2.0/database/synced_tables",
        headers=headers,
        json={
            "name": dest,
            "database_project_id": project_uid,
            "database_branch_id": branch_uid,
            "logical_database_name": "databricks_postgres",
            "spec": {
                "source_table_full_name": source,
                "primary_key_columns": cfg["pk"],
                "scheduling_policy": "SNAPSHOT",
            },
        },
    )
    if resp.status_code == 409:
        print(f"Already registered: {dest} (409 conflict)")
    else:
        resp.raise_for_status()
        print(f"Created: {dest}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Wait for sync to complete

# COMMAND ----------

DEMO_TAG = "renault-demo"

# Tag the sync pipelines for cost tracking
print("Tagging sync pipelines...\n")
for cfg in SYNC_CONFIG:
    dest = f"{CATALOG}.car_sales.{cfg['table']}_synced"
    resp = requests.get(f"{workspace_url}/api/2.0/database/synced_tables/{dest}", headers=headers)
    if resp.status_code == 200:
        pipeline_id = resp.json().get("data_synchronization_status", {}).get("pipeline_id")
        if pipeline_id:
            # Get current pipeline spec and add tags
            pipe_resp = requests.get(f"{workspace_url}/api/2.0/pipelines/{pipeline_id}", headers=headers)
            if pipe_resp.status_code == 200:
                pipe_spec = pipe_resp.json().get("spec", {})
                current_tags = pipe_spec.get("tags", {})
                current_tags["project"] = DEMO_TAG
                current_tags["customer"] = "renault"
                requests.put(
                    f"{workspace_url}/api/2.0/pipelines/{pipeline_id}",
                    headers=headers,
                    json={**pipe_spec, "tags": current_tags, "id": pipeline_id}
                )
                print(f"  Tagged pipeline {pipeline_id} for {cfg['table']}_synced")

print("\nWaiting for synced tables to come online...\n")
for cfg in SYNC_CONFIG:
    dest = f"{CATALOG}.car_sales.{cfg['table']}_synced"
    for attempt in range(60):
        resp = requests.get(f"{workspace_url}/api/2.0/database/synced_tables/{dest}", headers=headers)
        state = resp.json().get("data_synchronization_status", {}).get("detailed_state", "UNKNOWN")
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
scopes = [s.name for s in dbutils.secrets.listScopes()]
if "renault-demo" not in scopes:
    requests.post(f"{workspace_url}/api/2.0/secrets/scopes/create", headers=headers,
                  json={"scope": "renault-demo"}).raise_for_status()

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
        stored_keys = [s.key for s in dbutils.secrets.list("renault-demo")]
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

    # Create OAuth secret
    secret_resp = requests.post(
        f"{workspace_url}/api/2.0/accounts/servicePrincipals/{sp_num_id}/credentials/secrets",
        headers=headers
    )
    secret_resp.raise_for_status()
    sp_secret = secret_resp.json()["secret"]

    # Store in secrets
    requests.post(f"{workspace_url}/api/2.0/secrets/put", headers=headers,
                  json={"scope": "renault-demo", "key": f"{sp_cfg['name']}-id", "string_value": app_id}).raise_for_status()
    requests.post(f"{workspace_url}/api/2.0/secrets/put", headers=headers,
                  json={"scope": "renault-demo", "key": f"{sp_cfg['name']}-secret", "string_value": sp_secret}).raise_for_status()

    # Grant CAN_USE on Lakebase project
    requests.patch(
        f"{workspace_url}/api/2.0/permissions/database-projects/{project_uid}",
        headers=headers,
        json={"access_control_list": [{"service_principal_name": app_id, "permission_level": "CAN_USE"}]}
    ).raise_for_status()

    sp_credentials.append({"app_id": app_id, "secret": sp_secret, **sp_cfg})
    print(f"  Secret stored, project access granted")

print(f"\n{len(sp_credentials)} SPs ready")

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

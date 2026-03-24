# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Lakebase Setup (Synced Tables + Data API + RLS)
# MAGIC
# MAGIC Fully automated — works on a fresh workspace.
# MAGIC
# MAGIC 1. Creates synced tables (Delta Gold → Lakebase Postgres)
# MAGIC 2. Creates 2 service principals (one per concession group)
# MAGIC 3. Sets up Postgres roles + RLS policies
# MAGIC 4. Demos the Data API: each SP sees only their group's data
# MAGIC
# MAGIC **Manual steps** (one-time):
# MAGIC - Enable Data API: Lakebase UI → Data API → Enable
# MAGIC - Expose `car_sales` schema: Data API → Advanced settings → Exposed schemas

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

gold_tables = ["listings_detail", "concession_daily_kpis", "model_performance", "group_scorecard"]

for table in gold_tables:
    fqn = f"{CATALOG}.car_sales.{table}"
    try:
        spark.sql(f"ALTER TABLE {fqn} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
        print(f"CDF enabled: {fqn}")
    except Exception as e:
        print(f"CDF {fqn}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Get Lakebase project IDs

# COMMAND ----------

# Use REST API to get project info (CLI not authenticated in notebook runtime)
projects_resp = requests.get(f"{workspace_url}/api/2.0/database/projects", headers=headers)
projects = projects_resp.json().get("projects", projects_resp.json() if isinstance(projects_resp.json(), list) else [])

project_uid = None
for p in projects:
    if LAKEBASE_PROJECT in p.get("name", ""):
        project_uid = p["uid"]
        break

if not project_uid:
    print(f"Lakebase project '{LAKEBASE_PROJECT}' not found. Deploy the bundle first.")
    dbutils.notebook.exit("SKIPPED: Lakebase project not found")

branches_resp = requests.get(f"{workspace_url}/api/2.0/database/projects/{LAKEBASE_PROJECT}/branches", headers=headers)
branches = branches_resp.json().get("branches", branches_resp.json() if isinstance(branches_resp.json(), list) else [])
branch_uid = branches[0]["uid"]
branch_name = branches[0]["name"].split("/")[-1]

endpoints_resp = requests.get(f"{workspace_url}/api/2.0/database/projects/{LAKEBASE_PROJECT}/branches/{branch_name}/endpoints", headers=headers)
endpoints_data = endpoints_resp.json().get("endpoints", endpoints_resp.json() if isinstance(endpoints_resp.json(), list) else [])
endpoint_host = endpoints_data[0]["status"]["hosts"]["host"]
endpoint_name = endpoints_data[0]["name"].split("/")[-1]

print(f"Project: {project_uid}")
print(f"Branch: {branch_uid}")
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

    # Check if already exists
    resp = requests.get(
        f"{workspace_url}/api/2.0/database/synced_tables/{dest}",
        headers=headers
    )
    if resp.status_code == 200:
        state = resp.json().get("data_synchronization_status", {}).get("detailed_state", "?")
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
    if resp.status_code == 200:
        print(f"Created: {dest}")
    else:
        print(f"Error {cfg['table']}: {resp.json().get('message', resp.text)[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Wait for sync to complete

# COMMAND ----------

print("Waiting for synced tables to come online...\n")
for cfg in SYNC_CONFIG:
    dest = f"{CATALOG}.car_sales.{cfg['table']}_synced"
    for attempt in range(60):
        resp = requests.get(f"{workspace_url}/api/2.0/database/synced_tables/{dest}", headers=headers)
        state = resp.json().get("data_synchronization_status", {}).get("detailed_state", "UNKNOWN")
        if "ONLINE" in state:
            print(f"  {cfg['table']}_synced: {state} ✓")
            break
        elif "FAILED" in state:
            print(f"  {cfg['table']}_synced: {state} ✗")
            break
        time.sleep(5)
    else:
        print(f"  {cfg['table']}_synced: timeout (last state: {state})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create 2 Service Principals (one per concession group)
# MAGIC
# MAGIC - **SP Bernard**: sees only Groupe Bernard (GRP-01)
# MAGIC - **SP Gueudet**: sees only Groupe Gueudet (GRP-02)

# COMMAND ----------

SP_CONFIGS = [
    {"name": "renault-groupe-bernard", "group_id": "GRP-01", "label": "Groupe Bernard"},
    {"name": "renault-groupe-gueudet", "group_id": "GRP-02", "label": "Groupe Gueudet"},
]

# Create or reuse secret scope
scopes = [s.name for s in dbutils.secrets.listScopes()]
if "renault-demo" not in scopes:
    requests.post(f"{workspace_url}/api/2.0/secrets/scopes/create", headers=headers, json={"scope": "renault-demo"})

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
        print(f"SP '{sp_cfg['name']}' already exists: {app_id}")
        # Try to get existing secret from scope
        try:
            secret = dbutils.secrets.get("renault-demo", f"{sp_cfg['name']}-secret")
            sp_credentials.append({"app_id": app_id, "secret": secret, **sp_cfg})
            continue
        except Exception:
            print(f"  No stored secret — creating new one")
    else:
        # Create new SP
        new_sp = w.service_principals.create(display_name=sp_cfg["name"], active=True)
        app_id = new_sp.application_id
        print(f"Created SP '{sp_cfg['name']}': {app_id}")

    # Create OAuth secret
    secret_resp = requests.post(
        f"{workspace_url}/api/2.0/accounts/servicePrincipals/{existing_sp.id if existing_sp else new_sp.id}/credentials/secrets",
        headers=headers
    )
    secret_data = secret_resp.json()
    sp_secret = secret_data["secret"]

    # Store in secrets scope
    requests.post(f"{workspace_url}/api/2.0/secrets/put", headers=headers,
                  json={"scope": "renault-demo", "key": f"{sp_cfg['name']}-id", "string_value": app_id})
    requests.post(f"{workspace_url}/api/2.0/secrets/put", headers=headers,
                  json={"scope": "renault-demo", "key": f"{sp_cfg['name']}-secret", "string_value": sp_secret})

    # Grant CAN_USE on Lakebase project
    requests.patch(
        f"{workspace_url}/api/2.0/permissions/database-projects/{project_uid}",
        headers=headers,
        json={"access_control_list": [{"service_principal_name": app_id, "permission_level": "CAN_USE"}]}
    )

    sp_credentials.append({"app_id": app_id, "secret": sp_secret, **sp_cfg})
    print(f"  Secret stored, project access granted")

print(f"\n{len(sp_credentials)} SPs ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Set up Postgres roles + RLS

# COMMAND ----------

# Connect to Lakebase Postgres
cred_resp = requests.post(
    f"{workspace_url}/api/2.0/database/projects/{LAKEBASE_PROJECT}/branches/{branch_name}/endpoints/{endpoint_name}/credentials",
    headers=headers
)
pg_cred = cred_resp.json()
email = w.current_user.me().user_name

conn = psycopg2.connect(
    host=endpoint_host, port=5432, dbname="databricks_postgres",
    user=email, password=pg_cred["token"], sslmode="require"
)
conn.autocommit = True
cur = conn.cursor()

cur.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth")

# Create Postgres roles for each SP and grant to authenticator
for sp in sp_credentials:
    try:
        cur.execute(f"SELECT databricks_create_role('{sp['app_id']}', 'SERVICE_PRINCIPAL')")
        print(f"Created Postgres role: {sp['name']}")
    except Exception as e:
        if "already exists" in str(e):
            print(f"Role exists: {sp['name']}")
        else:
            raise

    cur.execute(f'GRANT "{sp["app_id"]}" TO authenticator')
    cur.execute(f'GRANT USAGE ON SCHEMA car_sales TO "{sp["app_id"]}"')
    cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA car_sales TO "{sp["app_id"]}"')

# Enable Postgres RLS on synced tables
for cfg in SYNC_CONFIG:
    table = f"car_sales.{cfg['table']}_synced"
    try:
        cur.execute(f'ALTER TABLE {table} ENABLE ROW LEVEL SECURITY')
        print(f"RLS enabled: {table}")
    except Exception as e:
        print(f"RLS {table}: {e}")

# Create Postgres RLS policies per SP
for sp in sp_credentials:
    for cfg in SYNC_CONFIG:
        table = f"car_sales.{cfg['table']}_synced"
        policy_name = f"rls_{sp['name'].replace('-', '_')}_{cfg['table']}"
        # Check if table has group_id column
        if cfg["table"] in ["listings_detail", "concession_daily_kpis", "group_scorecard"]:
            filter_col = "group_id"
        else:
            continue  # model_performance has no group_id

        try:
            cur.execute(f'DROP POLICY IF EXISTS {policy_name} ON {table}')
            cur.execute(f"""
                CREATE POLICY {policy_name} ON {table}
                TO "{sp['app_id']}"
                USING ({filter_col} = '{sp['group_id']}')
            """)
            print(f"Policy: {sp['name']} → {cfg['table']} ({filter_col} = '{sp['group_id']}')")
        except Exception as e:
            print(f"Policy {policy_name}: {e}")

cur.execute("NOTIFY pgrst, 'reload schema'")
conn.close()
print("\nPostgres RLS configured ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Demo: Data API with RLS
# MAGIC
# MAGIC Each SP gets an OAuth token and queries the same endpoint.
# MAGIC **Different results** — each sees only their concession group's data.

# COMMAND ----------

DATA_API_BASE = f"https://{endpoint_host}/api/2.0/workspace/{workspace_id}/rest/databricks_postgres/car_sales"

for sp in sp_credentials:
    # Get OAuth token for this SP
    token_resp = requests.post(
        f"{workspace_url}/oidc/v1/token",
        data={
            "grant_type": "client_credentials",
            "client_id": sp["app_id"],
            "client_secret": sp["secret"],
            "scope": "all-apis",
        },
    )
    sp_token = token_resp.json()["access_token"]

    print(f"\n{'='*60}")
    print(f"  {sp['label']} ({sp['name']})")
    print(f"  Allowed: {sp['group_id']}")
    print(f"{'='*60}")

    # Query listings
    resp = requests.get(
        f"{DATA_API_BASE}/listings_detail_synced",
        headers={"Authorization": f"Bearer {sp_token}"},
        params={"select": "listing_id,brand,model,price,etat,concession_name,group_id", "limit": "5"}
    )
    if resp.status_code == 200:
        data = resp.json()
        print(f"\n  Listings: {len(data)} rows returned (limit 5)")
        if data:
            display(spark.createDataFrame(pd.DataFrame(data)))
    else:
        print(f"  Error: {resp.status_code} — {resp.text[:200]}")

    # Count total visible rows
    resp = requests.get(
        f"{DATA_API_BASE}/listings_detail_synced",
        headers={"Authorization": f"Bearer {sp_token}", "Prefer": "count=exact", "Range-Unit": "items", "Range": "0-0"}
    )
    total = resp.headers.get("Content-Range", "?")
    print(f"\n  Total visible rows: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Summary

# COMMAND ----------

print("=" * 60)
print("  LAKEBASE + DATA API + RLS COMPLETE")
print("=" * 60)
print(f"\n  Synced tables (Delta → Postgres):")
for cfg in SYNC_CONFIG:
    print(f"    car_sales.{cfg['table']}_synced")
print(f"\n  Service Principals:")
for sp in sp_credentials:
    print(f"    {sp['name']} → sees {sp['group_id']} ({sp['label']})")
print(f"\n  Data API: {DATA_API_BASE}")
print(f"\n  Postgres RLS: each SP filtered by group_id")
print(f"\n  Manual steps (one-time):")
print(f"    1. Enable Data API: Lakebase UI → Data API → Enable")
print(f"    2. Expose schemas: Data API → Advanced → add 'car_sales'")

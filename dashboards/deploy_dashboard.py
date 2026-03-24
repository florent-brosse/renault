# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy AI/BI Lakeview Dashboard
# MAGIC
# MAGIC Creates or updates the Renault Car Sales dashboard via the Lakeview API.
# MAGIC Run this after the pipeline has completed at least one full refresh.

# COMMAND ----------

import json
import requests

# Get catalog from widget
try:
    dbutils.widgets.text("catalog", "renault_demo", "Catalog name")
    CATALOG = dbutils.widgets.get("catalog")
except Exception:
    CATALOG = "renault_demo"

# Load dashboard definition and replace catalog placeholder
with open("/Workspace" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit("/", 1)[0] + "/renault_car_sales_dashboard.json") as f:
    raw = f.read()

# Replace the catalog name in all queries
raw = raw.replace("serverless_stable_le1wzb_catalog", CATALOG).replace("renault_demo", CATALOG)
dashboard_def = json.loads(raw)

# Get workspace URL and token
host = spark.conf.get("spark.databricks.workspaceUrl", "")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

if not host.startswith("https://"):
    host = f"https://{host}"

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dashboard

# COMMAND ----------

# Get current user for parent_path
current_user = spark.sql("SELECT current_user()").collect()[0][0]

DASHBOARD_NAME = "Renault - Ventes Automobiles"

# Check if dashboard already exists
existing = requests.get(f"{host}/api/2.0/lakeview/dashboards", headers=headers).json()
dashboard_id = None
for d in existing.get("dashboards", []):
    if d.get("display_name") == DASHBOARD_NAME:
        dashboard_id = d["dashboard_id"]
        break

serialized = json.dumps(dashboard_def)

if dashboard_id:
    # Update existing
    resp = requests.patch(f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}", headers=headers,
                          json={"serialized_dashboard": serialized})
    if resp.status_code == 200:
        print(f"Dashboard updated: {dashboard_id}")
    else:
        print(f"Update error {resp.status_code}: {resp.text}")
else:
    # Create new
    resp = requests.post(f"{host}/api/2.0/lakeview/dashboards", headers=headers,
                         json={"display_name": DASHBOARD_NAME, "parent_path": f"/Users/{current_user}",
                               "serialized_dashboard": serialized})
    if resp.status_code == 200:
        dashboard_id = resp.json()["dashboard_id"]
        print(f"Dashboard created: {dashboard_id}")
    else:
        print(f"Create error {resp.status_code}: {resp.text}")

print(f"URL: {host}/sql/dashboardsv3/{dashboard_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish Dashboard

# COMMAND ----------

try:
    pub_resp = requests.post(f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}/published", headers=headers, json={})
    if pub_resp.status_code == 200:
        print(f"Dashboard published successfully")
    else:
        print(f"Publish warning: {pub_resp.status_code} - {pub_resp.text}")
except Exception as e:
    print(f"Could not publish (OK for draft): {e}")

# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy AI/BI Lakeview Dashboard
# MAGIC
# MAGIC Creates or updates the Renault Car Sales dashboard via the Lakeview API.
# MAGIC Run this after the pipeline has completed at least one full refresh.

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient

# Get catalog from widget
try:
    dbutils.widgets.text("catalog", "renault_demo", "Catalog name")
    dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID")
    CATALOG = dbutils.widgets.get("catalog")
    WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
except Exception:
    CATALOG = "renault_demo"
    WAREHOUSE_ID = ""

assert WAREHOUSE_ID, "warehouse_id parameter is required"

# Load dashboard definition and replace catalog placeholder
with open("/Workspace" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit("/", 1)[0] + "/renault_car_sales_dashboard.json") as f:
    raw = f.read()

# Replace the catalog name in all queries
raw = raw.replace("serverless_stable_le1wzb_catalog", CATALOG).replace("renault_demo", CATALOG)
dashboard_def = json.loads(raw)

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dashboard

# COMMAND ----------

# Get current user for parent_path
current_user = w.current_user.me().user_name

DASHBOARD_NAME = "Renault - Ventes Automobiles"

# Check if dashboard already exists
dashboard_id = None
for d in w.lakeview.list():
    if d.display_name == DASHBOARD_NAME:
        dashboard_id = d.dashboard_id
        break

serialized = json.dumps(dashboard_def)

if dashboard_id:
    # Update existing
    w.lakeview.update(dashboard_id=dashboard_id, serialized_dashboard=serialized)
    print(f"Dashboard updated: {dashboard_id}")
else:
    # Create new
    result = w.lakeview.create(
        display_name=DASHBOARD_NAME,
        parent_path=f"/Users/{current_user}",
        serialized_dashboard=serialized,
    )
    dashboard_id = result.dashboard_id
    print(f"Dashboard created: {dashboard_id}")

host = w.config.host.rstrip("/")
print(f"URL: {host}/sql/dashboardsv3/{dashboard_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish Dashboard

# COMMAND ----------

try:
    w.lakeview.publish(dashboard_id=dashboard_id, warehouse_id=WAREHOUSE_ID)
    print("Dashboard published successfully")
except Exception as e:
    print(f"Could not publish (OK for draft): {e}")

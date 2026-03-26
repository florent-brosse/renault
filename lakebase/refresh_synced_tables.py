# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh Lakebase Synced Tables
# MAGIC
# MAGIC Triggers a SNAPSHOT refresh for all Renault synced tables.
# MAGIC SNAPSHOT mode doesn't auto-sync — must be triggered after each pipeline run.

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../generators/config

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

SYNCED_TABLES = [
    f"{CATALOG}.car_sales.listings_detail_synced",
    f"{CATALOG}.car_sales.concession_daily_kpis_synced",
    f"{CATALOG}.car_sales.model_performance_synced",
    f"{CATALOG}.car_sales.group_scorecard_synced",
]

pipeline_ids = []
for table_name in SYNCED_TABLES:
    table = w.database.get_synced_database_table(name=table_name)
    pid = table.data_synchronization_status.pipeline_id
    w.pipelines.start_update(pipeline_id=pid)
    print(f"Triggered: {table_name} (pipeline {pid})")
    pipeline_ids.append((table_name, pid))

# COMMAND ----------

# Wait for all syncs to complete
print("Waiting for syncs to complete...\n")
for table_name, pid in pipeline_ids:
    short_name = table_name.split(".")[-1]
    for attempt in range(60):
        pipe = w.pipelines.get(pipeline_id=pid)
        if pipe.state.value == "IDLE":
            print(f"  {short_name}: done")
            break
        elif pipe.state.value == "FAILED":
            print(f"  {short_name}: FAILED")
            break
        time.sleep(5)
    else:
        print(f"  {short_name}: timeout")

print("\nAll synced tables refreshed.")

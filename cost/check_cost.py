# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Cost Analysis
# MAGIC
# MAGIC Query system tables to see total cost of the demo, broken down by SKU.
# MAGIC Jobs are tagged with `project=renault-demo`.
# MAGIC Serverless pipeline costs are tracked via `dlt_pipeline_id` (budget policies needed for custom tags on serverless).
# MAGIC SQL warehouse costs are tracked via warehouse tags.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total DBU consumption for the Renault demo
# MAGIC SELECT
# MAGIC   sku_name,
# MAGIC   usage_type,
# MAGIC   ROUND(SUM(usage_quantity), 2) AS total_dbus,
# MAGIC   ROUND(SUM(usage_quantity) * 0.07, 2) AS estimated_cost_usd
# MAGIC FROM system.billing.usage
# MAGIC WHERE custom_tags['project'] = 'renault-demo'
# MAGIC   AND usage_date >= '2025-10-01'
# MAGIC GROUP BY sku_name, usage_type
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily cost trend
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   sku_name,
# MAGIC   ROUND(SUM(usage_quantity), 2) AS dbus,
# MAGIC   ROUND(SUM(usage_quantity) * 0.07, 2) AS estimated_cost_usd
# MAGIC FROM system.billing.usage
# MAGIC WHERE custom_tags['project'] = 'renault-demo'
# MAGIC   AND usage_date >= '2025-10-01'
# MAGIC GROUP BY usage_date, sku_name
# MAGIC ORDER BY usage_date DESC, dbus DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Breakdown by resource type (jobs, pipeline, warehouse, serverless)
# MAGIC SELECT
# MAGIC   workspace_id,
# MAGIC   sku_name,
# MAGIC   usage_type,
# MAGIC   billing_origin_product,
# MAGIC   usage_metadata.job_id,
# MAGIC   usage_metadata.warehouse_id,
# MAGIC   usage_metadata.dlt_pipeline_id,
# MAGIC   ROUND(SUM(usage_quantity), 2) AS total_dbus
# MAGIC FROM system.billing.usage
# MAGIC WHERE custom_tags['project'] = 'renault-demo'
# MAGIC   AND usage_date >= '2025-10-01'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pipeline + synced tables cost (by pipeline ID, billing_origin_product = DLT or DATABASE)
# MAGIC SELECT
# MAGIC   billing_origin_product,
# MAGIC   sku_name,
# MAGIC   usage_metadata.dlt_pipeline_id AS pipeline_id,
# MAGIC   ROUND(SUM(usage_quantity), 4) AS total_dbus,
# MAGIC   ROUND(SUM(usage_quantity) * 0.07, 4) AS estimated_cost_usd
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_metadata.dlt_pipeline_id IN (
# MAGIC   SELECT pipeline_id FROM system.lakeflow.pipelines
# MAGIC   WHERE name LIKE '%Renault%' OR name LIKE '%synced%car_sales%'
# MAGIC )
# MAGIC   AND usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY ALL
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lakebase cost (no custom tags — tracked by billing_origin_product)
# MAGIC SELECT
# MAGIC   billing_origin_product,
# MAGIC   sku_name,
# MAGIC   ROUND(SUM(usage_quantity), 4) AS total_dbus,
# MAGIC   ROUND(SUM(usage_quantity) * 0.07, 4) AS estimated_cost_usd
# MAGIC FROM system.billing.usage
# MAGIC WHERE billing_origin_product IN ('LAKEBASE', 'DATABASE')
# MAGIC   AND usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY ALL
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grand total (all sources combined)
# MAGIC SELECT
# MAGIC   billing_origin_product,
# MAGIC   ROUND(SUM(usage_quantity), 4) AS total_dbus,
# MAGIC   ROUND(SUM(usage_quantity) * 0.07, 4) AS estimated_cost_usd
# MAGIC FROM system.billing.usage
# MAGIC WHERE (
# MAGIC   custom_tags['project'] = 'renault-demo'
# MAGIC   OR usage_metadata.dlt_pipeline_id IN (
# MAGIC     SELECT pipeline_id FROM system.lakeflow.pipelines
# MAGIC     WHERE name LIKE '%Renault%' OR name LIKE '%synced%car_sales%'
# MAGIC   )
# MAGIC   OR billing_origin_product IN ('LAKEBASE', 'DATABASE')
# MAGIC )
# MAGIC AND usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY billing_origin_product
# MAGIC ORDER BY total_dbus DESC

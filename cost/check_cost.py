# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Cost Analysis
# MAGIC
# MAGIC Query system tables to see total cost of the demo, broken down by SKU.
# MAGIC All resources are tagged with `project=renault-demo` for filtering.

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

# MAGIC %sql
# MAGIC -- Grand total
# MAGIC SELECT
# MAGIC   ROUND(SUM(usage_quantity), 2) AS total_dbus,
# MAGIC   ROUND(SUM(usage_quantity) * 0.07, 2) AS estimated_cost_usd,
# MAGIC   COUNT(DISTINCT sku_name) AS num_skus,
# MAGIC   MIN(usage_date) AS first_usage,
# MAGIC   MAX(usage_date) AS last_usage
# MAGIC FROM system.billing.usage
# MAGIC WHERE custom_tags['project'] = 'renault-demo'
# MAGIC   AND usage_date >= '2025-10-01'

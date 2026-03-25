# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Cost Analysis
# MAGIC
# MAGIC Cost tracking uses two methods:
# MAGIC - `custom_tags['project'] = 'renault-demo'` for jobs, warehouse, Lakebase
# MAGIC - `usage_metadata.dlt_pipeline_id` for DP pipeline and synced table pipelines
# MAGIC   (serverless pipeline tags don't propagate to billing custom_tags)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tagged resources (jobs, warehouse, Lakebase)
# MAGIC SELECT
# MAGIC   billing_origin_product,
# MAGIC   sku_name,
# MAGIC   ROUND(SUM(usage_quantity), 4) AS total_dbus,
# MAGIC   ROUND(SUM(usage_quantity) * 0.07, 4) AS estimated_cost_usd
# MAGIC FROM system.billing.usage
# MAGIC WHERE custom_tags['project'] = 'renault-demo'
# MAGIC   AND usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY billing_origin_product, sku_name
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DP pipeline + synced table pipelines (by pipeline ID — tags don't appear in billing)
# MAGIC SELECT
# MAGIC   billing_origin_product,
# MAGIC   sku_name,
# MAGIC   usage_metadata.dlt_pipeline_id AS pipeline_id,
# MAGIC   ROUND(SUM(usage_quantity), 4) AS total_dbus,
# MAGIC   ROUND(SUM(usage_quantity) * 0.07, 4) AS estimated_cost_usd
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_metadata.dlt_pipeline_id IN (
# MAGIC   SELECT pipeline_id FROM system.lakeflow.pipelines
# MAGIC   WHERE name ILIKE '%renault%' OR name ILIKE '%synced%car_sales%'
# MAGIC )
# MAGIC   AND usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY ALL
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grand total (both sources combined, deduplicated)
# MAGIC SELECT
# MAGIC   billing_origin_product,
# MAGIC   ROUND(SUM(usage_quantity), 4) AS total_dbus,
# MAGIC   ROUND(SUM(usage_quantity) * 0.07, 4) AS estimated_cost_usd
# MAGIC FROM system.billing.usage
# MAGIC WHERE (
# MAGIC   custom_tags['project'] = 'renault-demo'
# MAGIC   OR usage_metadata.dlt_pipeline_id IN (
# MAGIC     SELECT pipeline_id FROM system.lakeflow.pipelines
# MAGIC     WHERE name ILIKE '%renault%' OR name ILIKE '%synced%car_sales%'
# MAGIC   )
# MAGIC )
# MAGIC AND usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY billing_origin_product
# MAGIC ORDER BY total_dbus DESC

# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Cost Analysis
# MAGIC
# MAGIC Cost tracking:
# MAGIC - **Jobs, Warehouse, Lakebase**: `custom_tags['project'] = 'renault-demo'` in billing
# MAGIC - **DP Pipeline, Synced pipelines**: serverless pipeline tags do NOT propagate to
# MAGIC   `system.billing.usage.custom_tags`. Must join via `dlt_pipeline_id` on `system.lakeflow.pipelines`.

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
# MAGIC -- DP pipeline + synced table pipelines (join via pipeline ID)
# MAGIC -- Pipeline spec.tags has project=renault-demo but it doesn't propagate to billing
# MAGIC SELECT
# MAGIC   u.billing_origin_product,
# MAGIC   u.sku_name,
# MAGIC   p.name AS pipeline_name,
# MAGIC   ROUND(SUM(u.usage_quantity), 4) AS total_dbus,
# MAGIC   ROUND(SUM(u.usage_quantity) * 0.07, 4) AS estimated_cost_usd
# MAGIC FROM system.billing.usage u
# MAGIC JOIN system.lakeflow.pipelines p
# MAGIC   ON u.usage_metadata.dlt_pipeline_id = p.pipeline_id
# MAGIC WHERE p.tags['project'] = 'renault-demo'
# MAGIC   AND u.usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY ALL
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grand total (all sources)
# MAGIC SELECT
# MAGIC   billing_origin_product,
# MAGIC   ROUND(SUM(usage_quantity), 4) AS total_dbus,
# MAGIC   ROUND(SUM(usage_quantity) * 0.07, 4) AS estimated_cost_usd
# MAGIC FROM system.billing.usage
# MAGIC WHERE (
# MAGIC   custom_tags['project'] = 'renault-demo'
# MAGIC   OR usage_metadata.dlt_pipeline_id IN (
# MAGIC     SELECT pipeline_id FROM system.lakeflow.pipelines
# MAGIC     WHERE tags['project'] = 'renault-demo'
# MAGIC   )
# MAGIC )
# MAGIC AND usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY billing_origin_product
# MAGIC ORDER BY total_dbus DESC

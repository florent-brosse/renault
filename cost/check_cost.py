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
# MAGIC   u.billing_origin_product,
# MAGIC   u.sku_name,
# MAGIC   ROUND(SUM(u.usage_quantity), 4) AS total_dbus,
# MAGIC   ROUND(SUM(u.usage_quantity * p.pricing.default), 4) AS cost_usd
# MAGIC FROM system.billing.usage u
# MAGIC JOIN system.billing.list_prices p
# MAGIC   ON u.sku_name = p.sku_name
# MAGIC   AND u.usage_start_time >= p.price_start_time
# MAGIC   AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
# MAGIC WHERE u.custom_tags['project'] = 'renault-demo'
# MAGIC   AND u.usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY u.billing_origin_product, u.sku_name
# MAGIC ORDER BY cost_usd DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DP pipeline + synced table pipelines (join via pipeline ID)
# MAGIC -- Pipeline spec.tags has project=renault-demo but it doesn't propagate to billing
# MAGIC SELECT
# MAGIC   u.billing_origin_product,
# MAGIC   u.sku_name,
# MAGIC   pl.name AS pipeline_name,
# MAGIC   ROUND(SUM(u.usage_quantity), 4) AS total_dbus,
# MAGIC   ROUND(SUM(u.usage_quantity * pr.pricing.default), 4) AS cost_usd
# MAGIC FROM system.billing.usage u
# MAGIC JOIN system.lakeflow.pipelines pl
# MAGIC   ON u.usage_metadata.dlt_pipeline_id = pl.pipeline_id
# MAGIC JOIN system.billing.list_prices pr
# MAGIC   ON u.sku_name = pr.sku_name
# MAGIC   AND u.usage_start_time >= pr.price_start_time
# MAGIC   AND (pr.price_end_time IS NULL OR u.usage_start_time < pr.price_end_time)
# MAGIC WHERE pl.tags['project'] = 'renault-demo'
# MAGIC   AND u.usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY ALL
# MAGIC ORDER BY cost_usd DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grand total (all sources)
# MAGIC SELECT
# MAGIC   u.billing_origin_product,
# MAGIC   ROUND(SUM(u.usage_quantity), 4) AS total_dbus,
# MAGIC   ROUND(SUM(u.usage_quantity * p.pricing.default), 4) AS cost_usd
# MAGIC FROM system.billing.usage u
# MAGIC JOIN system.billing.list_prices p
# MAGIC   ON u.sku_name = p.sku_name
# MAGIC   AND u.usage_start_time >= p.price_start_time
# MAGIC   AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
# MAGIC WHERE (
# MAGIC   u.custom_tags['project'] = 'renault-demo'
# MAGIC   OR u.usage_metadata.dlt_pipeline_id IN (
# MAGIC     SELECT pipeline_id FROM system.lakeflow.pipelines
# MAGIC     WHERE tags['project'] = 'renault-demo'
# MAGIC   )
# MAGIC )
# MAGIC AND u.usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY u.billing_origin_product
# MAGIC ORDER BY cost_usd DESC

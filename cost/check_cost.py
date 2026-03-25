# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Cost Analysis
# MAGIC
# MAGIC All resources are tagged with `project=renault-demo` and `customer=renault`.
# MAGIC Query `system.billing.usage` by `custom_tags` for unified cost tracking.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total cost by SKU
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
# MAGIC -- Daily cost trend
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   billing_origin_product,
# MAGIC   ROUND(SUM(usage_quantity), 4) AS dbus,
# MAGIC   ROUND(SUM(usage_quantity) * 0.07, 4) AS estimated_cost_usd
# MAGIC FROM system.billing.usage
# MAGIC WHERE custom_tags['project'] = 'renault-demo'
# MAGIC   AND usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY usage_date, billing_origin_product
# MAGIC ORDER BY usage_date DESC, dbus DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Breakdown by resource
# MAGIC SELECT
# MAGIC   billing_origin_product,
# MAGIC   sku_name,
# MAGIC   usage_metadata.job_id,
# MAGIC   usage_metadata.warehouse_id,
# MAGIC   usage_metadata.dlt_pipeline_id,
# MAGIC   ROUND(SUM(usage_quantity), 4) AS total_dbus
# MAGIC FROM system.billing.usage
# MAGIC WHERE custom_tags['project'] = 'renault-demo'
# MAGIC   AND usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY ALL
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grand total
# MAGIC SELECT
# MAGIC   ROUND(SUM(usage_quantity), 4) AS total_dbus,
# MAGIC   ROUND(SUM(usage_quantity) * 0.07, 4) AS estimated_cost_usd,
# MAGIC   COUNT(DISTINCT billing_origin_product) AS products,
# MAGIC   COUNT(DISTINCT sku_name) AS skus,
# MAGIC   MIN(usage_date) AS first_usage,
# MAGIC   MAX(usage_date) AS last_usage
# MAGIC FROM system.billing.usage
# MAGIC WHERE custom_tags['project'] = 'renault-demo'
# MAGIC   AND usage_date >= CURRENT_DATE - INTERVAL 7 DAYS

# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Cost Analysis v2 (Budget Policy)
# MAGIC
# MAGIC Serverless costs are attributed via **budget policy** `renault-policy`
# MAGIC which auto-tags billing with `project=renault-policy`.
# MAGIC Warehouse and Lakebase use `custom_tags['project'] = 'renault-demo'`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cost breakdown by product and SKU
# MAGIC SELECT
# MAGIC   billing_origin_product,
# MAGIC   sku_name,
# MAGIC   ROUND(SUM(usage_quantity), 4) AS total_dbus,
# MAGIC   ROUND(SUM(usage_quantity) * 0.07, 4) AS estimated_cost_usd
# MAGIC FROM system.billing.usage
# MAGIC WHERE (custom_tags['project'] IN ('renault-policy', 'renault-demo'))
# MAGIC   AND usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY billing_origin_product, sku_name
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grand total by product
# MAGIC SELECT
# MAGIC   billing_origin_product,
# MAGIC   ROUND(SUM(usage_quantity), 4) AS total_dbus,
# MAGIC   ROUND(SUM(usage_quantity) * 0.07, 4) AS estimated_cost_usd
# MAGIC FROM system.billing.usage
# MAGIC WHERE (custom_tags['project'] IN ('renault-policy', 'renault-demo'))
# MAGIC   AND usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY billing_origin_product
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily cost trend
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   billing_origin_product,
# MAGIC   ROUND(SUM(usage_quantity), 4) AS total_dbus,
# MAGIC   ROUND(SUM(usage_quantity) * 0.07, 4) AS estimated_cost_usd
# MAGIC FROM system.billing.usage
# MAGIC WHERE (custom_tags['project'] IN ('renault-policy', 'renault-demo'))
# MAGIC   AND usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY usage_date, billing_origin_product
# MAGIC ORDER BY usage_date DESC, total_dbus DESC

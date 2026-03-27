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
# MAGIC   u.billing_origin_product,
# MAGIC   u.sku_name,
# MAGIC   ROUND(SUM(u.usage_quantity), 4) AS total_dbus,
# MAGIC   ROUND(SUM(u.usage_quantity * p.pricing.default), 4) AS cost_usd
# MAGIC FROM system.billing.usage u
# MAGIC JOIN system.billing.list_prices p
# MAGIC   ON u.sku_name = p.sku_name
# MAGIC   AND u.usage_start_time >= p.price_start_time
# MAGIC   AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
# MAGIC WHERE (u.custom_tags['project'] IN ('renault-policy', 'renault-demo'))
# MAGIC   AND u.usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY u.billing_origin_product, u.sku_name
# MAGIC ORDER BY cost_usd DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grand total by product
# MAGIC SELECT
# MAGIC   u.billing_origin_product,
# MAGIC   ROUND(SUM(u.usage_quantity), 4) AS total_dbus,
# MAGIC   ROUND(SUM(u.usage_quantity * p.pricing.default), 4) AS cost_usd
# MAGIC FROM system.billing.usage u
# MAGIC JOIN system.billing.list_prices p
# MAGIC   ON u.sku_name = p.sku_name
# MAGIC   AND u.usage_start_time >= p.price_start_time
# MAGIC   AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
# MAGIC WHERE (u.custom_tags['project'] IN ('renault-policy', 'renault-demo'))
# MAGIC   AND u.usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY u.billing_origin_product
# MAGIC ORDER BY cost_usd DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily cost trend
# MAGIC SELECT
# MAGIC   u.usage_date,
# MAGIC   u.billing_origin_product,
# MAGIC   ROUND(SUM(u.usage_quantity), 4) AS total_dbus,
# MAGIC   ROUND(SUM(u.usage_quantity * p.pricing.default), 4) AS cost_usd
# MAGIC FROM system.billing.usage u
# MAGIC JOIN system.billing.list_prices p
# MAGIC   ON u.sku_name = p.sku_name
# MAGIC   AND u.usage_start_time >= p.price_start_time
# MAGIC   AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
# MAGIC WHERE (u.custom_tags['project'] IN ('renault-policy', 'renault-demo'))
# MAGIC   AND u.usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY u.usage_date, u.billing_origin_product
# MAGIC ORDER BY u.usage_date DESC, cost_usd DESC

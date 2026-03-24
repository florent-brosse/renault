# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Row-Level Security Setup
# MAGIC
# MAGIC Sets up RLS on Gold tables so each concession group only sees their own data.
# MAGIC Uses Unity Catalog row filters.
# MAGIC
# MAGIC ## How it works
# MAGIC 1. Create a mapping table: `user_group_mapping` — maps users/groups to concession group_ids
# MAGIC 2. Create a row filter function that checks the current user's group membership
# MAGIC 3. Apply the row filter to Gold tables
# MAGIC
# MAGIC ## For the demo
# MAGIC - Create sample users or use existing Entra ID groups mapped to concession groups
# MAGIC - Admins (workspace admins) see all data
# MAGIC - Concession group users see only their group's data

# COMMAND ----------

# MAGIC %run ../generators/config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create user-to-group mapping table

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.car_sales")

# This table maps SSO users/groups to concession groups
# In production, this would be synced from the IdP (Entra ID / Okta)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.car_sales.user_group_mapping (
  user_or_group STRING COMMENT 'Email or group name from IdP',
  group_id STRING COMMENT 'Concession group ID (GRP-XX)',
  group_name STRING COMMENT 'Concession group name',
  is_admin BOOLEAN COMMENT 'If true, sees all data regardless of group'
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# Insert sample mappings — adapt these to your demo users
sample_mappings = [
    # Admins see everything
    ("admin@renault.com", None, None, True),
    # Group-specific users
    ("groupe.bernard@renault.com", "GRP-01", "Groupe Bernard", False),
    ("groupe.gueudet@renault.com", "GRP-02", "Groupe Gueudet", False),
    ("groupe.jeanlain@renault.com", "GRP-03", "Groupe Jean Lain", False),
    ("groupe.bodemer@renault.com", "GRP-04", "Groupe Bodemer", False),
    ("groupe.dubreuil@renault.com", "GRP-05", "Groupe Dubreuil", False),
    ("groupe.mary@renault.com", "GRP-06", "Groupe Mary", False),
    ("groupe.parot@renault.com", "GRP-07", "Groupe Parot", False),
    ("groupe.claro@renault.com", "GRP-08", "Groupe Claro", False),
]

df_mappings = spark.createDataFrame(sample_mappings, ["user_or_group", "group_id", "group_name", "is_admin"])
df_mappings.write.mode("overwrite").saveAsTable(f"{CATALOG}.car_sales.user_group_mapping")

print(f"User-group mapping table created with {len(sample_mappings)} entries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create the row filter function

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.car_sales.rls_concession_group_filter(row_group_id STRING)
RETURNS BOOLEAN
COMMENT 'Row filter: returns TRUE if the current user is allowed to see this concession group'
RETURN
  -- Admins see everything
  EXISTS (
    SELECT 1 FROM {CATALOG}.car_sales.user_group_mapping
    WHERE user_or_group = current_user()
      AND is_admin = TRUE
  )
  OR
  -- IS_ACCOUNT_GROUP_MEMBER check for IdP groups
  EXISTS (
    SELECT 1 FROM {CATALOG}.car_sales.user_group_mapping
    WHERE is_admin = FALSE
      AND group_id = row_group_id
      AND IS_ACCOUNT_GROUP_MEMBER(user_or_group)
  )
  OR
  -- Direct user match
  EXISTS (
    SELECT 1 FROM {CATALOG}.car_sales.user_group_mapping
    WHERE user_or_group = current_user()
      AND (group_id = row_group_id OR is_admin = TRUE)
  )
""")

print("Row filter function created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Apply row filters to Gold tables

# COMMAND ----------

# Tables that have group_id column and need RLS
gold_tables_with_rls = [
    "concession_daily_kpis",
    "group_scorecard",
    "listings_detail",
]

for table in gold_tables_with_rls:
    fqn = f"{CATALOG}.car_sales.{table}"
    try:
        spark.sql(f"""
        ALTER TABLE {fqn}
        SET ROW FILTER {CATALOG}.car_sales.rls_concession_group_filter ON (group_id)
        """)
        print(f"RLS applied to {fqn}")
    except Exception as e:
        print(f"Could not apply RLS to {fqn}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify RLS

# COMMAND ----------

print(f"\nCurrent user: {spark.sql('SELECT current_user()').collect()[0][0]}")
print(f"\nTo test RLS:")
print(f"  1. Add your email to {CATALOG}.car_sales.user_group_mapping")
print(f"  2. Set is_admin=TRUE to see all data, or assign a specific group_id")
print(f"  3. Query a Gold table — you should only see your group's data")
print(f"\nExample:")
print(f"  INSERT INTO {CATALOG}.car_sales.user_group_mapping VALUES (current_user(), 'GRP-01', 'Groupe Bernard', FALSE)")
print(f"  SELECT * FROM {CATALOG}.car_sales.listings_detail LIMIT 10")

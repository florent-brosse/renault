# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Row-Level Security via ABAC Policies
# MAGIC
# MAGIC Sets up attribute-based access control (ABAC) on Gold tables so each
# MAGIC concession group only sees their own data.
# MAGIC
# MAGIC ## How it works
# MAGIC 1. Create a governed tag `concession_group` on Unity Catalog
# MAGIC 2. Tag the `group_id` column on Gold tables
# MAGIC 3. Create a UDF that checks the current user's group attribute
# MAGIC 4. Create an ABAC policy that auto-applies row filtering
# MAGIC
# MAGIC See: https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policies

# COMMAND ----------

# MAGIC %run ../generators/config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the mapping table (user → concession group)
# MAGIC
# MAGIC In production this would be synced from IdP (Entra ID / Okta).

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_CAR_SALES}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping (
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
df_mappings.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping")

print(f"User-group mapping table created with {len(sample_mappings)} entries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tag the `group_id` column on Gold tables
# MAGIC
# MAGIC We tag the `group_id` column with `concession_group` so the ABAC policy
# MAGIC auto-discovers which column to filter on.

# COMMAND ----------

gold_tables_with_rls = [
    "concession_daily_kpis",
    "group_scorecard",
    "listings_detail",
]

for table in gold_tables_with_rls:
    fqn = f"{CATALOG}.{SCHEMA_CAR_SALES}.{table}"
    try:
        spark.sql(f"""
        ALTER TABLE {fqn}
        ALTER COLUMN group_id
        SET TAGS ('concession_group' = 'group_id')
        """)
        print(f"Tagged group_id on {fqn}")
    except Exception as e:
        print(f"Tag {fqn}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create the row filter UDF
# MAGIC
# MAGIC This function checks if the current user is allowed to see a given `group_id`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA_CAR_SALES}.rls_filter_by_group(row_group_id STRING)
RETURNS BOOLEAN
COMMENT 'ABAC row filter: returns TRUE if the current user can see this concession group'
RETURN (
  SELECT COALESCE(
    -- Admin check
    (SELECT MAX(CASE WHEN is_admin THEN TRUE ELSE FALSE END)
     FROM {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping
     WHERE user_or_group = current_user()),
    FALSE
  )
  OR
  -- Group membership check (direct user or IdP group)
  EXISTS (
    SELECT 1 FROM {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping
    WHERE group_id = row_group_id
      AND (user_or_group = current_user() OR IS_ACCOUNT_GROUP_MEMBER(user_or_group))
  )
)
""")

print("Row filter UDF created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create the ABAC policy
# MAGIC
# MAGIC This policy auto-applies to any table in the schema that has a column
# MAGIC tagged with `concession_group`.

# COMMAND ----------

try:
    spark.sql(f"""
    CREATE OR REPLACE POLICY rls_concession_group
    ON SCHEMA {CATALOG}.{SCHEMA_CAR_SALES}
    COMMENT 'Row-level security: filter by concession group membership'
    ROW FILTER {CATALOG}.{SCHEMA_CAR_SALES}.rls_filter_by_group
    TO `account users`
    EXCEPT `admins`
    FOR TABLES
    MATCH COLUMNS has_tag_value('concession_group', 'group_id') AS grp
    USING COLUMNS (grp)
    """)
    print("ABAC policy created — auto-applies to all tagged Gold tables")
except Exception as e:
    print(f"ABAC policy creation failed: {e}")
    print("\nFalling back to manual row filters...")
    # Fallback: apply row filters manually (works on all runtimes)
    for table in gold_tables_with_rls:
        fqn = f"{CATALOG}.{SCHEMA_CAR_SALES}.{table}"
        try:
            spark.sql(f"""
            ALTER TABLE {fqn}
            SET ROW FILTER {CATALOG}.{SCHEMA_CAR_SALES}.rls_filter_by_group ON (group_id)
            """)
            print(f"Manual row filter applied to {fqn}")
        except Exception as e2:
            print(f"  Could not apply to {fqn}: {e2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify

# COMMAND ----------

current_user = spark.sql("SELECT current_user()").collect()[0][0]
print(f"Current user: {current_user}")
print(f"\nTo test RLS:")
print(f"  1. Add your email to {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping")
print(f"  2. Set is_admin=TRUE to see all, or assign a group_id")
print(f"  3. Query a Gold table — filtered automatically")
print(f"\nExample:")
print(f"  INSERT INTO {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping")
print(f"  VALUES (current_user(), 'GRP-01', 'Groupe Bernard', FALSE)")

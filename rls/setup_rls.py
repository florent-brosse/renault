# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Row-Level Security via Dynamic Views
# MAGIC
# MAGIC Creates dynamic views on top of Gold MVs with built-in RLS filtering.
# MAGIC
# MAGIC ## Why dynamic views (not ALTER MV SET ROW FILTER)?
# MAGIC - `ALTER MV SET ROW FILTER` and ABAC policies both block synced tables
# MAGIC   ("Table cannot have both row/column security and online materialized views")
# MAGIC - Dynamic views keep MVs clean for Lakebase synced tables
# MAGIC - Same RLS effect: each user sees only their concession group's data
# MAGIC
# MAGIC ## Architecture
# MAGIC ```
# MAGIC Gold MVs (no RLS) ──→ Synced tables → Postgres (Data API)
# MAGIC      │
# MAGIC      └──→ Dynamic views (WHERE rls_filter()) → Dashboards / Genie / DBSQL
# MAGIC ```

# COMMAND ----------

# MAGIC %run ../generators/config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create mapping table (user → concession group)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_CAR_SALES}")

# Create mapping table if not exists (Lakebase may have already created it with SP entries)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping (
  user_or_group STRING,
  group_id STRING,
  group_name STRING,
  is_admin BOOLEAN
) USING DELTA
""")

# MERGE IdP group mappings (preserves SPs inserted by Lakebase)
idp_mappings = [
    ("admin@renault.com", None, None, True),
    ("groupe_bernard", "GRP-01", "Groupe Bernard", False),
    ("groupe_gueudet", "GRP-02", "Groupe Gueudet", False),
    ("groupe_jeanlain", "GRP-03", "Groupe Jean Lain", False),
    ("groupe_bodemer", "GRP-04", "Groupe Bodemer", False),
    ("groupe_dubreuil", "GRP-05", "Groupe Dubreuil", False),
    ("groupe_mary", "GRP-06", "Groupe Mary", False),
    ("groupe_parot", "GRP-07", "Groupe Parot", False),
    ("groupe_claro", "GRP-08", "Groupe Claro", False),
]

for user, gid, gname, is_admin in idp_mappings:
    gid_sql = f"'{gid}'" if gid else "NULL"
    gname_sql = f"'{gname}'" if gname else "NULL"
    spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping t
    USING (SELECT '{user}' AS user_or_group, {gid_sql} AS group_id, {gname_sql} AS group_name, {is_admin} AS is_admin) s
    ON t.user_or_group = s.user_or_group
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

# Add current user as admin
current_user = spark.sql("SELECT current_user()").collect()[0][0]
spark.sql(f"""
MERGE INTO {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping t
USING (SELECT '{current_user}' AS user_or_group, CAST(NULL AS STRING) AS group_id, CAST(NULL AS STRING) AS group_name, TRUE AS is_admin) s
ON t.user_or_group = s.user_or_group
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

total = spark.sql(f"SELECT count(*) FROM {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping").collect()[0][0]
print(f"Mapping table: {total} entries (IdP groups + SPs + admin)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create RLS filter function

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA_CAR_SALES}.rls_filter(row_group_id STRING)
RETURNS BOOLEAN
RETURN
  EXISTS (
    SELECT 1 FROM {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping
    WHERE user_or_group = current_user() AND is_admin = TRUE
  )
  OR EXISTS (
    SELECT 1 FROM {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping
    WHERE group_id = row_group_id
      AND (user_or_group = current_user() OR IS_ACCOUNT_GROUP_MEMBER(user_or_group))
  )
""")
print("RLS filter function created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create dynamic views with RLS
# MAGIC
# MAGIC Views call `rls_filter(group_id)` in WHERE — each user sees only their group.
# MAGIC Gold MVs remain clean (no RLS) so synced tables work.

# COMMAND ----------

VIEWS_WITH_RLS = [
    "listings_detail",
    "concession_daily_kpis",
    "group_scorecard",
]

for table in VIEWS_WITH_RLS:
    spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA_CAR_SALES}.v_{table} AS
    SELECT * FROM {CATALOG}.{SCHEMA_CAR_SALES}.{table}
    WHERE {CATALOG}.{SCHEMA_CAR_SALES}.rls_filter(group_id)
    """)
    print(f"View created: v_{table}")

# model_performance has no group_id — create pass-through view for consistency
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA_CAR_SALES}.v_model_performance AS
SELECT * FROM {CATALOG}.{SCHEMA_CAR_SALES}.model_performance
""")
print("View created: v_model_performance (no RLS — no group_id)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify RLS

# COMMAND ----------

# Test 1: Admin sees all
total_admin = spark.sql(f"SELECT count(*) FROM {CATALOG}.{SCHEMA_CAR_SALES}.v_listings_detail").collect()[0][0]
groups_admin = spark.sql(f"SELECT count(distinct group_id) FROM {CATALOG}.{SCHEMA_CAR_SALES}.v_listings_detail").collect()[0][0]
print(f"[TEST 1] Admin: {total_admin:,} rows, {groups_admin} groups")
assert groups_admin == 8, f"Admin should see 8 groups, got {groups_admin}"

# Test 2: Switch to GRP-01
spark.sql(f"DELETE FROM {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping WHERE user_or_group = '{current_user}'")
spark.sql(f"INSERT INTO {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping VALUES ('{current_user}', 'GRP-01', 'Groupe Bernard', FALSE)")

total_grp01 = spark.sql(f"SELECT count(*) FROM {CATALOG}.{SCHEMA_CAR_SALES}.v_listings_detail").collect()[0][0]
groups_grp01 = spark.sql(f"SELECT count(distinct group_id) FROM {CATALOG}.{SCHEMA_CAR_SALES}.v_listings_detail").collect()[0][0]
print(f"[TEST 2] GRP-01: {total_grp01:,} rows, {groups_grp01} group(s)")
assert groups_grp01 == 1, f"GRP-01 should see 1 group, got {groups_grp01}"

# Test 3: Switch to GRP-02
spark.sql(f"DELETE FROM {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping WHERE user_or_group = '{current_user}'")
spark.sql(f"INSERT INTO {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping VALUES ('{current_user}', 'GRP-02', 'Groupe Gueudet', FALSE)")

total_grp02 = spark.sql(f"SELECT count(*) FROM {CATALOG}.{SCHEMA_CAR_SALES}.v_listings_detail").collect()[0][0]
groups_grp02 = spark.sql(f"SELECT count(distinct group_id) FROM {CATALOG}.{SCHEMA_CAR_SALES}.v_listings_detail").collect()[0][0]
print(f"[TEST 3] GRP-02: {total_grp02:,} rows, {groups_grp02} group(s)")
assert groups_grp02 == 1, f"GRP-02 should see 1 group, got {groups_grp02}"
assert total_grp02 != total_grp01, "GRP-01 and GRP-02 should have different row counts"

# Test 4: MV still has no filter (for synced tables)
total_mv = spark.sql(f"SELECT count(*) FROM {CATALOG}.{SCHEMA_CAR_SALES}.listings_detail").collect()[0][0]
print(f"[TEST 4] MV (no filter): {total_mv:,} rows")
assert total_mv == total_admin, "MV should always show all data"

# Restore admin
spark.sql(f"DELETE FROM {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping WHERE user_or_group = '{current_user}'")
spark.sql(f"INSERT INTO {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping VALUES ('{current_user}', NULL, NULL, TRUE)")

print(f"\n{'='*60}")
print(f"  RLS VERIFIED — Dynamic Views")
print(f"{'='*60}")
print(f"  Admin:     {total_admin:,} rows (8 groups)")
print(f"  GRP-01:    {total_grp01:,} rows (1 group)")
print(f"  GRP-02:    {total_grp02:,} rows (1 group)")
print(f"  MV (raw):  {total_mv:,} rows (all — no filter)")
print(f"  All assertions passed")

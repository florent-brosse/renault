# Databricks notebook source
# MAGIC %md
# MAGIC # Renault Demo — Row-Level Security via ABAC Policies
# MAGIC
# MAGIC Uses attribute-based access control (ABAC) so each concession group
# MAGIC only sees their own data. No mapping table needed — the policy defines
# MAGIC WHO sees WHAT directly.
# MAGIC
# MAGIC ## How it works
# MAGIC 1. Tag the `group_id` column on Gold tables with a governed tag
# MAGIC 2. Create a simple, deterministic UDF that filters rows by allowed group IDs
# MAGIC 3. Create one ABAC policy per group — auto-applies to all tagged tables
# MAGIC
# MAGIC ## Best practices (from docs)
# MAGIC - UDF does NOT call `IS_ACCOUNT_GROUP_MEMBER()` or `current_user()`
# MAGIC - UDF only transforms/filters data using its parameters
# MAGIC - Policy defines WHO (principals/groups) and WHEN (tags) the UDF applies
# MAGIC
# MAGIC See: https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/udf-best-practices

# COMMAND ----------

# MAGIC %run ../generators/config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tag the `group_id` column on Gold tables

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
# MAGIC ## 2. Create the row filter UDF
# MAGIC
# MAGIC Simple, deterministic: takes the row's group_id and a list of allowed groups.
# MAGIC Returns TRUE if the row should be visible.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA_CAR_SALES}.filter_by_concession_group(
    row_group_id STRING,
    allowed_groups ARRAY<STRING>
)
RETURNS BOOLEAN
DETERMINISTIC
COMMENT 'Row filter: returns TRUE if row_group_id is in the allowed groups list'
RETURN array_contains(allowed_groups, row_group_id)
""")

print("Row filter UDF created (deterministic, no side effects)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create ABAC policies — one per concession group
# MAGIC
# MAGIC Each policy:
# MAGIC - Applies to a specific IdP group (e.g., `groupe_bernard`)
# MAGIC - Passes the allowed group IDs to the UDF
# MAGIC - Auto-applies to any table with `concession_group` tagged column

# COMMAND ----------

# Map: IdP group name → allowed concession group IDs
# In production, these IdP groups come from Entra ID / Okta via SCIM
GROUP_POLICIES = [
    {"idp_group": "groupe_bernard",   "allowed": ["GRP-01"], "label": "Groupe Bernard"},
    {"idp_group": "groupe_gueudet",   "allowed": ["GRP-02"], "label": "Groupe Gueudet"},
    {"idp_group": "groupe_jeanlain",  "allowed": ["GRP-03"], "label": "Groupe Jean Lain"},
    {"idp_group": "groupe_bodemer",   "allowed": ["GRP-04"], "label": "Groupe Bodemer"},
    {"idp_group": "groupe_dubreuil",  "allowed": ["GRP-05"], "label": "Groupe Dubreuil"},
    {"idp_group": "groupe_mary",      "allowed": ["GRP-06"], "label": "Groupe Mary"},
    {"idp_group": "groupe_parot",     "allowed": ["GRP-07"], "label": "Groupe Parot"},
    {"idp_group": "groupe_claro",     "allowed": ["GRP-08"], "label": "Groupe Claro"},
]

abac_success = False
for gp in GROUP_POLICIES:
    allowed_sql = "ARRAY(" + ", ".join(f"'{g}'" for g in gp["allowed"]) + ")"
    policy_name = f"rls_{gp['idp_group']}"
    try:
        spark.sql(f"""
        CREATE OR REPLACE POLICY {policy_name}
        ON SCHEMA {CATALOG}.{SCHEMA_CAR_SALES}
        COMMENT 'RLS for {gp["label"]}: only sees {", ".join(gp["allowed"])}'
        ROW FILTER {CATALOG}.{SCHEMA_CAR_SALES}.filter_by_concession_group
        TO `{gp["idp_group"]}`
        FOR TABLES
        MATCH COLUMNS has_tag_value('concession_group', 'group_id') AS grp
        USING COLUMNS (grp, {allowed_sql})
        """)
        print(f"Policy {policy_name} created for `{gp['idp_group']}` → {gp['allowed']}")
        abac_success = True
    except Exception as e:
        print(f"Policy {policy_name} failed: {e}")
        break

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3b. Fallback: manual row filters (if ABAC not supported)
# MAGIC
# MAGIC Uses a simpler UDF with a mapping table instead of ABAC policies.

# COMMAND ----------

if not abac_success:
    print("ABAC policies not supported on this workspace — using manual row filters\n")

    # Create mapping table
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping (
      user_or_group STRING,
      group_id STRING,
      group_name STRING,
      is_admin BOOLEAN
    ) USING DELTA
    """)

    sample_mappings = [
        ("admin@renault.com", None, None, True),
    ] + [
        (gp["idp_group"], gp["allowed"][0], gp["label"], False)
        for gp in GROUP_POLICIES
    ]
    df = spark.createDataFrame(sample_mappings, ["user_or_group", "group_id", "group_name", "is_admin"])
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping")
    print(f"Mapping table: {len(sample_mappings)} entries")

    # Create simple row filter UDF
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
    print("Row filter UDF created")

    # Apply to Gold materialized views
    # Gold tables from DP are materialized views — must use ALTER MATERIALIZED VIEW, not ALTER TABLE
    for table in gold_tables_with_rls:
        fqn = f"{CATALOG}.{SCHEMA_CAR_SALES}.{table}"
        spark.sql(f"ALTER MATERIALIZED VIEW {fqn} SET ROW FILTER {CATALOG}.{SCHEMA_CAR_SALES}.rls_filter ON (group_id)")
        print(f"Row filter applied to {fqn}")

    # Add current user as admin so they can see all data
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
    spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping
    VALUES ('{current_user}', NULL, NULL, TRUE)
    """)
    print(f"\nAdded {current_user} as admin")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify RLS

# COMMAND ----------

current_user = spark.sql("SELECT current_user()").collect()[0][0]
print(f"Current user: {current_user}")

# ─── Test 1: Admin sees ALL groups ───
total_admin = spark.sql(f"SELECT count(*) FROM {CATALOG}.{SCHEMA_CAR_SALES}.listings_detail").collect()[0][0]
groups_admin = spark.sql(f"SELECT count(distinct group_id) FROM {CATALOG}.{SCHEMA_CAR_SALES}.listings_detail").collect()[0][0]
print(f"\n[TEST 1] As admin: {total_admin:,} rows across {groups_admin} groups")
assert groups_admin == 8, f"Admin should see 8 groups, got {groups_admin}"

# ─── Test 2: Switch to GRP-01 — should see ONLY GRP-01 ───
spark.sql(f"DELETE FROM {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping WHERE user_or_group = '{current_user}'")
spark.sql(f"INSERT INTO {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping VALUES ('{current_user}', 'GRP-01', 'Groupe Bernard', FALSE)")

total_grp01 = spark.sql(f"SELECT count(*) FROM {CATALOG}.{SCHEMA_CAR_SALES}.listings_detail").collect()[0][0]
groups_grp01 = spark.sql(f"SELECT count(distinct group_id) FROM {CATALOG}.{SCHEMA_CAR_SALES}.listings_detail").collect()[0][0]
print(f"[TEST 2] As GRP-01: {total_grp01:,} rows across {groups_grp01} group(s)")
assert groups_grp01 == 1, f"GRP-01 user should see 1 group, got {groups_grp01}"
assert total_grp01 < total_admin, f"GRP-01 should see fewer rows than admin"

# ─── Test 3: Switch to GRP-02 — should see ONLY GRP-02 ───
spark.sql(f"DELETE FROM {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping WHERE user_or_group = '{current_user}'")
spark.sql(f"INSERT INTO {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping VALUES ('{current_user}', 'GRP-02', 'Groupe Gueudet', FALSE)")

total_grp02 = spark.sql(f"SELECT count(*) FROM {CATALOG}.{SCHEMA_CAR_SALES}.listings_detail").collect()[0][0]
groups_grp02 = spark.sql(f"SELECT count(distinct group_id) FROM {CATALOG}.{SCHEMA_CAR_SALES}.listings_detail").collect()[0][0]
print(f"[TEST 3] As GRP-02: {total_grp02:,} rows across {groups_grp02} group(s)")
assert groups_grp02 == 1, f"GRP-02 user should see 1 group, got {groups_grp02}"
assert total_grp02 != total_grp01, f"GRP-01 and GRP-02 should see different row counts"

# ─── Restore admin ───
spark.sql(f"DELETE FROM {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping WHERE user_or_group = '{current_user}'")
spark.sql(f"INSERT INTO {CATALOG}.{SCHEMA_CAR_SALES}.user_group_mapping VALUES ('{current_user}', NULL, NULL, TRUE)")

total_restored = spark.sql(f"SELECT count(*) FROM {CATALOG}.{SCHEMA_CAR_SALES}.listings_detail").collect()[0][0]
print(f"[TEST 4] Admin restored: {total_restored:,} rows")
assert total_restored == total_admin, f"Admin should see same rows as before"

print(f"\n{'='*60}")
print(f"  RLS VERIFIED")
print(f"{'='*60}")
print(f"  Admin:  {total_admin:,} rows (8 groups)")
print(f"  GRP-01: {total_grp01:,} rows (1 group)")
print(f"  GRP-02: {total_grp02:,} rows (1 group)")
print(f"  All assertions passed")

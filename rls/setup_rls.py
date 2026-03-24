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
    except Exception as e:
        print(f"Policy {policy_name} failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify setup

# COMMAND ----------

current_user = spark.sql("SELECT current_user()").collect()[0][0]
print(f"Current user: {current_user}")
print(f"\nABAC policies created for {len(GROUP_POLICIES)} concession groups:")
for gp in GROUP_POLICIES:
    print(f"  `{gp['idp_group']}` → sees {gp['allowed']}")
print(f"\nAdmins / workspace owners see all data (not matched by any policy)")
print(f"\nTagged tables:")
for t in gold_tables_with_rls:
    print(f"  {CATALOG}.{SCHEMA_CAR_SALES}.{t}")
print(f"\nTo test: add your user to one of the IdP groups above,")
print(f"  then query a Gold table — only matching rows are returned.")

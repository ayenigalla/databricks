# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks System Tables — Pipeline Costing Reference
# MAGIC
# MAGIC A working set of SQL queries (plus Python to run them programmatically) for pulling real
# MAGIC pipeline/job cost data out of Databricks' Unity Catalog system schemas. Every query cell
# MAGIC below is read-only — safe to run as-is against your workspace.
# MAGIC
# MAGIC **How to use this notebook:** set the widgets at the top — **Pipeline ID** (swap in your own),
# MAGIC **Pipeline Run/Update ID** (optional — leave blank to see all runs), and **Run Date** (optional —
# MAGIC leave blank and every query defaults to the last 30 days through today). Then run cells top to
# MAGIC bottom; every `%sql` cell below picks the widgets up automatically via `$pipeline_id`,
# MAGIC `$update_id`, `$effective_start_date` and `$effective_end_date`.

# COMMAND ----------

# Widgets so you only set these once — every %sql cell below reads them via $variable
dbutils.widgets.text("pipeline_id", "00732f83-cd59-4c76-ac0d-57958532ab5b", "Pipeline ID")
dbutils.widgets.text("update_id", "", "Pipeline Run/Update ID (optional — blank = all runs)")
dbutils.widgets.text("run_date", "", "Run Date, YYYY-MM-DD (optional — blank = last 30 days)")

# COMMAND ----------

# Resolve the date range: a specific run_date means "just that day";
# leaving it blank means "last 30 days through today". Re-run this cell
# any time you change the run_date widget above.
from datetime import date, timedelta

run_date_input = dbutils.widgets.get("run_date").strip()
if run_date_input:
    effective_start_date = run_date_input
    effective_end_date = run_date_input
else:
    effective_start_date = (date.today() - timedelta(days=30)).isoformat()
    effective_end_date = date.today().isoformat()

dbutils.widgets.text("effective_start_date", effective_start_date, "Effective Start Date (resolved)")
dbutils.widgets.text("effective_end_date", effective_end_date, "Effective End Date (resolved)")

print(f"Using date range: {effective_start_date} to {effective_end_date}"
      + (" (explicit run_date)" if run_date_input else " (default: last 30 days)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Tables you'll use
# MAGIC
# MAGIC | Table | What it gives you |
# MAGIC |---|---|
# MAGIC | `system.billing.usage` | The core ledger — one row per resource per hour: DBUs consumed, SKU, and a `usage_metadata` struct identifying *what* consumed them (cluster, job, pipeline, warehouse, node type). |
# MAGIC | `system.billing.list_prices` | `$/DBU` by SKU, with `price_start_time`/`price_end_time` validity windows (rates change over time). |
# MAGIC | `system.lakeflow.pipelines` | Pipeline metadata (name, catalog/schema, creator). **SCD2** — history table, needs a "latest version" pattern. |
# MAGIC | `system.lakeflow.pipeline_update_timeline` | One row per pipeline update (run): duration, result, trigger type. |
# MAGIC | `system.lakeflow.jobs` / `job_tasks` | Job/task metadata (SCD2, same pattern as pipelines). |
# MAGIC | `system.lakeflow.job_run_timeline` / `job_task_run_timeline` | Run-level duration and outcome for jobs/tasks. |
# MAGIC | `system.compute.clusters` | Cluster configs — driver/worker node types, autoscale bounds. |
# MAGIC
# MAGIC **Prerequisite:** these are Unity Catalog system schemas — an account admin has to enable
# MAGIC `billing`, `lakeflow`, and `compute` under the `system` catalog (Catalog Explorer → System, or
# MAGIC the System Tables API) before any of this returns rows. `pipelines` and `pipeline_update_timeline`
# MAGIC are newer additions and may need enabling separately. Field availability varies slightly by cloud
# MAGIC (AWS/Azure/GCP) — if a query below returns nothing, check enablement first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Core usage & cost queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Raw DBU usage for one pipeline
# MAGIC Respects all three widgets: filters to `update_id` if you set one, and to the resolved
# MAGIC date range (`run_date`, or last 30 days if you left it blank) either way.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   sku_name,
# MAGIC   usage_metadata.dlt_pipeline_id AS pipeline_id,
# MAGIC   usage_metadata.dlt_update_id   AS update_id,
# MAGIC   usage_metadata.node_type       AS node_type,
# MAGIC   usage_quantity                 AS dbus
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_metadata.dlt_pipeline_id = '$pipeline_id'
# MAGIC   AND ('$update_id' = '' OR usage_metadata.dlt_update_id = '$update_id')
# MAGIC   AND usage_date BETWEEN '$effective_start_date' AND '$effective_end_date'
# MAGIC ORDER BY usage_date

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Actual $ cost for that pipeline (join to list price)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   u.usage_date,
# MAGIC   u.sku_name,
# MAGIC   SUM(u.usage_quantity)                              AS total_dbus,
# MAGIC   SUM(u.usage_quantity * lp.pricing.default)          AS total_cost_usd
# MAGIC FROM system.billing.usage u
# MAGIC JOIN system.billing.list_prices lp
# MAGIC   ON u.sku_name = lp.sku_name
# MAGIC  AND u.cloud = lp.cloud
# MAGIC  AND u.usage_start_time >= lp.price_start_time
# MAGIC  AND (u.usage_start_time < lp.price_end_time OR lp.price_end_time IS NULL)
# MAGIC WHERE u.usage_metadata.dlt_pipeline_id = '$pipeline_id'
# MAGIC   AND ('$update_id' = '' OR u.usage_metadata.dlt_update_id = '$update_id')
# MAGIC   AND u.usage_date BETWEEN '$effective_start_date' AND '$effective_end_date'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY u.usage_date

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Every pipeline, ranked by cost (last 30 days)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest_pipelines AS (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time DESC) AS rn
# MAGIC   FROM system.lakeflow.pipelines
# MAGIC   QUALIFY rn = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   COALESCE(p.name, u.usage_metadata.dlt_pipeline_id) AS pipeline_name,
# MAGIC   u.usage_metadata.dlt_pipeline_id                    AS pipeline_id,
# MAGIC   ROUND(SUM(u.usage_quantity), 1)                     AS total_dbus,
# MAGIC   ROUND(SUM(u.usage_quantity * lp.pricing.default), 2) AS total_cost_usd
# MAGIC FROM system.billing.usage u
# MAGIC JOIN system.billing.list_prices lp
# MAGIC   ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
# MAGIC  AND u.usage_start_time >= lp.price_start_time
# MAGIC  AND (u.usage_start_time < lp.price_end_time OR lp.price_end_time IS NULL)
# MAGIC LEFT JOIN latest_pipelines p
# MAGIC   ON u.workspace_id = p.workspace_id AND u.usage_metadata.dlt_pipeline_id = p.pipeline_id
# MAGIC WHERE u.usage_metadata.dlt_pipeline_id IS NOT NULL
# MAGIC   AND u.usage_date BETWEEN '\$effective_start_date' AND '\$effective_end_date'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY total_cost_usd DESC
# MAGIC LIMIT 25

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Daily/monthly cost trend for one pipeline

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DATE_TRUNC('MONTH', u.usage_date) AS month,
# MAGIC   ROUND(SUM(u.usage_quantity * lp.pricing.default), 2) AS monthly_cost_usd
# MAGIC FROM system.billing.usage u
# MAGIC JOIN system.billing.list_prices lp
# MAGIC   ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
# MAGIC  AND u.usage_start_time >= lp.price_start_time
# MAGIC  AND (u.usage_start_time < lp.price_end_time OR lp.price_end_time IS NULL)
# MAGIC WHERE u.usage_metadata.dlt_pipeline_id = '$pipeline_id'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Cost broken out by node/instance type (for right-sizing)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   u.usage_metadata.dlt_pipeline_id AS pipeline_id,
# MAGIC   u.usage_metadata.node_type       AS node_type,
# MAGIC   SUM(u.usage_quantity)            AS total_dbus,
# MAGIC   SUM(DATEDIFF(SECOND, u.usage_start_time, u.usage_end_time)) / 3600.0 AS total_node_hours,
# MAGIC   SUM(u.usage_quantity) / NULLIF(SUM(DATEDIFF(SECOND, u.usage_start_time, u.usage_end_time)) / 3600.0, 0) AS dbu_per_hour
# MAGIC FROM system.billing.usage u
# MAGIC WHERE u.usage_metadata.dlt_pipeline_id IS NOT NULL
# MAGIC   AND u.usage_date BETWEEN '\$effective_start_date' AND '\$effective_end_date'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 Cost by destination table (materialized view / streaming table)
# MAGIC Useful when one pipeline writes many tables and you want per-table attribution.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   usage_metadata.uc_table_catalog AS catalog,
# MAGIC   usage_metadata.uc_table_schema  AS schema,
# MAGIC   usage_metadata.uc_table_name    AS table_name,
# MAGIC   ROUND(SUM(usage_quantity), 1)   AS total_dbus
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_metadata.uc_table_name IS NOT NULL
# MAGIC   AND usage_date BETWEEN '\$effective_start_date' AND '\$effective_end_date'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.7 Pipeline run history — duration, result, trigger type
# MAGIC Set `update_id` to drill into one specific run; leave it blank to see every run in the
# MAGIC resolved date range.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   pipeline_id,
# MAGIC   update_id,
# MAGIC   start_time,
# MAGIC   end_time,
# MAGIC   DATEDIFF(SECOND, start_time, end_time) / 60.0 AS duration_minutes,
# MAGIC   result_state,
# MAGIC   trigger_type
# MAGIC FROM system.lakeflow.pipeline_update_timeline
# MAGIC WHERE pipeline_id = '$pipeline_id'
# MAGIC   AND ('$update_id' = '' OR update_id = '$update_id')
# MAGIC   AND DATE(start_time) BETWEEN '$effective_start_date' AND '$effective_end_date'
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.8 Join cost to run duration — $ per update, and $/minute
# MAGIC Also respects `update_id` (blank = every update) and the resolved date range.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH updates AS (
# MAGIC   SELECT pipeline_id, update_id, start_time, end_time, result_state
# MAGIC   FROM system.lakeflow.pipeline_update_timeline
# MAGIC   WHERE ('$update_id' = '' OR update_id = '$update_id')
# MAGIC     AND DATE(start_time) BETWEEN '$effective_start_date' AND '$effective_end_date'
# MAGIC ),
# MAGIC costs AS (
# MAGIC   SELECT
# MAGIC     u.usage_metadata.dlt_pipeline_id AS pipeline_id,
# MAGIC     u.usage_metadata.dlt_update_id   AS update_id,
# MAGIC     SUM(u.usage_quantity * lp.pricing.default) AS cost_usd
# MAGIC   FROM system.billing.usage u
# MAGIC   JOIN system.billing.list_prices lp
# MAGIC     ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
# MAGIC    AND u.usage_start_time >= lp.price_start_time
# MAGIC    AND (u.usage_start_time < lp.price_end_time OR lp.price_end_time IS NULL)
# MAGIC   WHERE u.usage_metadata.dlt_update_id IS NOT NULL
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC SELECT
# MAGIC   upd.pipeline_id, upd.update_id, upd.result_state,
# MAGIC   DATEDIFF(SECOND, upd.start_time, upd.end_time) / 60.0 AS duration_min,
# MAGIC   c.cost_usd,
# MAGIC   c.cost_usd / NULLIF(DATEDIFF(SECOND, upd.start_time, upd.end_time) / 60.0, 0) AS cost_per_minute
# MAGIC FROM updates upd
# MAGIC JOIN costs c ON upd.pipeline_id = c.pipeline_id AND upd.update_id = c.update_id
# MAGIC ORDER BY upd.start_time DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.9 Non-pipeline jobs — cost by job

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest_jobs AS (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
# MAGIC   FROM system.lakeflow.jobs
# MAGIC   QUALIFY rn = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   COALESCE(j.name, u.usage_metadata.job_id) AS job_name,
# MAGIC   ROUND(SUM(u.usage_quantity * lp.pricing.default), 2) AS total_cost_usd
# MAGIC FROM system.billing.usage u
# MAGIC JOIN system.billing.list_prices lp
# MAGIC   ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
# MAGIC  AND u.usage_start_time >= lp.price_start_time
# MAGIC  AND (u.usage_start_time < lp.price_end_time OR lp.price_end_time IS NULL)
# MAGIC LEFT JOIN latest_jobs j
# MAGIC   ON u.workspace_id = j.workspace_id AND u.usage_metadata.job_id = j.job_id
# MAGIC WHERE u.usage_metadata.job_id IS NOT NULL
# MAGIC   AND u.usage_date BETWEEN '\$effective_start_date' AND '\$effective_end_date'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY total_cost_usd DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.10 Tag-based cost allocation (chargeback by team/project)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   custom_tags['team']    AS team,
# MAGIC   custom_tags['project']  AS project,
# MAGIC   ROUND(SUM(usage_quantity * lp.pricing.default), 2) AS total_cost_usd
# MAGIC FROM system.billing.usage u
# MAGIC JOIN system.billing.list_prices lp
# MAGIC   ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
# MAGIC  AND u.usage_start_time >= lp.price_start_time
# MAGIC  AND (u.usage_start_time < lp.price_end_time OR lp.price_end_time IS NULL)
# MAGIC WHERE u.usage_date BETWEEN '\$effective_start_date' AND '\$effective_end_date'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY total_cost_usd DESC

# COMMAND ----------

# MAGIC %md
# MAGIC This only works if clusters/pipelines are actually tagged — tag enforcement (cluster policies requiring `team`/`project` tags) is what makes this reliable.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.11 Serverless vs. classic compute split

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE WHEN sku_name LIKE '%SERVERLESS%' THEN 'Serverless' ELSE 'Classic' END AS compute_type,
# MAGIC   ROUND(SUM(usage_quantity * lp.pricing.default), 2) AS total_cost_usd
# MAGIC FROM system.billing.usage u
# MAGIC JOIN system.billing.list_prices lp
# MAGIC   ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
# MAGIC  AND u.usage_start_time >= lp.price_start_time
# MAGIC  AND (u.usage_start_time < lp.price_end_time OR lp.price_end_time IS NULL)
# MAGIC WHERE u.usage_metadata.dlt_pipeline_id IS NOT NULL
# MAGIC   AND u.usage_date BETWEEN '\$effective_start_date' AND '\$effective_end_date'
# MAGIC GROUP BY ALL

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.12 Handling corrections (retraction/restatement rows)
# MAGIC Databricks occasionally emits a negative-quantity **retraction** row plus a corrected
# MAGIC **restatement** row for a past billing period. Summing naively is usually fine, but if you
# MAGIC group at fine granularity (e.g. per hour) and see a zeroed-out row, that's why:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   usage_metadata.job_id,
# MAGIC   usage_start_time,
# MAGIC   usage_end_time,
# MAGIC   SUM(usage_quantity) AS usage_quantity
# MAGIC FROM system.billing.usage
# MAGIC GROUP BY ALL
# MAGIC HAVING usage_quantity != 0  -- drops fully-netted correction pairs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Turning this into a live dashboard
# MAGIC
# MAGIC Databricks' own recommendation is to build an **AI/BI Dashboard** on top of these queries
# MAGIC rather than re-running ad hoc SQL each time — point it at `system.billing.usage` +
# MAGIC `system.billing.list_prices` + `system.lakeflow.pipelines`, and it'll stay current
# MAGIC automatically as new usage rows land (usage is typically available within a few hours).
# MAGIC Queries 1.3 and 1.4 above are good starting tiles.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Running these programmatically (Python)
# MAGIC
# MAGIC The cell above already shows the native way (`%sql` cells / widgets) — the three patterns
# MAGIC below are for when you need to run the same queries *from outside* this notebook (a
# MAGIC scheduled script, a CI job, an external app).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Databricks SQL Connector — run + export to CSV (external script)

# COMMAND ----------

from databricks import sql
import pandas as pd

QUERY = '''
SELECT usage_metadata.dlt_pipeline_id AS pipeline_id,
       usage_date,
       SUM(usage_quantity) AS total_dbus
FROM system.billing.usage
WHERE usage_metadata.dlt_pipeline_id IS NOT NULL
  AND usage_date BETWEEN '\$effective_start_date' AND '\$effective_end_date'
GROUP BY ALL
'''

with sql.connect(
    server_hostname="<workspace-hostname>",
    http_path="<sql-warehouse-http-path>",
    access_token="<personal-access-token-or-oauth-token>",
) as conn:
    with conn.cursor() as cur:
        cur.execute(QUERY)
        df = cur.fetchall_arrow().to_pandas()

df.to_csv("pipeline_dbu_usage.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Databricks SDK — statement execution API (no external connector needed)

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()  # picks up auth from the Databricks CLI config / env vars

result = w.statement_execution.execute_statement(
    warehouse_id="<sql-warehouse-id>",
    statement='''
        SELECT usage_metadata.dlt_pipeline_id AS pipeline_id,
               SUM(usage_quantity * lp.pricing.default) AS cost_usd
        FROM system.billing.usage u
        JOIN system.billing.list_prices lp
          ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
         AND u.usage_start_time >= lp.price_start_time
         AND (u.usage_start_time < lp.price_end_time OR lp.price_end_time IS NULL)
        WHERE u.usage_metadata.dlt_pipeline_id IS NOT NULL
        GROUP BY ALL
    ''',
)
for row in result.result.data_array:
    print(row)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 PySpark — native in this notebook (this is what `%sql` cells above compile down to)

# COMMAND ----------

df = spark.sql('''
    SELECT usage_metadata.dlt_pipeline_id AS pipeline_id,
           usage_date,
           SUM(usage_quantity) AS total_dbus
    FROM system.billing.usage
    WHERE usage_metadata.dlt_pipeline_id IS NOT NULL
      AND usage_date >= date_sub(current_date(), 90)
    GROUP BY ALL
''')
display(df)
df.toPandas().to_csv("/dbfs/tmp/pipeline_dbu_usage.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. SCD Type 1 — both senses
# MAGIC
# MAGIC "SCD1" shows up in two different places here, so both are covered.
# MAGIC
# MAGIC ### 4.1 Querying SCD1-style system tables (simpler than the SCD2 pattern in §1)
# MAGIC `system.lakeflow.pipelines`, `jobs`, and `job_tasks` are **SCD2** — every change appends a new
# MAGIC row, so you need the `ROW_NUMBER() ... QUALIFY rn=1` pattern from §1.3/§1.9 to get the current
# MAGIC version. Fact/log tables like `system.billing.usage` and `system.lakeflow.pipeline_update_timeline`
# MAGIC behave more like **SCD1** in effect — there's no "current version" to pick out, you just query
# MAGIC them directly (each row is already a discrete event, not a row that gets superseded):

# COMMAND ----------

# MAGIC %sql
# MAGIC -- No dedup needed — every row is a distinct, final event
# MAGIC SELECT pipeline_id, update_id, start_time, end_time, result_state
# MAGIC FROM system.lakeflow.pipeline_update_timeline
# MAGIC WHERE pipeline_id = '$pipeline_id'
# MAGIC   AND ('$update_id' = '' OR update_id = '$update_id')
# MAGIC   AND DATE(start_time) BETWEEN '$effective_start_date' AND '$effective_end_date'
# MAGIC ORDER BY start_time DESC

# COMMAND ----------

# MAGIC %md
# MAGIC If you're ever unsure whether a system table needs the dedup pattern, check for a `change_time`/`delete_time` pair — that's the SCD2 tell. If those columns don't exist, treat it as SCD1/append-only and skip the `QUALIFY`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Implementing SCD Type 1 in your own Bronze → Silver pipeline
# MAGIC If the question is really about how *your* Silver merge should be built (overwrite-in-place
# MAGIC rather than versioned history), Lakeflow Declarative Pipelines does this natively with
# MAGIC `AUTO CDC INTO`:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE silver_customers;
# MAGIC
# MAGIC AUTO CDC INTO silver_customers
# MAGIC FROM STREAM(bronze_customers)
# MAGIC KEYS (customer_id)
# MAGIC SEQUENCE BY updated_at
# MAGIC STORED AS SCD TYPE 1

# COMMAND ----------

# Python (PySpark) equivalent, inside a Lakeflow Declarative Pipeline
import dlt

dlt.create_streaming_table("silver_customers")

dlt.create_auto_cdc_flow(
    target="silver_customers",
    source="bronze_customers",
    keys=["customer_id"],
    sequence_by="updated_at",
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC SCD Type 1 keeps only the latest value per key (no history) — cheapest to store and query,
# MAGIC right choice when Silver only needs to reflect current state. Swap `stored_as_scd_type=1` for
# MAGIC `2` (or `STORED AS SCD TYPE 2`) if you need to preserve the change history instead — that's a
# MAGIC one-line switch, no pipeline redesign required. Worth noting for the cost model: SCD1 merges
# MAGIC are typically cheaper per run than SCD2, since there's no extra history-row bookkeeping — if
# MAGIC your Silver layer uses SCD1, the "Avg delta volume per run" input in the B2S sizing sheet can
# MAGIC reasonably stay on the low end.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Caveats worth knowing before you trust the numbers
# MAGIC
# MAGIC - **`pipelines` and `pipeline_update_timeline` are newer/Public-Preview-stage tables** in some
# MAGIC   accounts — confirm they're enabled and populated before relying on the joins in §1.3/1.7/1.8.
# MAGIC - **SCD2 tables** (`pipelines`, `jobs`, `job_tasks`) keep full change history — always filter to
# MAGIC   the latest row per entity (the `ROW_NUMBER() ... QUALIFY rn=1` pattern used above) or you'll
# MAGIC   double-count.
# MAGIC - **`usage_metadata` is sparsely populated** — which fields are non-null depends on the
# MAGIC   SKU/compute type (a serverless pipeline populates `dlt_pipeline_id`; an all-purpose cluster
# MAGIC   populates `cluster_id`; a SQL warehouse populates `warehouse_id`). Don't assume every row has
# MAGIC   every field.
# MAGIC - **Corrections happen** — see §1.12. Rare, but can cause negative or duplicate-looking rows in
# MAGIC   raw exports.
# MAGIC - **Field/table names can vary slightly by cloud** (AWS/Azure/GCP) and change over Databricks
# MAGIC   Runtime versions — if a query errors on a missing column, check your workspace's actual
# MAGIC   `system.billing.usage` schema with `DESCRIBE TABLE system.billing.usage` first.
# MAGIC - **This only prices the DBU platform fee.** The underlying cloud VM cost is billed separately
# MAGIC   by AWS/Azure/GCP (Cost & Usage Report / Cost Management export) — join on cluster tags or
# MAGIC   timestamps if you want one blended number.

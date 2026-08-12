# Databricks System Tables — Pipeline Costing Reference

A working set of SQL queries (plus Python to run them programmatically) for pulling real pipeline/job cost data out of Databricks' Unity Catalog system schemas. Everything here is read-only SQL against system tables — safe to run in a SQL editor or notebook.

---

## 0. Tables you'll use

| Table | What it gives you |
|---|---|
| `system.billing.usage` | The core ledger — one row per resource per hour: DBUs consumed, SKU, and a `usage_metadata` struct identifying *what* consumed them (cluster, job, pipeline, warehouse, node type). |
| `system.billing.list_prices` | `$/DBU` by SKU, with `price_start_time`/`price_end_time` validity windows (rates change over time). |
| `system.lakeflow.pipelines` | Pipeline metadata (name, catalog/schema, creator). **SCD2** — history table, needs a "latest version" pattern. |
| `system.lakeflow.pipeline_update_timeline` | One row per pipeline update (run): duration, result, trigger type. |
| `system.lakeflow.jobs` / `job_tasks` | Job/task metadata (SCD2, same pattern as pipelines). |
| `system.lakeflow.job_run_timeline` / `job_task_run_timeline` | Run-level duration and outcome for jobs/tasks. |
| `system.compute.clusters` | Cluster configs — driver/worker node types, autoscale bounds. |

**Prerequisite:** these are Unity Catalog system schemas — an account admin has to enable `billing`, `lakeflow`, and `compute` under the `system` catalog (Catalog Explorer → System, or the System Tables API) before any of this returns rows. `pipelines` and `pipeline_update_timeline` are newer additions and may need enabling separately. Field availability and exact enablement steps vary slightly by cloud (AWS/Azure/GCP) — if a query below returns nothing, check enablement first.

---

## 1. Core usage & cost queries

### 1.1 Raw DBU usage for one pipeline
```sql
SELECT
  usage_date,
  sku_name,
  usage_metadata.dlt_pipeline_id AS pipeline_id,
  usage_metadata.dlt_update_id   AS update_id,
  usage_metadata.node_type       AS node_type,
  usage_quantity                 AS dbus
FROM system.billing.usage
WHERE usage_metadata.dlt_pipeline_id = '00732f83-cd59-4c76-ac0d-57958532ab5b'  -- from the pipeline's Details tab
  AND usage_date >= DATE_SUB(CURRENT_DATE(), 90)
ORDER BY usage_date;
```

### 1.2 Actual $ cost for that pipeline (join to list price)
```sql
SELECT
  u.usage_date,
  u.sku_name,
  SUM(u.usage_quantity)                              AS total_dbus,
  SUM(u.usage_quantity * lp.pricing.default)          AS total_cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices lp
  ON u.sku_name = lp.sku_name
 AND u.cloud = lp.cloud
 AND u.usage_start_time >= lp.price_start_time
 AND (u.usage_start_time < lp.price_end_time OR lp.price_end_time IS NULL)
WHERE u.usage_metadata.dlt_pipeline_id = '00732f83-cd59-4c76-ac0d-57958532ab5b'
  AND u.usage_date >= DATE_SUB(CURRENT_DATE(), 90)
GROUP BY ALL
ORDER BY u.usage_date;
```

### 1.3 Every pipeline, ranked by cost (last 30 days)
```sql
WITH latest_pipelines AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time DESC) AS rn
  FROM system.lakeflow.pipelines
  QUALIFY rn = 1
)
SELECT
  COALESCE(p.name, u.usage_metadata.dlt_pipeline_id) AS pipeline_name,
  u.usage_metadata.dlt_pipeline_id                    AS pipeline_id,
  ROUND(SUM(u.usage_quantity), 1)                     AS total_dbus,
  ROUND(SUM(u.usage_quantity * lp.pricing.default), 2) AS total_cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices lp
  ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
 AND u.usage_start_time >= lp.price_start_time
 AND (u.usage_start_time < lp.price_end_time OR lp.price_end_time IS NULL)
LEFT JOIN latest_pipelines p
  ON u.workspace_id = p.workspace_id AND u.usage_metadata.dlt_pipeline_id = p.pipeline_id
WHERE u.usage_metadata.dlt_pipeline_id IS NOT NULL
  AND u.usage_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY ALL
ORDER BY total_cost_usd DESC
LIMIT 25;
```

### 1.4 Daily/monthly cost trend for one pipeline
```sql
SELECT
  DATE_TRUNC('MONTH', u.usage_date) AS month,
  ROUND(SUM(u.usage_quantity * lp.pricing.default), 2) AS monthly_cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices lp
  ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
 AND u.usage_start_time >= lp.price_start_time
 AND (u.usage_start_time < lp.price_end_time OR lp.price_end_time IS NULL)
WHERE u.usage_metadata.dlt_pipeline_id = '00732f83-cd59-4c76-ac0d-57958532ab5b'
GROUP BY 1
ORDER BY 1;
```

### 1.5 Cost broken out by node/instance type (for right-sizing)
```sql
SELECT
  u.usage_metadata.dlt_pipeline_id AS pipeline_id,
  u.usage_metadata.node_type       AS node_type,
  SUM(u.usage_quantity)            AS total_dbus,
  SUM(DATEDIFF(SECOND, u.usage_start_time, u.usage_end_time)) / 3600.0 AS total_node_hours,
  SUM(u.usage_quantity) / NULLIF(SUM(DATEDIFF(SECOND, u.usage_start_time, u.usage_end_time)) / 3600.0, 0) AS dbu_per_hour
FROM system.billing.usage u
WHERE u.usage_metadata.dlt_pipeline_id IS NOT NULL
  AND u.usage_date >= DATE_SUB(CURRENT_DATE(), 90)
GROUP BY ALL
ORDER BY total_dbus DESC;
```

### 1.6 Cost by destination table (materialized view / streaming table)
Useful when one pipeline writes many tables and you want per-table attribution.
```sql
SELECT
  usage_metadata.uc_table_catalog AS catalog,
  usage_metadata.uc_table_schema  AS schema,
  usage_metadata.uc_table_name    AS table_name,
  ROUND(SUM(usage_quantity), 1)   AS total_dbus
FROM system.billing.usage
WHERE usage_metadata.uc_table_name IS NOT NULL
  AND usage_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY ALL
ORDER BY total_dbus DESC;
```

### 1.7 Pipeline run history — duration, result, trigger type
```sql
SELECT
  pipeline_id,
  update_id,
  start_time,
  end_time,
  DATEDIFF(SECOND, start_time, end_time) / 60.0 AS duration_minutes,
  result_state,
  trigger_type
FROM system.lakeflow.pipeline_update_timeline
WHERE pipeline_id = '00732f83-cd59-4c76-ac0d-57958532ab5b'
ORDER BY start_time DESC
LIMIT 100;
```

### 1.8 Join cost to run duration — $ per update, and $/minute
```sql
WITH updates AS (
  SELECT pipeline_id, update_id, start_time, end_time, result_state
  FROM system.lakeflow.pipeline_update_timeline
),
costs AS (
  SELECT
    u.usage_metadata.dlt_pipeline_id AS pipeline_id,
    u.usage_metadata.dlt_update_id   AS update_id,
    SUM(u.usage_quantity * lp.pricing.default) AS cost_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices lp
    ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
   AND u.usage_start_time >= lp.price_start_time
   AND (u.usage_start_time < lp.price_end_time OR lp.price_end_time IS NULL)
  WHERE u.usage_metadata.dlt_update_id IS NOT NULL
  GROUP BY ALL
)
SELECT
  upd.pipeline_id, upd.update_id, upd.result_state,
  DATEDIFF(SECOND, upd.start_time, upd.end_time) / 60.0 AS duration_min,
  c.cost_usd,
  c.cost_usd / NULLIF(DATEDIFF(SECOND, upd.start_time, upd.end_time) / 60.0, 0) AS cost_per_minute
FROM updates upd
JOIN costs c ON upd.pipeline_id = c.pipeline_id AND upd.update_id = c.update_id
ORDER BY upd.start_time DESC;
```

### 1.9 Non-pipeline jobs — cost by job
```sql
WITH latest_jobs AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
  FROM system.lakeflow.jobs
  QUALIFY rn = 1
)
SELECT
  COALESCE(j.name, u.usage_metadata.job_id) AS job_name,
  ROUND(SUM(u.usage_quantity * lp.pricing.default), 2) AS total_cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices lp
  ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
 AND u.usage_start_time >= lp.price_start_time
 AND (u.usage_start_time < lp.price_end_time OR lp.price_end_time IS NULL)
LEFT JOIN latest_jobs j
  ON u.workspace_id = j.workspace_id AND u.usage_metadata.job_id = j.job_id
WHERE u.usage_metadata.job_id IS NOT NULL
  AND u.usage_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY ALL
ORDER BY total_cost_usd DESC;
```

### 1.10 Tag-based cost allocation (chargeback by team/project)
```sql
SELECT
  custom_tags['team']    AS team,
  custom_tags['project']  AS project,
  ROUND(SUM(usage_quantity * lp.pricing.default), 2) AS total_cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices lp
  ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
 AND u.usage_start_time >= lp.price_start_time
 AND (u.usage_start_time < lp.price_end_time OR lp.price_end_time IS NULL)
WHERE u.usage_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY ALL
ORDER BY total_cost_usd DESC;
```
This only works if clusters/pipelines are actually tagged — tag enforcement (cluster policies requiring `team`/`project` tags) is what makes this reliable.

### 1.11 Serverless vs. classic compute split
```sql
SELECT
  CASE WHEN sku_name LIKE '%SERVERLESS%' THEN 'Serverless' ELSE 'Classic' END AS compute_type,
  ROUND(SUM(usage_quantity * lp.pricing.default), 2) AS total_cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices lp
  ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
 AND u.usage_start_time >= lp.price_start_time
 AND (u.usage_start_time < lp.price_end_time OR lp.price_end_time IS NULL)
WHERE u.usage_metadata.dlt_pipeline_id IS NOT NULL
  AND u.usage_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY ALL;
```

### 1.12 Handling corrections (retraction/restatement rows)
Databricks occasionally emits a negative-quantity **retraction** row plus a corrected **restatement** row for a past billing period. Summing naively is usually fine, but if you group at fine granularity (e.g. per hour) and see a zeroed-out row, that's why:
```sql
SELECT
  usage_metadata.job_id,
  usage_start_time,
  usage_end_time,
  SUM(usage_quantity) AS usage_quantity
FROM system.billing.usage
GROUP BY ALL
HAVING usage_quantity != 0;   -- drops fully-netted correction pairs
```

---

## 2. Turning this into a live dashboard

Databricks' own recommendation is to build an **AI/BI Dashboard** on top of these queries rather than re-running ad hoc SQL each time — point it at `system.billing.usage` + `system.billing.list_prices` + `system.lakeflow.pipelines`, and it'll stay current automatically as new usage rows land (usage is typically available within a few hours). Queries 1.3 and 1.4 above are good starting tiles.

---

## 3. Running these programmatically (Python)

### 3.1 Databricks SQL Connector — run + export to CSV
```python
from databricks import sql
import pandas as pd

QUERY = """
SELECT usage_metadata.dlt_pipeline_id AS pipeline_id,
       usage_date,
       SUM(usage_quantity) AS total_dbus
FROM system.billing.usage
WHERE usage_metadata.dlt_pipeline_id IS NOT NULL
  AND usage_date >= DATE_SUB(CURRENT_DATE(), 90)
GROUP BY ALL
"""

with sql.connect(
    server_hostname="<workspace-hostname>",
    http_path="<sql-warehouse-http-path>",
    access_token="<personal-access-token-or-oauth-token>",
) as conn:
    with conn.cursor() as cur:
        cur.execute(QUERY)
        df = cur.fetchall_arrow().to_pandas()

df.to_csv("pipeline_dbu_usage.csv", index=False)
```

### 3.2 Databricks SDK — statement execution API (no external connector needed)
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()  # picks up auth from the Databricks CLI config / env vars

result = w.statement_execution.execute_statement(
    warehouse_id="<sql-warehouse-id>",
    statement="""
        SELECT usage_metadata.dlt_pipeline_id AS pipeline_id,
               SUM(usage_quantity * lp.pricing.default) AS cost_usd
        FROM system.billing.usage u
        JOIN system.billing.list_prices lp
          ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
         AND u.usage_start_time >= lp.price_start_time
         AND (u.usage_start_time < lp.price_end_time OR lp.price_end_time IS NULL)
        WHERE u.usage_metadata.dlt_pipeline_id IS NOT NULL
        GROUP BY ALL
    """,
)
for row in result.result.data_array:
    print(row)
```

### 3.3 PySpark — inside a Databricks notebook (no external auth needed)
```python
df = spark.sql("""
    SELECT usage_metadata.dlt_pipeline_id AS pipeline_id,
           usage_date,
           SUM(usage_quantity) AS total_dbus
    FROM system.billing.usage
    WHERE usage_metadata.dlt_pipeline_id IS NOT NULL
      AND usage_date >= date_sub(current_date(), 90)
    GROUP BY ALL
""")
display(df)
df.toPandas().to_csv("/dbfs/tmp/pipeline_dbu_usage.csv", index=False)
```

---

## 4. Caveats worth knowing before you trust the numbers

- **`pipelines` and `pipeline_update_timeline` are newer/Public-Preview-stage tables** in some accounts — confirm they're enabled and populated before relying on the joins in §1.3/1.7/1.8.
- **SCD2 tables** (`pipelines`, `jobs`, `job_tasks`) keep full change history — always filter to the latest row per entity (the `ROW_NUMBER() ... QUALIFY rn=1` pattern used above) or you'll double-count.
- **`usage_metadata` is sparsely populated** — which fields are non-null depends on the SKU/compute type (a serverless pipeline populates `dlt_pipeline_id`; an all-purpose cluster populates `cluster_id`; a SQL warehouse populates `warehouse_id`). Don't assume every row has every field.
- **Corrections happen** — see §1.12. Rare, but can cause negative or duplicate-looking rows in raw exports.
- **Field/table names can vary slightly by cloud** (AWS/Azure/GCP) and change over Databricks Runtime versions — if a query errors on a missing column, check your workspace's actual `system.billing.usage` schema with `DESCRIBE TABLE system.billing.usage` first.
- **This only prices the DBU platform fee.** The underlying cloud VM cost is billed separately by AWS/Azure/GCP (Cost & Usage Report / Cost Management export) — join on cluster tags or timestamps if you want one blended number.

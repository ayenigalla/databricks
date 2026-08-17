# Databricks notebook source
# MAGIC %md
# MAGIC # SDP Continuous Pipeline — Event Log Monitoring
# MAGIC
# MAGIC Monitoring notebook for a Databricks SDP (Spark Declarative Pipelines) pipeline
# MAGIC running in **continuous** mode, built on the 14 event types documented in the
# MAGIC companion report (`SDP_Event_Log_Report.docx`).
# MAGIC
# MAGIC **Prerequisites**
# MAGIC - `CAN VIEW` (or higher) permission on the pipeline.
# MAGIC - Ability to query the `event_log()` table-valued function (Unity Catalog pipeline).
# MAGIC
# MAGIC **How to use**
# MAGIC 1. Fill in the `pipeline_id` widget below (find it on the pipeline's *Settings* page,
# MAGIC    or in the URL: `.../pipelines/<pipeline_id>`).
# MAGIC 2. Pick a lookback window for the volume/error sections.
# MAGIC 3. Run All. The "Current status" section always looks at the full history
# MAGIC    (state changes are rare, so a short lookback can miss the last one);
# MAGIC    everything else respects the lookback window.
# MAGIC 4. To monitor continuously, schedule this notebook as a Databricks Job on a
# MAGIC    short interval (e.g. every 15 minutes) — see the note at the bottom.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup

# COMMAND ----------

dbutils.widgets.text("pipeline_id", "", "Pipeline ID")
dbutils.widgets.dropdown(
    "lookback_hours", "24", ["1", "6", "24", "72", "168"], "Lookback window (hours)"
)
dbutils.widgets.text("backlog_seconds_alert", "900", "Alert: backlog age (seconds) >")
dbutils.widgets.text("expectation_fail_rate_alert", "5", "Alert: expectation fail rate (%) >")

pipeline_id = dbutils.widgets.get("pipeline_id")
lookback_hours = int(dbutils.widgets.get("lookback_hours"))
backlog_seconds_alert = int(dbutils.widgets.get("backlog_seconds_alert"))
expectation_fail_rate_alert = float(dbutils.widgets.get("expectation_fail_rate_alert"))

assert pipeline_id, "Set the pipeline_id widget above before running this notebook."
print(f"Monitoring pipeline: {pipeline_id}")
print(f"Lookback window: {lookback_hours}h")

# COMMAND ----------

# Base views: full history (small, state-change queries only) and a windowed
# view (volume / error / backlog queries). Using TABLE(...) works too if you'd
# rather point at a table the pipeline produces:
#   spark.sql(f"CREATE OR REPLACE TEMP VIEW event_log_full AS SELECT * FROM event_log(TABLE(catalog.schema.table))")

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW event_log_full AS
    SELECT * FROM event_log('{pipeline_id}')
""")

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW event_log_recent AS
    SELECT * FROM event_log_full
    WHERE timestamp >= current_timestamp() - INTERVAL {lookback_hours} HOURS
""")

recent_count = spark.sql("SELECT COUNT(*) AS n FROM event_log_recent").collect()[0]["n"]
print(f"{recent_count} events in the last {lookback_hours}h")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Current Pipeline Status
# MAGIC
# MAGIC Looks at full history — `update_progress` state changes are infrequent in a
# MAGIC continuous pipeline (typically only on start/stop/failure), so a short
# MAGIC lookback window can miss the last one.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Most recent update state
# MAGIC SELECT
# MAGIC   origin.update_id,
# MAGIC   timestamp,
# MAGIC   details:update_progress.state::string AS state,
# MAGIC   details:update_progress.cancellation_cause::string AS cancellation_cause
# MAGIC FROM event_log_full
# MAGIC WHERE event_type = 'update_progress'
# MAGIC QUALIFY ROW_NUMBER() OVER (ORDER BY timestamp DESC) = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Most recent status per flow (should mostly read RUNNING / IDLE in continuous mode)
# MAGIC SELECT
# MAGIC   origin.flow_name,
# MAGIC   timestamp AS as_of,
# MAGIC   details:flow_progress.status::string AS status
# MAGIC FROM event_log_full
# MAGIC WHERE event_type = 'flow_progress'
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY origin.flow_name ORDER BY timestamp DESC) = 1
# MAGIC ORDER BY status, flow_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Event Volume by Type
# MAGIC
# MAGIC Counts across all 14 event types over the lookback window — a quick way to
# MAGIC spot something that stopped emitting (usually a bad sign) or started
# MAGIC emitting far more than usual.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_type, level, COUNT(*) AS n
# MAGIC FROM event_log_recent
# MAGIC GROUP BY event_type, level
# MAGIC ORDER BY n DESC

# COMMAND ----------

import matplotlib.pyplot as plt

# Palette: sequential blue for a single-series magnitude-by-category bar chart.
BLUE = "#2a78d6"

counts_df = spark.sql("""
    SELECT event_type, COUNT(*) AS n
    FROM event_log_recent
    GROUP BY event_type
    ORDER BY n DESC
""").toPandas()

fig, ax = plt.subplots(figsize=(9, 5))
bars = ax.barh(counts_df["event_type"], counts_df["n"], color=BLUE)
ax.invert_yaxis()
ax.set_xlabel(f"Events in the last {lookback_hours}h")
ax.set_title("Event volume by type")
ax.spines[["top", "right"]].set_visible(False)
for b in bars:
    ax.text(b.get_width(), b.get_y() + b.get_height() / 2, f" {int(b.get_width())}",
            va="center", fontsize=9, color="#52514e")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Errors & Warnings Feed

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   event_type,
# MAGIC   level,
# MAGIC   origin.flow_name,
# MAGIC   message,
# MAGIC   details
# MAGIC FROM event_log_recent
# MAGIC WHERE level IN ('WARN', 'ERROR')
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 200

# COMMAND ----------

# Status-severity colors (fixed roles — not reused for categorical series)
STATUS_COLORS = {"INFO": BLUE, "WARN": "#fab219", "ERROR": "#d03b3b", "METRICS": "#898781"}

level_df = spark.sql("""
    SELECT level, COUNT(*) AS n FROM event_log_recent GROUP BY level ORDER BY n DESC
""").toPandas()

fig, ax = plt.subplots(figsize=(6, 4))
colors = [STATUS_COLORS.get(lv, "#898781") for lv in level_df["level"]]
bars = ax.bar(level_df["level"], level_df["n"], color=colors)
ax.set_title(f"Events by severity — last {lookback_hours}h")
ax.spines[["top", "right"]].set_visible(False)
for b in bars:
    ax.text(b.get_x() + b.get_width() / 2, b.get_height(), f"{int(b.get_height())}",
            ha="center", va="bottom", fontsize=9, color="#52514e")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Streaming Backlog & Flow Throughput
# MAGIC
# MAGIC The core continuous-mode health signal: is the pipeline keeping up with its
# MAGIC input? `flow_progress.metrics` carries backlog fields per flow; `stream_progress`
# MAGIC carries the underlying Structured Streaming batch metrics.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   origin.flow_name,
# MAGIC   details:flow_progress.status::string AS status,
# MAGIC   details:flow_progress.metrics.num_output_rows::bigint AS output_rows,
# MAGIC   details:flow_progress.metrics.backlog_bytes::bigint AS backlog_bytes,
# MAGIC   details:flow_progress.metrics.backlog_records::bigint AS backlog_records,
# MAGIC   details:flow_progress.metrics.backlog_files::bigint AS backlog_files,
# MAGIC   details:flow_progress.metrics.backlog_seconds::bigint AS backlog_seconds
# MAGIC FROM event_log_recent
# MAGIC WHERE event_type = 'flow_progress'
# MAGIC   AND details:flow_progress.metrics.backlog_seconds IS NOT NULL
# MAGIC ORDER BY timestamp DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Flows currently furthest behind — the shortlist to investigate first
# MAGIC SELECT
# MAGIC   origin.flow_name,
# MAGIC   timestamp AS as_of,
# MAGIC   details:flow_progress.metrics.backlog_seconds::bigint AS backlog_seconds,
# MAGIC   details:flow_progress.metrics.backlog_records::bigint AS backlog_records
# MAGIC FROM event_log_recent
# MAGIC WHERE event_type = 'flow_progress'
# MAGIC   AND details:flow_progress.metrics.backlog_seconds IS NOT NULL
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY origin.flow_name ORDER BY timestamp DESC) = 1
# MAGIC ORDER BY backlog_seconds DESC NULLS LAST

# COMMAND ----------

backlog_df = spark.sql("""
    SELECT
      timestamp,
      origin.flow_name AS flow_name,
      details:flow_progress.metrics.backlog_seconds::bigint AS backlog_seconds
    FROM event_log_recent
    WHERE event_type = 'flow_progress'
      AND details:flow_progress.metrics.backlog_seconds IS NOT NULL
    ORDER BY timestamp
""").toPandas()

if backlog_df.empty:
    print("No backlog metrics in this window yet — sources may not report backlog, "
          "or the pipeline hasn't run in the lookback window.")
else:
    top_flows = backlog_df.groupby("flow_name")["backlog_seconds"].max().sort_values(ascending=False).head(5).index
    fig, ax = plt.subplots(figsize=(10, 5))
    for flow in top_flows:
        sub = backlog_df[backlog_df["flow_name"] == flow]
        ax.plot(sub["timestamp"], sub["backlog_seconds"], linewidth=2, label=flow)
    ax.axhline(backlog_seconds_alert, color="#d03b3b", linestyle="--", linewidth=1,
               label=f"alert threshold ({backlog_seconds_alert}s)")
    ax.set_ylabel("Backlog (seconds)")
    ax.set_title("Backlog age over time — top 5 flows")
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(fontsize=8, frameon=False)
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Quality (Expectations)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH quality_events AS (
# MAGIC   SELECT
# MAGIC     timestamp,
# MAGIC     origin.flow_name AS flow_name,
# MAGIC     details:flow_progress.data_quality.dropped_records::bigint AS dropped_records,
# MAGIC     CAST(
# MAGIC       details:flow_progress.data_quality.expectations
# MAGIC       AS ARRAY<STRUCT<name STRING, dataset STRING, passed_records BIGINT, failed_records BIGINT>>
# MAGIC     ) AS expectations
# MAGIC   FROM event_log_recent
# MAGIC   WHERE event_type = 'flow_progress'
# MAGIC     AND details:flow_progress.data_quality.expectations IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   flow_name,
# MAGIC   exp.name AS expectation_name,
# MAGIC   exp.dataset,
# MAGIC   exp.passed_records,
# MAGIC   exp.failed_records,
# MAGIC   ROUND(100.0 * exp.failed_records / NULLIF(exp.passed_records + exp.failed_records, 0), 2) AS fail_rate_pct
# MAGIC FROM quality_events
# MAGIC LATERAL VIEW explode(expectations) AS exp
# MAGIC ORDER BY fail_rate_pct DESC NULLS LAST, timestamp DESC

# COMMAND ----------

quality_df = spark.sql("""
    WITH quality_events AS (
      SELECT
        timestamp,
        origin.flow_name AS flow_name,
        CAST(
          details:flow_progress.data_quality.expectations
          AS ARRAY<STRUCT<name STRING, dataset STRING, passed_records BIGINT, failed_records BIGINT>>
        ) AS expectations
      FROM event_log_recent
      WHERE event_type = 'flow_progress'
        AND details:flow_progress.data_quality.expectations IS NOT NULL
    )
    SELECT
      exp.name AS expectation_name,
      exp.dataset,
      SUM(exp.failed_records) AS failed_records,
      SUM(exp.passed_records) AS passed_records
    FROM quality_events
    LATERAL VIEW explode(expectations) AS exp
    GROUP BY exp.name, exp.dataset
""").toPandas()

breaches = quality_df[
    (quality_df["passed_records"] + quality_df["failed_records"] > 0)
    & (100.0 * quality_df["failed_records"] / (quality_df["passed_records"] + quality_df["failed_records"])
       > expectation_fail_rate_alert)
]
if breaches.empty:
    print(f"No expectation is failing above the {expectation_fail_rate_alert}% threshold in this window.")
else:
    print(f"Expectations above the {expectation_fail_rate_alert}% fail-rate threshold:")
    display(breaches)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Compute & Resource Health
# MAGIC
# MAGIC Only populated for pipelines on **classic** compute — serverless pipelines
# MAGIC won't emit `autoscale` / `cluster_resources` events.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   details:autoscale.status::string AS status,
# MAGIC   details:autoscale.requested_num_executors::int AS requested_executors,
# MAGIC   details:autoscale.optimal_num_executors::int AS optimal_executors
# MAGIC FROM event_log_recent
# MAGIC WHERE event_type = 'autoscale'
# MAGIC ORDER BY timestamp DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT timestamp, details:cluster_resources::string AS cluster_resources_raw
# MAGIC FROM event_log_recent
# MAGIC WHERE event_type = 'cluster_resources'
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Restarts & Operator Actions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   details:user_action.user_name::string AS user_name,
# MAGIC   details:user_action.action::string AS action
# MAGIC FROM event_log_recent
# MAGIC WHERE event_type = 'user_action'
# MAGIC ORDER BY timestamp DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   details:create_update.run_as::string AS run_as,
# MAGIC   details:create_update.cause::string AS cause
# MAGIC FROM event_log_recent
# MAGIC WHERE event_type = 'create_update'
# MAGIC ORDER BY timestamp DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Undocumented Event Types (raw inspection)
# MAGIC
# MAGIC `graph_created`, `user_code_context`, `background_operations`, and
# MAGIC `dataset_life_cycle` are not in Databricks' public event log schema
# MAGIC reference as of August 2026 (see the companion report, Section 3). Pull raw
# MAGIC rows here to confirm their actual payload shape for your pipeline before
# MAGIC building anything that depends on them.

# COMMAND ----------

UNDOCUMENTED_EVENT_TYPES = [
    "graph_created",
    "user_code_context",
    "background_operations",
    "dataset_life_cycle",
]

for et in UNDOCUMENTED_EVENT_TYPES:
    n = spark.sql(f"SELECT COUNT(*) AS n FROM event_log_full WHERE event_type = '{et}'").collect()[0]["n"]
    print(f"{et}: {n} events in full history")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Change the event_type filter to inspect each one in turn
# MAGIC SELECT timestamp, level, origin.flow_name, details
# MAGIC FROM event_log_full
# MAGIC WHERE event_type = 'graph_created'
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Next Steps
# MAGIC
# MAGIC - **Schedule this notebook** as a Databricks Job (e.g. every 15 minutes) with
# MAGIC   the `pipeline_id` widget pre-filled via a job parameter, so Section 1 and
# MAGIC   Section 4 stay current without manual runs.
# MAGIC - **Turn the alert checks into real alerts**: wrap Sections 4 and 5's
# MAGIC   threshold checks in a condition that calls a webhook / sends a message
# MAGIC   (e.g. via a Slack or email connector) when a breach is found, instead of
# MAGIC   just printing.
# MAGIC - **Promote the SQL cells to a Lakeview dashboard** once you're happy with
# MAGIC   the queries — each `%sql` cell above maps directly to a dashboard tile.
# MAGIC - **Confirm the four undocumented event types** against Section 8's raw
# MAGIC   output and fold anything useful back into Sections 2–7.
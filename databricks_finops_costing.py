# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Usage FinOps Costing Notebook
# MAGIC
# MAGIC Cost visibility and chargeback reporting built on **Unity Catalog system tables**
# MAGIC (`system.billing.usage`, `system.billing.list_prices`, and the compute/jobs
# MAGIC inventory tables). No external billing export is required.
# MAGIC
# MAGIC **Covers:**
# MAGIC 1. Cost by workspace / SKU / cluster & warehouse
# MAGIC 2. Spend trend over time (daily, weekly, monthly, MoM delta)
# MAGIC 3. Chargeback / showback by team & cost-center tag
# MAGIC 4. Budget vs. actual tracking with anomaly flags
# MAGIC
# MAGIC **Prerequisites**
# MAGIC - Unity Catalog **system schema** access must be enabled for this account
# MAGIC   (Account Console → Settings → System tables, or ask your account admin).
# MAGIC   Docs: `https://docs.databricks.com/en/admin/system-tables/index.html`
# MAGIC - The principal running this notebook needs `USE CATALOG` / `SELECT` on the
# MAGIC   `system` catalog (`GRANT USE SCHEMA, SELECT ON SCHEMA system.billing TO <you>`).
# MAGIC - Column names below match the system table schema as of mid-2025. If your
# MAGIC   workspace is on an older/newer schema version, adjust column names in the
# MAGIC   **Config** cell — everything downstream references the same few aliases.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Config & widgets
# MAGIC All tunable parameters live here. Re-run this cell after changing a widget value.

# COMMAND ----------

dbutils.widgets.text("start_date", "", "Start date (YYYY-MM-DD, blank = 30 days back)")
dbutils.widgets.text("end_date", "", "End date (YYYY-MM-DD, blank = today)")
dbutils.widgets.text("monthly_budget_usd", "10000", "Monthly budget (USD)")
dbutils.widgets.text("anomaly_zscore_threshold", "2.5", "Anomaly z-score threshold")
dbutils.widgets.text("team_tag_keys", "team,Team,cost_center,CostCenter,cost-center", "Candidate tag keys for team/cost-center (comma-sep, first match wins)")
dbutils.widgets.text("write_back_catalog", "", "Catalog to write summary tables to (blank = skip write-back)")
dbutils.widgets.text("write_back_schema", "finops", "Schema to write summary tables to")

# COMMAND ----------

from datetime import date, timedelta
import pyspark.sql.functions as F
import pyspark.sql.types as T

end_date_str = dbutils.widgets.get("end_date").strip()
start_date_str = dbutils.widgets.get("start_date").strip()

END_DATE = date.fromisoformat(end_date_str) if end_date_str else date.today()
START_DATE = date.fromisoformat(start_date_str) if start_date_str else END_DATE - timedelta(days=30)

MONTHLY_BUDGET_USD = float(dbutils.widgets.get("monthly_budget_usd"))
ANOMALY_Z_THRESHOLD = float(dbutils.widgets.get("anomaly_zscore_threshold"))
TEAM_TAG_KEYS = [k.strip() for k in dbutils.widgets.get("team_tag_keys").split(",") if k.strip()]
WRITE_BACK_CATALOG = dbutils.widgets.get("write_back_catalog").strip()
WRITE_BACK_SCHEMA = dbutils.widgets.get("write_back_schema").strip()

print(f"Analysis window : {START_DATE} -> {END_DATE}")
print(f"Monthly budget  : ${MONTHLY_BUDGET_USD:,.2f}")
print(f"Anomaly z-thresh: {ANOMALY_Z_THRESHOLD}")
print(f"Team tag keys   : {TEAM_TAG_KEYS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load raw usage + list prices, compute list cost
# MAGIC
# MAGIC `system.billing.usage` is one row per usage record (roughly hourly, per SKU,
# MAGIC per resource). It does **not** contain a dollar amount directly — cost is
# MAGIC `usage_quantity * list price effective at usage_start_time for that sku_name`.
# MAGIC `system.billing.list_prices` is a slowly-changing table (has `price_start_time`
# MAGIC / `price_end_time`), so the join is a point-in-time (as-of) join, not an
# MAGIC equi-join on sku alone.

# COMMAND ----------

usage_raw = (
    spark.table("system.billing.usage")
    .filter((F.col("usage_date") >= F.lit(START_DATE)) & (F.col("usage_date") <= F.lit(END_DATE)))
)

prices_raw = spark.table("system.billing.list_prices")

# Point-in-time join: for each usage row, find the price row for the same sku_name
# whose validity window contains usage_start_time. Using a range join hint keeps
# this efficient at scale.
usage_priced = (
    usage_raw.alias("u")
    .join(
        prices_raw.alias("p"),
        on=(
            (F.col("u.sku_name") == F.col("p.sku_name"))
            & (F.col("u.cloud") == F.col("p.cloud"))
            & (F.col("u.usage_start_time") >= F.col("p.price_start_time"))
            & (
                F.col("p.price_end_time").isNull()
                | (F.col("u.usage_start_time") < F.col("p.price_end_time"))
            )
        ),
        how="left",
    )
    .select(
        "u.*",
        F.col("p.pricing.effective_list.default").alias("list_price_per_unit"),
        F.col("p.currency_code").alias("currency_code"),
    )
    .withColumn(
        "list_cost_usd",
        F.round(F.col("usage_quantity") * F.coalesce(F.col("list_price_per_unit"), F.lit(0.0)), 4),
    )
)

unpriced_rows = usage_priced.filter(F.col("list_price_per_unit").isNull()).count()
if unpriced_rows:
    print(f"WARNING: {unpriced_rows} usage rows had no matching list price and were costed at $0. "
          f"Check for new/legacy SKUs not covered by system.billing.list_prices.")

usage_priced.cache()
print(f"Loaded {usage_priced.count():,} usage records, ${usage_priced.agg(F.sum('list_cost_usd')).first()[0] or 0:,.2f} total list cost.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Enrich with workspace name and pull out chargeback tags
# MAGIC
# MAGIC `usage.custom_tags` already carries the resource's (cluster/job/warehouse/pipeline)
# MAGIC custom tags at the time of usage, so no extra join is needed for tag-based
# MAGIC chargeback. Workspace name is looked up best-effort from
# MAGIC `system.access.workspaces_latest`; if that table isn't available/granted in
# MAGIC your account, we fall back to showing `workspace_id` only.

# COMMAND ----------

try:
    workspaces = (
        spark.table("system.access.workspaces_latest")
        .select(
            F.col("workspace_id"),
            F.col("workspace_name"),
        )
        .dropDuplicates(["workspace_id"])
    )
    usage_enriched = usage_priced.join(workspaces, on="workspace_id", how="left")
except Exception as e:
    print(f"Could not load system.access.workspaces_latest ({e}); using workspace_id as the display name.")
    usage_enriched = usage_priced.withColumn("workspace_name", F.col("workspace_id"))

usage_enriched = usage_enriched.withColumn(
    "workspace_label", F.coalesce(F.col("workspace_name"), F.col("workspace_id"))
)


def first_matching_tag(tag_keys):
    """Return a Spark column expression that coalesces the first present tag key."""
    exprs = [F.col("custom_tags").getItem(k) for k in tag_keys]
    return F.coalesce(*exprs, F.lit("untagged"))


usage_enriched = usage_enriched.withColumn("chargeback_team", first_matching_tag(TEAM_TAG_KEYS))

# Convenience columns pulled out of usage_metadata for resource-level breakdowns.
usage_enriched = usage_enriched.withColumn("cluster_id", F.col("usage_metadata.cluster_id"))
usage_enriched = usage_enriched.withColumn("job_id", F.col("usage_metadata.job_id"))
usage_enriched = usage_enriched.withColumn("warehouse_id", F.col("usage_metadata.warehouse_id"))
usage_enriched = usage_enriched.withColumn("dlt_pipeline_id", F.col("usage_metadata.dlt_pipeline_id"))

usage_enriched.cache()
display(usage_enriched.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cost by workspace, SKU, and cluster/warehouse
# MAGIC
# MAGIC The single most useful FinOps cut: where is the money going.

# COMMAND ----------

cost_by_workspace_sku = (
    usage_enriched.groupBy("workspace_label", "sku_name", "usage_unit")
    .agg(
        F.sum("usage_quantity").alias("usage_qty"),
        F.sum("list_cost_usd").alias("list_cost_usd"),
    )
    .orderBy(F.desc("list_cost_usd"))
)
display(cost_by_workspace_sku)

# COMMAND ----------

cost_by_workspace = (
    cost_by_workspace_sku.groupBy("workspace_label")
    .agg(F.sum("list_cost_usd").alias("list_cost_usd"))
    .orderBy(F.desc("list_cost_usd"))
)
display(cost_by_workspace)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top clusters and SQL warehouses by cost
# MAGIC
# MAGIC Joined against `system.compute.clusters` / `system.compute.warehouses` for
# MAGIC human-readable names. These inventory tables are best-effort too — if a
# MAGIC cluster/warehouse has since been deleted it may still resolve (the tables are
# MAGIC SCD2), but if the table itself isn't available we fall back to the raw id.

# COMMAND ----------

def safe_table(name):
    try:
        return spark.table(name)
    except Exception as e:
        print(f"Could not load {name} ({e}); skipping name enrichment for it.")
        return None


clusters_tbl = safe_table("system.compute.clusters")
warehouses_tbl = safe_table("system.compute.warehouses")

top_clusters = (
    usage_enriched.filter(F.col("cluster_id").isNotNull())
    .groupBy("workspace_label", "cluster_id", "chargeback_team")
    .agg(F.sum("list_cost_usd").alias("list_cost_usd"))
)

if clusters_tbl is not None:
    clusters_latest = clusters_tbl.select("cluster_id", "cluster_name").dropDuplicates(["cluster_id"])
    top_clusters = top_clusters.join(clusters_latest, on="cluster_id", how="left")
else:
    top_clusters = top_clusters.withColumn("cluster_name", F.lit(None).cast("string"))

top_clusters = top_clusters.select(
    "workspace_label",
    F.coalesce("cluster_name", "cluster_id").alias("cluster"),
    "chargeback_team",
    "list_cost_usd",
).orderBy(F.desc("list_cost_usd"))

display(top_clusters.limit(50))

# COMMAND ----------

top_warehouses = (
    usage_enriched.filter(F.col("warehouse_id").isNotNull())
    .groupBy("workspace_label", "warehouse_id", "chargeback_team")
    .agg(F.sum("list_cost_usd").alias("list_cost_usd"))
)

if warehouses_tbl is not None:
    warehouses_latest = warehouses_tbl.select("warehouse_id", "warehouse_name").dropDuplicates(["warehouse_id"])
    top_warehouses = top_warehouses.join(warehouses_latest, on="warehouse_id", how="left")
else:
    top_warehouses = top_warehouses.withColumn("warehouse_name", F.lit(None).cast("string"))

top_warehouses = top_warehouses.select(
    "workspace_label",
    F.coalesce("warehouse_name", "warehouse_id").alias("warehouse"),
    "chargeback_team",
    "list_cost_usd",
).orderBy(F.desc("list_cost_usd"))

display(top_warehouses.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Spend trend over time
# MAGIC
# MAGIC Daily, weekly, and monthly rollups plus month-over-month delta so spikes and
# MAGIC step-changes are easy to spot.

# COMMAND ----------

daily_cost = (
    usage_enriched.groupBy("usage_date")
    .agg(F.sum("list_cost_usd").alias("list_cost_usd"))
    .orderBy("usage_date")
)
display(daily_cost)

# COMMAND ----------

weekly_cost = (
    usage_enriched.withColumn("week_start", F.date_trunc("week", "usage_date"))
    .groupBy("week_start")
    .agg(F.sum("list_cost_usd").alias("list_cost_usd"))
    .orderBy("week_start")
)
display(weekly_cost)

# COMMAND ----------

monthly_cost = (
    usage_enriched.withColumn("month_start", F.date_trunc("month", "usage_date"))
    .groupBy("month_start")
    .agg(F.sum("list_cost_usd").alias("list_cost_usd"))
    .orderBy("month_start")
)

from pyspark.sql.window import Window

mom_window = Window.orderBy("month_start")
monthly_cost_mom = monthly_cost.withColumn(
    "prev_month_cost_usd", F.lag("list_cost_usd").over(mom_window)
).withColumn(
    "mom_delta_usd", F.round(F.col("list_cost_usd") - F.col("prev_month_cost_usd"), 2)
).withColumn(
    "mom_delta_pct",
    F.round(
        100 * (F.col("list_cost_usd") - F.col("prev_month_cost_usd")) / F.col("prev_month_cost_usd"),
        1,
    ),
)
display(monthly_cost_mom)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trend chart (matplotlib)

# COMMAND ----------

import matplotlib.pyplot as plt

daily_pdf = daily_cost.toPandas().sort_values("usage_date")

fig, ax = plt.subplots(figsize=(11, 4))
ax.plot(daily_pdf["usage_date"], daily_pdf["list_cost_usd"], marker="o", linewidth=1.5)
ax.set_title(f"Daily Databricks list cost, {START_DATE} to {END_DATE}")
ax.set_xlabel("Date")
ax.set_ylabel("Cost (USD)")
ax.grid(alpha=0.3)
fig.autofmt_xdate()
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Chargeback / showback by team
# MAGIC
# MAGIC Uses the `chargeback_team` column derived in Section 3 from `custom_tags`
# MAGIC (first match across `team_tag_keys`). Untagged usage rolls up to `"untagged"`
# MAGIC so it stays visible instead of silently disappearing — treat a large
# MAGIC `untagged` bucket as a tagging-hygiene action item, not a data bug.

# COMMAND ----------

chargeback_by_team = (
    usage_enriched.groupBy("chargeback_team")
    .agg(
        F.sum("list_cost_usd").alias("list_cost_usd"),
        F.countDistinct("workspace_id").alias("workspaces_touched"),
    )
    .orderBy(F.desc("list_cost_usd"))
)

total_cost = chargeback_by_team.agg(F.sum("list_cost_usd")).first()[0] or 1.0
chargeback_by_team = chargeback_by_team.withColumn(
    "pct_of_total", F.round(100 * F.col("list_cost_usd") / F.lit(total_cost), 1)
)
display(chargeback_by_team)

untagged_pct = (
    chargeback_by_team.filter(F.col("chargeback_team") == "untagged")
    .select("pct_of_total")
    .first()
)
if untagged_pct and untagged_pct[0] and untagged_pct[0] > 20:
    print(f"WARNING: {untagged_pct[0]}% of spend is untagged. Consider enforcing cluster/job "
          f"tagging policies (e.g. via cluster policies) to improve chargeback accuracy.")

# COMMAND ----------

chargeback_team_daily = (
    usage_enriched.groupBy("usage_date", "chargeback_team")
    .agg(F.sum("list_cost_usd").alias("list_cost_usd"))
    .orderBy("usage_date")
)
display(chargeback_team_daily)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Budget vs. actual, and anomaly flags
# MAGIC
# MAGIC - **Budget vs actual**: compares month-to-date actual spend against a
# MAGIC   pro-rated share of `monthly_budget_usd` for the elapsed days in the current
# MAGIC   month, and projects a full-month run-rate.
# MAGIC - **Anomaly flags**: for each day, compute a rolling 30-day mean/stddev of
# MAGIC   total daily cost (excluding the day itself) and flag days where actual
# MAGIC   cost exceeds `mean + anomaly_zscore_threshold * stddev`. This is a simple,
# MAGIC   explainable z-score approach — swap in a fancier model if needed.

# COMMAND ----------

import calendar

today = date.today()
month_start = today.replace(day=1)
days_in_month = calendar.monthrange(today.year, today.month)[1]
days_elapsed = today.day

mtd_cost_row = (
    usage_enriched.filter(F.col("usage_date") >= F.lit(month_start))
    .agg(F.sum("list_cost_usd").alias("mtd_cost"))
    .first()
)
mtd_cost = mtd_cost_row["mtd_cost"] or 0.0

prorated_budget = MONTHLY_BUDGET_USD * (days_elapsed / days_in_month)
projected_month_end = mtd_cost / days_elapsed * days_in_month if days_elapsed else 0.0

print(f"Month to date ({month_start} -> {today}):")
print(f"  Actual MTD spend      : ${mtd_cost:,.2f}")
print(f"  Pro-rated budget MTD  : ${prorated_budget:,.2f}")
print(f"  MTD variance          : ${mtd_cost - prorated_budget:,.2f} "
      f"({'OVER' if mtd_cost > prorated_budget else 'under'} budget pace)")
print(f"  Full monthly budget   : ${MONTHLY_BUDGET_USD:,.2f}")
print(f"  Projected month-end   : ${projected_month_end:,.2f} "
      f"({'OVER' if projected_month_end > MONTHLY_BUDGET_USD else 'under'} budget if current run-rate holds)")

# COMMAND ----------

budget_vs_actual = spark.createDataFrame(
    [
        ("Actual MTD", float(mtd_cost)),
        ("Pro-rated budget MTD", float(prorated_budget)),
        ("Projected month-end", float(projected_month_end)),
        ("Full monthly budget", float(MONTHLY_BUDGET_USD)),
    ],
    ["metric", "usd"],
)
display(budget_vs_actual)

# COMMAND ----------

daily_window = Window.orderBy("usage_date").rowsBetween(-30, -1)

daily_with_stats = (
    daily_cost
    .withColumn("rolling_mean", F.avg("list_cost_usd").over(daily_window))
    .withColumn("rolling_stddev", F.stddev("list_cost_usd").over(daily_window))
    .withColumn(
        "zscore",
        F.when(
            F.col("rolling_stddev").isNotNull() & (F.col("rolling_stddev") > 0),
            (F.col("list_cost_usd") - F.col("rolling_mean")) / F.col("rolling_stddev"),
        ),
    )
    .withColumn(
        "is_anomaly",
        F.col("zscore").isNotNull() & (F.col("zscore") > ANOMALY_Z_THRESHOLD),
    )
)

anomalies = daily_with_stats.filter(F.col("is_anomaly")).orderBy("usage_date")
display(daily_with_stats.orderBy("usage_date"))

anomaly_count = anomalies.count()
if anomaly_count:
    print(f"{anomaly_count} anomalous day(s) detected (cost > rolling mean + {ANOMALY_Z_THRESHOLD} std dev):")
    display(anomalies.select("usage_date", "list_cost_usd", "rolling_mean", "zscore"))
else:
    print(f"No anomalous days detected in the analysis window at z >= {ANOMALY_Z_THRESHOLD}. "
          f"Note: the first 30 days of any window won't have enough history for a rolling baseline.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Executive summary

# COMMAND ----------

top_workspace_row = cost_by_workspace.first()
top_sku_row = (
    usage_enriched.groupBy("sku_name").agg(F.sum("list_cost_usd").alias("c")).orderBy(F.desc("c")).first()
)
window_total = usage_enriched.agg(F.sum("list_cost_usd")).first()[0] or 0.0

if top_workspace_row:
    top_workspace_line = f"  Top workspace by cost     : {top_workspace_row['workspace_label']} (${top_workspace_row['list_cost_usd']:,.2f})"
else:
    top_workspace_line = "  Top workspace by cost     : n/a"

if top_sku_row:
    top_sku_line = f"  Top SKU by cost           : {top_sku_row['sku_name']} (${top_sku_row['c']:,.2f})"
else:
    top_sku_line = "  Top SKU by cost           : n/a"

summary_lines = [
    f"FinOps summary for {START_DATE} -> {END_DATE}",
    f"  Total list cost           : ${window_total:,.2f}",
    top_workspace_line,
    top_sku_line,
    f"  MTD vs pro-rated budget   : ${mtd_cost - prorated_budget:,.2f} variance",
    f"  Anomalous days flagged    : {anomaly_count}",
]
print("\n".join(summary_lines))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Optional write-back
# MAGIC
# MAGIC If `write_back_catalog` is set, persist the key summary tables as Delta
# MAGIC tables so a BI tool (or a scheduled Databricks SQL alert) can consume them
# MAGIC without re-running this notebook. Skipped by default.

# COMMAND ----------

if WRITE_BACK_CATALOG:
    target_schema = f"{WRITE_BACK_CATALOG}.{WRITE_BACK_SCHEMA}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")

    (cost_by_workspace_sku.write.mode("overwrite")
        .saveAsTable(f"{target_schema}.cost_by_workspace_sku"))
    (daily_with_stats.write.mode("overwrite")
        .saveAsTable(f"{target_schema}.daily_cost_with_anomalies"))
    (chargeback_by_team.write.mode("overwrite")
        .saveAsTable(f"{target_schema}.chargeback_by_team"))
    (budget_vs_actual.write.mode("overwrite")
        .saveAsTable(f"{target_schema}.budget_vs_actual"))

    print(f"Wrote summary tables to {target_schema}: "
          f"cost_by_workspace_sku, daily_cost_with_anomalies, chargeback_by_team, budget_vs_actual")
else:
    print("write_back_catalog widget is blank — skipping write-back. "
          "Set it and re-run this cell to persist summary tables.")

# COMMAND ----------

usage_priced.unpersist()
usage_enriched.unpersist()
print("Done.")

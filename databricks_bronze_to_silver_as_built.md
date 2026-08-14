# [Pipeline Name] – Databricks Bronze → Silver As-Built Documentation

## 1. Document Control

| Field | Detail |
|---|---|
| Document Owner | |
| Author(s) | |
| Date Created | |
| Last Updated | |
| Version | 1.0 |
| Status | Draft / In Review / Approved |
| Reviewers / Approvers | |

## 2. Overview

**Purpose:** Brief description of why this pipeline exists and what business/technical need it addresses (e.g. cleansing, deduplication, conforming, or enriching raw Bronze data into curated Silver).

**Summary:** One paragraph describing what the pipeline does end to end.

## 3. Scope

- In scope: (datasets, tables, domains covered)
- Out of scope: (explicitly excluded items)

## 4. Architecture

Insert architecture diagram here (draw.io, Lucidchart, or exported image embedded in Confluence).

Brief narrative description of the flow: trigger → compute → source read → transform → target write → downstream consumers.

## 5. Source Details (Bronze Layer)

| Field | Detail |
|---|---|
| Catalog | |
| Schema/Database | |
| Table(s) | |
| Storage Location (path/URI) | |
| Format (Delta/Parquet) | |
| Partitioning | |
| Row Count / Volume (approx) | |
| Refresh Frequency | |

## 6. Target Details (Silver Layer)

| Field | Detail |
|---|---|
| Catalog | |
| Schema/Database | |
| Table(s) | |
| Storage Location (path/URI) | |
| Format (Delta/Parquet) | |
| Partitioning | |
| Write Mode (append/overwrite/merge) | |
| Retention / Time Travel Settings | |

## 7. Pipeline / Job Details

| Field | Detail |
|---|---|
| Job Name (Databricks Workflow) | |
| Job ID | |
| Notebook / Script Path | |
| Repo & Branch (Git source) | |
| Cluster Type (Job cluster / All-purpose) | |
| Cluster Config (node type, workers, DBR version) | |
| Autoscaling | |
| Libraries / Init Scripts | |
| Schedule / Trigger (cron, event-based) | |
| Timeout / Retry Policy | |
| Run As / Service Principal | |

## 8. Data Flow & Transformation Logic

Step-by-step description of processing logic:

1. Read from Bronze source (filters, incremental logic, watermarking)
2. Transformations applied (cleansing, dedup, type casting, flattening, enrichment, etc.)
3. Write to Silver target (merge keys, write mode)

## 9. Schema Mapping

| Source Column (Bronze) | Target Column (Silver) | Data Type | Transformation/Notes |
|---|---|---|---|
| | | | |

## 10. Configuration & Parameters

| Parameter | Value | Description |
|---|---|---|
| | | |

Include: widget/parameter values, environment variables, secret scopes referenced (not actual secret values).

## 11. Dependencies

- **Upstream:** jobs/pipelines that must complete before this runs
- **Downstream:** consumers of the Silver output (dashboards, other jobs, ML pipelines, Gold aggregations)

## 12. Data Quality & Validation

- Validation checks performed (row counts, null checks, schema enforcement, checksums)
- Tool used (e.g. Great Expectations, DLT expectations, custom checks)
- Reconciliation process between source and target

## 13. Error Handling & Logging

- Failure behavior (fail fast, quarantine bad records, dead-letter table)
- Logging destination (system tables, external logging, alerts)
- Alerting mechanism (email, Slack, PagerDuty) and thresholds

## 14. Security & Access Control

| Item | Detail |
|---|---|
| Unity Catalog Permissions | |
| Row/Column Level Security | |
| Service Principal / Identity used | |
| Secret Scopes | |
| PII Handling | |

## 15. Monitoring & Alerting

- Dashboards used to monitor job health
- SLA / expected run duration
- Alert thresholds and escalation path

## 16. Deployment Details

| Field | Detail |
|---|---|
| CI/CD Tool | |
| Deployment Pipeline Link | |
| Environments (Dev/Test/Prod) | |
| Deployment Date | |
| Deployed By | |
| Change/Release Ticket | |

## 17. Testing & Sign-off

- Test cases executed and results summary
- UAT sign-off (who, when)
- Known defects at go-live

## 18. Rollback Plan

Steps to revert the pipeline/data changes if issues are found post-deployment.

## 19. Known Issues / Limitations

| Issue | Impact | Workaround | Status |
|---|---|---|---|

## 20. Appendix

- Links to source repo
- Links to related Confluence pages / Jira tickets
- Contact list (data engineering, support team)

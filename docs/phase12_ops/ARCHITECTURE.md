# Phase 12 - Operations and Evidence Architecture
**Status:** Draft (authoritative for Phase 12 work)
**Last updated:** 2026-01-05

## 1. Overview
Phase 12 adds a daily operations driver and a longitudinal evidence layer.
It coordinates existing ingestion and modeling pipelines, tracks run outcomes,
and generates daily and rolling diagnostics. No modeling math changes are made.

## 2. Module Layout
### 2.1 Orchestration
- `pipelines/ops/run_daily.py`
  - Runs a safe sequence of steps gated by env flags.
  - Records run metadata and failures in `fact_run_registry`.

### 2.2 Evidence Layer (DuckDB)
New append-only fact tables written by daily diagnostics:
- `fact_run_registry`: run metadata, step status, error summary, timings.
- `fact_odds_coverage_daily`: counts by vendor/book/market, snapshot coverage.
- `fact_mapping_quality_daily`: mapping rates for event/player joins.
- `fact_ev_summary_daily`: EV distribution summaries by market/vendor/book.
- `fact_forecast_accuracy_daily`: accuracy-only metrics for model outputs.

### 2.3 Reporting
- `outputs/monitoring/daily_report_<date>.md`
  - Single-day summary, failures, and key coverage stats.
- `outputs/monitoring/rolling_report.md`
  - Rolling-window summaries, trends, and deltas.

### 2.4 Backfill Scaffold
- `pipelines/odds/backfill_unabated_snapshots.py`
  - Bounded, resumable backfill with strict request/time caps.
  - Idempotent inserts into normalized tables.

## 3. Daily Driver Sequence
Order is fixed but each step is gated by env flags. Default behavior remains
unchanged when flags are off.

1) Odds ingestion (PlayNow + Unabated + OddsShark)
2) Production probability snapshot pipeline (existing entrypoints)
3) EV reporting (existing entrypoints)
4) Outcomes refresh (if applicable)
5) Diagnostics reports + evidence table writes

## 4. Failure Handling and Hardening
- Vendor requests share standardized timeouts and retry limits.
- Vendor failures are captured with context and do not stop the run.
- Failures are recorded in `fact_run_registry` and the daily report.

## 5. Data Flow (High Level)
- Vendor scrapers write raw payloads to `outputs/odds/raw/...`
- Normalization writes to `fact_prop_odds` (append-only).
- Existing production pipeline generates probabilities and EV outputs.
- Diagnostics queries produce daily summaries and populate evidence tables.
- Reports render from evidence tables and last-run metadata.

## 6. Boundaries and Constraints
- No changes to Phase 10 model logic or EV formulas.
- No ROI gating or tuning; ROI may be observational only.
- No raw payloads or DuckDB files committed to git.

## 7. Anti-Hang and Resource Rules
- No unbounded recursive scans (depth <= 4; exclude outputs/** and data/**).
- Do not print large payloads; store raw payloads and print summaries only.
- DuckDB inspection uses SQL with LIMIT <= 50.
- Network calls must set connect timeout, read timeout, max retries <= 3.
- Backfills must use max requests/time stop conditions.

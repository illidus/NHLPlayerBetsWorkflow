# Phase 12 - Operations and Longitudinal Evidence Layer
**Status:** Draft (authoritative for Phase 12 work)
**Last updated:** 2026-01-05

## 1. Purpose
Phase 12 hardens daily operations and adds a longitudinal evidence layer so the
system can run unattended while accumulating diagnostics over time.

This phase adds operational plumbing, run tracking, and reporting only. It does
not change any Phase 10 modeling math or Phase 11 ingestion logic.

## 2. Non-Negotiables (Binding)
- Do not change Phase 10 canonical modeling logic:
  - distributions, mu formulas, rolling windows, feature definitions
  - calibration fitting or selection policy
  - MARKET_POLICY rules
  - accuracy-only gating metrics
- Do not change EV formulas. Only input wiring, joins, and operational plumbing.
- ROI may exist only as explicitly labeled observational output, never gating.

## 3. In Scope
### 3.1 Daily driver (safe ordering)
- New entrypoint: `pipelines/ops/run_daily.py`
- Safe order with env flags:
  1) odds ingestion (PlayNow + Unabated + OddsShark)
  2) production probability snapshot pipeline (existing entrypoints)
  3) EV reporting
  4) outcomes refresh (if applicable)
  5) diagnostics reports
- Must not change default behavior when flags are off.

### 3.2 Longitudinal evidence layer (DuckDB)
New append-only fact tables and daily/rolling reports:
- `fact_run_registry`
- `fact_odds_coverage_daily`
- `fact_mapping_quality_daily`
- `fact_ev_summary_daily`
- `fact_forecast_accuracy_daily` (accuracy-only)
- `outputs/monitoring/daily_report_<date>.md`
- `outputs/monitoring/rolling_report.md`

### 3.3 Vendor hardening
- Standardize timeouts/retries across vendors.
- Graceful degradation: vendor failure logs, run continues.
- Record failures in `fact_run_registry` and daily report.

### 3.4 Bounded backfill scaffold
- New entrypoint: `pipelines/odds/backfill_unabated_snapshots.py`
- Date range args, max requests/time, dry-run mode.
- Idempotent inserts.

## 4. Optional (If Time)
- OddsShark synthetic event IDs (deterministic).
- Preserve quoted odds fields alongside derived american/decimal.
- CI-friendly smoke tests (schema, uniqueness, hashing, join-window).

## 5. Out of Scope
- Any changes to model math, distributions, or calibration.
- Changes to MARKET_POLICY or accuracy gating definitions.
- ROI-driven selection or tuning.
- Committing raw payloads or DuckDB files.

## 6. Verification Requirements
- `python scripts/golden_run_validate.py` must pass.
- Daily driver runs in minimal mode and produces artifacts.
- Idempotency demonstrated for odds ingestion/backfill.
- Vendor failures do not crash daily run.

## 7. Stop Conditions
Phase 12 is complete when:
- Daily driver runs end-to-end in a safe default configuration.
- Longitudinal tables populate and reports generate.
- Vendor failures are recorded and do not crash runs.
- Backfill scaffold works with strict bounds.
- Golden validation passes.

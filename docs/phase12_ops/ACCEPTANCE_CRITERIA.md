# Phase 12 Acceptance Criteria

## Daily Driver (Subsection A)
- [x] `pipelines/ops/run_daily.py --help` prints usage.
- [x] `pipelines/ops/run_daily.py` with all steps off exits without side effects.
- [x] `pipelines/ops/run_daily.py --run-odds-ingestion --run-ev` completes without crashing and records step status.
- [x] Vendor failures are recorded in the daily run log and do not halt the run (unless `--fail-fast`).

## Evidence Layer (Subsection B)
- [x] Diagnostics run writes `outputs/monitoring/daily_report_<date>.md`.
- [x] Diagnostics run writes `outputs/monitoring/rolling_report.md`.
- [x] Evidence tables populated for the day: `fact_run_registry`, `fact_odds_coverage_daily`, `fact_mapping_quality_daily`, `fact_ev_summary_daily`, `fact_forecast_accuracy_daily`.

## Vendor Hardening (Subsection C)
- [x] Vendor calls use connect/read timeouts and capped retries (<=3).
- [x] Forced failure mode records vendor failure while allowing run to complete.

## Backfill Scaffold (Subsection D)
- [x] `pipelines/odds/backfill_unabated_snapshots.py --dry-run` prints a plan.
- [x] One-day backfill completes within caps.
- [x] Rerun does not add duplicates (idempotent inserts).

## Final Verification
- [x] `python scripts/golden_run_validate.py` passes.

## Optional E1 - OddsShark Synthetic Event IDs
- [x] OddsShark records use deterministic `event_id_vendor` derived from date + away + home.
- [x] Unit test passes for deterministic synthetic ID generation.

## Optional E2 - Preserve Quoted Odds
- [x] Quoted odds fields (`odds_quoted_*`) are preserved alongside derived odds.
- [x] Unit test validates quoted odds fields are preserved.

## Optional E3 - Smoke Tests
- [x] Schema creation smoke test passes.
- [x] Idempotent insert behavior smoke test passes.
- [x] Vendor wrapper caps smoke test passes.
- [x] No-side-effects run_daily smoke test passes.

## Optional E4 - Operational Hardening
- [x] `run_daily.py --dry-run` prints planned steps without side effects.
- [x] Subprocess steps enforce timeouts and report timeout errors.

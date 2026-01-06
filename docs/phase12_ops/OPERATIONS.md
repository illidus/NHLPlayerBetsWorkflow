# Phase 12 Operations

## Daily Driver
### Command
- `python pipelines/ops/run_daily.py --help`
- `python pipelines/ops/run_daily.py`
- `python pipelines/ops/run_daily.py --run-odds-ingestion --run-ev`
- `python pipelines/ops/run_daily.py --run-diagnostics`

### Expected Artifacts
- `outputs/monitoring/daily_run_<YYYY-MM-DD>.md`
- `outputs/monitoring/daily_report_<YYYY-MM-DD>.md`
- `outputs/monitoring/rolling_report.md`
- DuckDB tables:
  - `fact_run_registry`
  - `fact_odds_coverage_daily`
  - `fact_mapping_quality_daily`
  - `fact_ev_summary_daily`
  - `fact_forecast_accuracy_daily`

### Troubleshooting
- If odds ingestion fails, inspect `outputs/monitoring/ingest_status_<YYYY-MM-DD>.json`.
- If diagnostics fail, confirm `data/db/nhl_backtest.duckdb` exists and `outputs/projections/SingleGamePropProbabilities.csv` is present.
- Use `FORCE_VENDOR_FAILURE=UNABATED` (or `ODDSSHARK`, `PLAYNOW`) to simulate vendor failures safely.

## Unabated Backfill
### Command
- `python pipelines/odds/backfill_unabated_snapshots.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD --dry-run`
- `python pipelines/odds/backfill_unabated_snapshots.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD --max-requests 1 --max-elapsed-seconds 60`

### Expected Artifacts
- Raw snapshots under `outputs/odds/raw/UNABATED/YYYY/MM/DD/`
- Inserts into `fact_prop_odds` and `raw_odds_payloads`

### Troubleshooting
- If the backfill stops early, verify `--max-requests` and `--max-elapsed-seconds` caps.
- Reruns should skip existing snapshot dates and avoid duplicate inserts.

## Run Log
### 2026-01-06
- Commands:
  - `python pipelines/ops/run_daily.py --help`
  - `python pipelines/ops/run_daily.py`
  - `python pipelines/ops/run_daily.py --run-odds-ingestion --run-ev`
  - `python pipelines/ops/run_daily.py --run-diagnostics` (initial failure, then rerun after fix)
  - `FORCE_VENDOR_FAILURE=UNABATED python pipelines/ops/run_daily.py --run-odds-ingestion --run-diagnostics`
  - `python pipelines/odds/backfill_unabated_snapshots.py --start-date 2026-01-05 --end-date 2026-01-06 --dry-run`
  - `python pipelines/odds/backfill_unabated_snapshots.py --start-date 2026-01-07 --end-date 2026-01-07 --max-requests 1 --max-elapsed-seconds 60`
  - `python scripts/golden_run_validate.py`
- Results:
  - Daily driver help/skip runs completed.
  - Odds ingestion + EV run completed; multi-book EV export produced.
  - Diagnostics initially failed (DuckDB prepared parameter in temp view); fixed by inlining values; rerun succeeded.
  - Forced vendor failure recorded for Unabated while run completed.
  - Backfill dry-run printed plan; one-day backfill run completed within caps and skipped duplicate payloads.
  - Golden run validation PASS.
- Artifacts:
  - `outputs/monitoring/daily_run_2026-01-06.md`
  - `outputs/monitoring/daily_report_2026-01-06.md`
  - `outputs/monitoring/rolling_report.md`
  - `outputs/monitoring/ingest_status_2026-01-06.json`
  - `outputs/ev_analysis/MultiBookBestBets.xlsx`
  - `outputs/audits/ev_prob_audit_2026-01-07.md`
  - `outputs/audits/ev_prob_audit_2026-01-07.csv`
  - `outputs/audits/ev_prob_audit_2026-01-07.jsonl`

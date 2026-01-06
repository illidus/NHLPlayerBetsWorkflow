# Pipeline Gates & Runbook

## Golden Validation (Phase 10 parity)
- Command: `python scripts/golden_run_validate.py`
- Purpose: Runs production pipeline twice (calibration on/off) plus optional accuracy backtest to ensure Phase 10 behavior unchanged.
- Expected artifacts:
  - EV audit CSVs in `outputs/audits/ev_prob_audit_*.csv`
  - (Optional backtest) `outputs/backtest_reports/forecast_accuracy.md`
  - EV exports in `outputs/ev_analysis/` from the standard runner.

## Odds Ingestion (Phase 11)
- Command: `python pipelines/backtesting/ingest_odds_to_duckdb.py`
- Behavior: Initializes Phase 11 tables, then ingests UNABATED → PLAYNOW → ODDSSHARK snapshots.
- Expected artifacts:
  - Raw payloads under `outputs/odds/raw/<VENDOR>/YYYY/MM/DD/` with `.sha256` sidecars.
  - Normalized odds appended to `fact_prop_odds` in `data/db/nhl_backtest.duckdb`.
  - Ingestion registry rows in `raw_odds_payloads`.
- Vendor-specific toggles are not exposed; individual vendor runs require manual edits or wrapper scripts.

## EV Report Generation (Multi-book)
- Command: `python src/nhl_bets/analysis/runner_duckdb.py`
- Prereqs: Phase 11 odds ingested + `outputs/projections/SingleGamePropProbabilities.csv` present. Mapping updates (`normalize.update_player_mappings` / `update_event_mappings`) are manual today.
- Expected artifacts:
  - `outputs/ev_analysis/MultiBookBestBets.xlsx` (written twice; last write includes `ev_sort`).
  - Console preview of top 10 EV rows.

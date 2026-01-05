# Repo Inventory - BEFORE Refactor
**Date:** 2026-01-05
**Status:** Phase 10 (Production)

## 1. Directory Structure (Depth 4)
```text
C:\Users\Ryisa\Documents\Scripts\NHLPlayerBetsWorkflow\
├───1_PlayerProps/
│   ├───nhl_player_props_all.csv
│   └───scrapers/
│       └───nhl_props_scraper.py
├───1_Scrape/
│   ├───playnow_api_client.py
│   └───scrape_playnow_api.py
├───2_Projections/
│   ├───BaseSingleGameProjections.csv
│   ├───GameContext.csv
│   ├───MODEL_PROJECTION_THEORY.md
│   ├───produce_game_context.py
│   ├───produce_live_base_projections.py
│   ├───single_game_probs.py
│   └───SingleGamePropProbabilities.csv
├───3_EV_Analysis/
│   ├───BestCandidatesFiltered.xlsx
│   ├───ev_bets_ranked.csv
│   ├───main.py
│   ├───MODEL_NOTES.md
│   └───src/
│       ├───audit.py
│       ├───distributions.py
│       ├───ev.py
│       ├───export.py
│       ├───file_io.py
│       ├───main.py
│       ├───normalize.py
│       └───parse.py
├───4_Backtesting/
│   ├───10_data_sources/
│   ├───20_data_store/
│   │   └───nhl_backtest.duckdb
│   ├───30_pipelines/
│   │   ├───apply_calibrators.py
│   │   ├───build_player_features.py
│   │   ├───evaluate_forecast_accuracy.py
│   │   ├───... (many more pipelines)
│   ├───40_runner/
│   │   └───run_ev_backtest.py
│   ├───50_metrics/
│   │   ├───evaluate_calibration.py
│   │   ├───... (many more experiments)
│   ├───60_reports/
│   └───70_models/
│       ├───calibrators/
│       └───calibrators_posthoc/
├───docs/
│   ├───accuracy/
│   ├───audits/
│   ├───experiments/
│   └───meta/
├───nhl_bets/
│   └───probs/
│       ├───distributions.py
│       └───single_game_model.py
├───scripts/
│   ├───generate_best_bets.py
│   └───golden_run_validate.py
└───SingleGamePlayerPrediction/
    └───BaseSingleGameProjections.csv
```

## 2. Python Entrypoints (Scripts with Main Guards)
- `run_workflow.py` (Main Orchestrator)
- `1_Scrape/scrape_playnow_api.py`
- `2_Projections/single_game_probs.py`
- `2_Projections/produce_live_base_projections.py`
- `2_Projections/produce_game_context.py`
- `3_EV_Analysis/main.py`
- `3_EV_Analysis/src/main.py`
- `4_Backtesting/40_runner/run_ev_backtest.py`
- `scripts/generate_best_bets.py`
- `scripts/golden_run_validate.py`
- `verify_phase6.py`
- `analyze_phase7_results.py`
- (And all files in `4_Backtesting/30_pipelines/` and `4_Backtesting/50_metrics/`)

## 3. Canonical Directories (Logic/Source)
- `1_Scrape/`
- `2_Projections/`
- `3_EV_Analysis/src/`
- `4_Backtesting/30_pipelines/`
- `nhl_bets/`

## 4. Generated Artifact Directories
- `3_EV_Analysis/` (CSV/XLSX outputs)
- `4_Backtesting/20_data_store/` (DuckDB)
- `4_Backtesting/60_reports/` (CSVs)
- `docs/audits/`
- `docs/accuracy/`
- `duckdb_temp/`
- `SingleGamePlayerPrediction/` (Duplicate of `2_Projections/` outputs)

## 5. Identified "Scatter" / Cleanup Candidates
- Root directory is cluttered with debug scripts (`debug_*.py`).
- `3_EV_Analysis/` contains both source (`src/`) and outputs.
- `SingleGamePlayerPrediction/` appears to be a redundant output folder.
- `nhl_bets/` and `3_EV_Analysis/src/` and `2_Projections/` have overlapping responsibilities for probability math.
- Many top-level `.csv`, `.xlsx`, and `.log` files.

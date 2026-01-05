# Repo Inventory (AFTER)
**Date:** 2026-01-05

## 1. Directory Structure (Summary)
The repository has been reorganized to separate logic, pipelines, and artifacts.

- `src/nhl_bets/`: Canonical logic (Protected).
- `pipelines/`: Production and Backtesting workflows.
- `outputs/`: All generated CSVs, XLSX, and reports.
- `data/`: DuckDB database and raw source data.
- `experiments/`: Research, validation, and temporary metrics.
- `docs/`: Governance, theory, and refactor records.
- `scripts/`: CLI utilities and validation helpers.
- `sandbox/`: Personal debug and temporary files.

## 2. File Movement Map (Old → New)

| Original Path | Refactored Path |
| :--- | :--- |
| `1_Scrape/` | `src/nhl_bets/scrapers/` |
| `2_Projections/` | `src/nhl_bets/projections/` |
| `3_EV_Analysis/src/` | `src/nhl_bets/analysis/` |
| `4_Backtesting/30_pipelines/` | `pipelines/backtesting/` |
| `4_Backtesting/60_reports/` | `outputs/backtest_reports/` |
| `4_Backtesting/70_models/` | `data/models/` |
| `4_Archived_BestBets/` | `outputs/archived_bets/` |
| `run_workflow.py` | `pipelines/production/run_production_pipeline.py` |
| `debug_*.py` (root) | `sandbox/` |
| `*.md` (root audits) | `outputs/audits/` |

## 3. Entrypoint Inventory
Core entrypoints are now clearly grouped in `pipelines/`:

- **Production:** `pipelines/production/run_production_pipeline.py`
- **Scraper:** `src/nhl_bets/scrapers/scrape_playnow_api.py`
- **Projection Model:** `src/nhl_bets/projections/single_game_probs.py`
- **EV Runner:** `src/nhl_bets/analysis/runner.py`
- **Backtest Engine:** `pipelines/backtesting/run_ev_backtest.py`

## 4. Verification Check
- Total Files: 213
- Main Guards Found: 22
- __init__.py Status: Added to all `src/` subdirectories to support package-level imports.

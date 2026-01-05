# Refactor Plan
**Date:** 2026-01-05

## 1. Proposed Target Structure

### `src/nhl_bets/` (Canonical Logic - Core Package)
- `scrapers/`: Content from `1_Scrape/` and `1_PlayerProps/scrapers/`.
- `projections/`: Logic from `2_Projections/` and `nhl_bets/probs/`.
- `analysis/`: Logic from `3_EV_Analysis/src/`.
- `common/`: Shared utilities, distributions, and constants.

### `pipelines/` (Workflows & Entrypoints)
- `production/`: `run_workflow.py` (renamed to `run_production_pipeline.py`) and supporting scripts.
- `backtesting/`: Scripts from `4_Backtesting/30_pipelines/` and `4_Backtesting/40_runner/`.

### `experiments/` (Research & Metrics)
- `metrics/`: Scripts from `4_Backtesting/50_metrics/`.
- `validation/`: `verify_phase6.py`, `analyze_phase7_results.py`, etc.

### `data/` (Data Store & Models)
- `db/`: `data/db/nhl_backtest.duckdb`.
- `models/`: `4_Backtesting/70_models/` (Calibrators).
- `raw/`: Raw CSV data sources.

### `outputs/` (Generated Artifacts & Reports)
- `ev_analysis/`: `3_EV_Analysis/*.csv/xlsx`.
- `backtest_reports/`: `4_Backtesting/60_reports/`.
- `audits/`: `docs/audits/` and `ev_prob_audit_*.csv`.
- `accuracy/`: `docs/accuracy/`.

### `scripts/` (CLI Utilities)
- `generate_best_bets.py`, `golden_run_validate.py`, etc.

### `sandbox/` (Temporary & Debug)
- All `debug_*.py` files from root.

---

## 2. File Movement Map (Examples)

| Old Path | New Path |
| --- | --- |
| `run_workflow.py` | `pipelines/production/run_production_pipeline.py` |
| `1_Scrape/scrape_playnow_api.py` | `src/nhl_bets/scrapers/playnow_api.py` |
| `2_Projections/single_game_probs.py` | `src/nhl_bets/projections/single_game_probs.py` |
| `3_EV_Analysis/src/ev.py` | `src/nhl_bets/analysis/ev.py` |
| `data/db/nhl_backtest.duckdb` | `data/db/nhl_backtest.duckdb` |
| `debug_db.py` | `sandbox/debug_db.py` |
| `2_Projections/MODEL_PROJECTION_THEORY.md` | `docs/MODEL_PROJECTION_THEORY.md` |

---

## 3. Preserving Behavior & Verification

- **Imports:** Update all imports to use absolute paths (e.g., `import nhl_bets.projections`).
- **Paths:** Use `pathlib` to ensure paths are relative to the project root, regardless of where the script is run.
- **Entrypoints:** Create shim scripts or update existing ones to point to the new logic locations.
- **Verification:**
    1. Run `python -m py_compile` on all moved files.
    2. Run `scripts/golden_run_validate.py` to ensure logic parity.
    3. Dry-run `pipelines/production/run_production_pipeline.py`.

---

## 4. Out of Scope
- Rewriting the mathematical logic.
- Updating the DuckDB schema.
- Changing the calibration fitting process.

# Leaderboard Governance

**Status:** Active
**Effective:** 2026-01-11

This document governs the **Master Backtest Leaderboard**, ensuring it remains a trusted record of model performance.

## 1. Run Classifications

### Official
- **Requirements:**
  1.  Generated via `run_production_pipeline.py` with a valid `--profile`.
  2.  Produced a `run_manifest_*.json` in `outputs/runs/`.
  3.  Produced an `eval_manifest_*.json` in `outputs/eval/`.
  4.  Used the standard Hold-Out Set (Nov 2023).
- **Usage:** Used for "Truth" comparisons and regression gates.

### Legacy Imported
- **Requirements:**
  1.  Found in `outputs/eval/` or `outputs/backtesting/` with parsable metrics.
  2.  Dates align with known historical phases.
- **Usage:** Historical context only. Do not use for regression gates.

### Excluded
- **Criteria:**
  1.  Missing metrics.
  2.  Metric outliers (Log Loss < 0.20 or > 0.40).
  3.  Known bugged code revisions.

## 2. Adding a New Official Run

1.  **Configure:** Create or select a profile in `config/`.
2.  **Execute:**
    ```powershell
    python pipelines/production/run_production_pipeline.py --profile my_new_profile
    ```
3.  **Evaluate:** (If not auto-triggered)
    ```powershell
    python pipelines/backtesting/evaluate_forecast_accuracy.py --table fact_probabilities_my_new_profile
    ```
4.  **Index:**
    ```powershell
    python scripts/index_historical_evals.py
    ```

## 3. Maintenance

The leaderboard is re-generated automatically by `scripts/index_historical_evals.py`. **Do not edit the Markdown file manually.**

## 4. Required Manifest Fields

The indexer (`scripts/index_historical_evals.py`) expects the following schema in `outputs/runs/run_manifest_*.json` or `outputs/eval/eval_manifest_*.json`:

### Essential Keys
*   `timestamp` (ISO 8601 string): Used to date the run.
*   `profile` (string): The config profile used (e.g., `production_experiment_b`).
*   `run_id` (string, optional): Unique identifier.

### Metrics (in Eval Manifests)
*   `metrics_global` (object):
    *   `log_loss` (float): **Primary Sorting Metric**.
    *   `brier_score` (float).

### Provenance (New in Gen 3)
*   `provenance` (object):
    *   `is_baseline` (bool)
    *   `profile_description` (string)


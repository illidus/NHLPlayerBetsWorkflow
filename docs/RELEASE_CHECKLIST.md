# Operational Release Checklist

Use this checklist to ensure the NHL Player Bets pipeline is functioning correctly after any infrastructure or orchestration changes.

## 1. Execution Modes

### A. Default Production Run
Standard daily run with calibrated probabilities for ASSISTS and POINTS.
- **Command**: `python run_workflow.py`
- **Expected Outputs**:
    - `outputs/ev_analysis/ev_bets_ranked.xlsx` (and .csv)
    - `ev_prob_audit_YYYY-MM-DD.md` (and .csv, .jsonl)

### B. Debug Mode (Calibration Disabled)
Forces use of raw distributional probabilities for all markets.
- **Command (PowerShell)**: `$env:DISABLE_CALIBRATION="1"; python run_workflow.py`
- **Command (Bash)**: `DISABLE_CALIBRATION=1 python run_workflow.py`
- **Verification**: Audit reports should show `ProbSource = Raw` for all entries.

### C. Accuracy Backtest
Full historical evaluation of model forecast quality.
- **Command (PowerShell)**: `$env:RUN_ACCURACY_BACKTEST="1"; python run_workflow.py`
- **Expected Outputs**:
    - `outputs/backtest_reports/forecast_accuracy.md`
    - `outputs/backtest_reports/forecast_accuracy_bins.csv`

---

## 2. Manual Verification Steps

1.  **Check Audit Source Selection**:
    Open the latest `ev_prob_audit_*.md` and verify:
    - **ASSISTS/POINTS**: `Probability Source` should be `Calibrated`.
    - **GOALS/SOG/BLOCKS**: `Probability Source` should be `Raw`.
2.  **Verify Calibration Logic Integrity**:
    Grep/Search codebase for `p_over_calibrated` to ensure the selection rule is active:
    - `Select-String "p_over_calibrated" 3_EV_Analysis/src/main.py`
3.  **Inspect Forecast Report**:
    Verify `forecast_accuracy.md` contains no `NaN` values and that the `Variant` column is present in all diagnostic tables.

---

## 3. Maintenance & Retraining

- **Calibrator Models**: Live in `data/models/calibrators_posthoc/`.
- **Retraining Command**: `python pipelines/backtesting/fit_posthoc_calibrators.py`.
- **MoneyPuck Sync**: `python pipelines/backtesting/download_moneypuck_team_player_gbg.py`.

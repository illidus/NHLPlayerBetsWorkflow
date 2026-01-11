# Operations: Model Refresh & Maintenance

**Status:** Active (Gen 3)
**Canonical Wrapper:** `pipelines/production/run_production_pipeline.py`

This document defines the standard operating procedures (SOP) for maintaining the NHL Player Bets projection system.

## 1. Daily Operations

### Generate Predictions (Production)
```powershell
python pipelines/production/run_production_pipeline.py --profile production_experiment_b
```
*   **Profile:** `production_experiment_b` (Gen 3)
*   **Behavior:** NB Distributions, Tail Calibration, Interactions.

### Generate Predictions (Baseline)
Use this to compare against "safe" legacy numbers if Prod looks suspicious.
```powershell
python pipelines/production/run_production_pipeline.py --profile baseline_profile
```

### Nightly Healthcheck
Runs every night to catch regressions early (7-day window).
*   **Command:** `scripts/windows_run_nightly_healthcheck.ps1`
*   **Output:** `outputs/eval/LATEST.md`

## 2. Weekly & Monthly Schedules

### Weekly Robustness Sweep
Validates the model against the full season history.
*   **Command:** `scripts/windows_run_weekly_robustness.ps1`
*   **Artifact:** `outputs/eval/robustness_leaderboard_primary_lines_*.md`

### Monthly Model Refresh
Retrains calibrators and re-optimizes parameters.
*   **Command:** `scripts/windows_run_monthly_refresh.ps1`
*   **Updates:** `data/models/` (Alphas, Calibrators).
*   **Action:** Must commit changes to git after successful run.

## 3. Manual Maintenance

### Tuning Betas & Interactions (Quarterly)
1.  Run `python scripts/optimize_betas.py`
2.  Run `python scripts/optimize_interaction_multipliers.py`
3.  Verify `outputs/beta_optimization/` json files.
4.  Update `config/production_profile.json` if paths changed (usually they stay constant).

### Emergency Rollback
**Do NOT edit the production profile to revert features.**
Instead, switch your operational command to use the **Baseline Profile**.

*   **Soft Rollback:** `... --profile baseline_profile`
*   **Hard Rollback:** See [SYSTEM_HISTORY_AND_ROLLBACK.md](SYSTEM_HISTORY_AND_ROLLBACK.md).
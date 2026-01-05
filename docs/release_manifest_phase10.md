# Release Manifest (Phase 10: Production Stability)

## 1. Canonical Entry Points
| Script | Purpose |
| :--- | :--- |
| `pipelines/production/run_production_pipeline.py` | Full daily workflow: Sync, Project, Analyze, Audit. |
| `scripts/golden_run_validate.py` | System health check and verification. |
| `scripts/generate_best_bets.py` | Final filtering and Excel report generation. |
| `pipelines/backtesting/evaluate_forecast_accuracy.py` | Backtest evaluation (Log Loss, Brier, ECE). |

## 2. Required Artifacts (Pre-Run)
- **Database**: `data/db/nhl_backtest.duckdb` (Must be populated with historical features).
- **Calibrators**: `data/models/calibrators_posthoc/` (Required for Assists/Points calibration).
- **Theory Docs**: `docs/MODEL_PROJECTION_THEORY.md` (Reference for all math).

## 3. Generated Artifacts (Post-Run)
- **Audit Logs**: `outputs/audits/ev_prob_audit_YYYY-MM-DD.md` (Full math trace).
- **Probabilities**: `outputs/projections/SingleGamePropProbabilities.csv`.
- **Bets**: `outputs/ev_analysis/BestCandidatesFiltered.xlsx`.

## 4. External Dependencies & Upstream Risks
- **MoneyPuck**: Subject to 403 Forbidden errors if scraped too aggressively.
- **Sportsbook API (PlayNow)**: Endpoint structure or tokenization may change without notice.
- **DuckDB**: Requires `SET memory_limit` and `SET threads` on high-concurrency environments to prevent OOM.

## 5. Phase 10 Operational Checklist
### Pre-Run
- [ ] Verify `nhl_backtest.duckdb` is not locked by another process.
- [ ] Ensure `data/models/` contains the latest joblib calibrators.
- [ ] Check internet connectivity for live API scraping.

### Execution
- [ ] Run `python pipelines/production/run_production_pipeline.py`.
- [ ] Monitor `outputs/audits/workflow_run.log` for execution errors.

### Post-Run / Verification
- [ ] Verify `ev_prob_audit_YYYY-MM-DD.md` contains expected game count.
- [ ] Check `BestCandidatesFiltered.xlsx` for valid EV ranges (0% to 25%).
- [ ] Confirm `ProbSource` column in audit reflects "Calibrated" for Assists/Points.

# Diff & Risk Summary (Phase 10)

## 1. Refactor Summary
The system has been restructured for "Production Stability" (Phase 10). Key changes include:
- **Centralized Orchestration**: Moving from fragmented scripts to `pipelines/production/run_production_pipeline.py`.
- **Auditability**: Implementation of the `AuditManager` to track every probability calculation back to its source (Raw vs Calibrated).
- **Directory Standardization**: Separation of `data/`, `outputs/`, `src/`, and `docs/` to follow standard Python project conventions.
- **Enhanced Logging**: Migration of debug output to `outputs/audits/workflow_run.log`.

## 2. Immutable Core (NO CHANGES)
The following mathematical components have been preserved strictly according to `MODEL_PROJECTION_THEORY.md`:
- **Distributions**: Poisson (G/A/P) and Negative Binomial (SOG/BLK) logic.
- **Hyperparameters**: Alpha values for SOG (0.35) and BLOCKS (0.60).
- **Rolling Windows**: L40 for Assists/Points, L10 for Goals, L20 for TOI.
- **SOG Formula**: The Corsi-Split/Thru% derivation remains untouched.

## 3. Git Push & Environment Risks
| Risk Area | Description | Mitigation |
| :--- | :--- | :--- |
| **Data Leakage** | Historical odds or proprietary results might be accidentally committed. | Strict adherence to `docs/git_preflight_audit.md` exclusion list. |
| **Path Portability** | Several scripts may still reference absolute paths (e.g., `C:\Users\Ryisa\...`). | Future Phase: Migrate to relative paths or `pathlib`. |
| **Secrets** | Scraper might contain API headers or session cookies. | Verified: `scrape_playnow_api.py` uses dynamic tokens where possible. |
| **Missing DB** | The repo is useless without `nhl_backtest.duckdb`. | Must provide a "DB Initialization" script or documentation for new users. |

## 4. Verification Verdict
Phase 10 is considered **STABLE**. All verification scripts in `scripts/golden_run_validate.py` pass. The math is consistent with historical baselines, and the system is ready for initial version control.

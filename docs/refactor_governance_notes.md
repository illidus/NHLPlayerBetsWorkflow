# Refactor Governance Notes
**Date:** 2026-01-05

## 1. Canonical & Protected Logic
The following are **NON-NEGOTIABLE** and must not be altered in logic, only in location/import:

- **Mathematical Distributions:** Poisson (Goals, Assists, Points) and Negative Binomial (SOG, BLOCKS) defined in `MODEL_PROJECTION_THEORY.md`.
- **Rolling Windows:** L10 (xG), L20 (Usage/TOI), L40 (Assists/Points/Thru%).
- **Mu Formulae:** `mu_base` and `mu_adj` calculations for all markets.
- **Multiplier Logic:** Opponent, Goalie, ITT, and B2B multipliers.
- **Calibration Policy:** `MARKET_POLICY` (Calibrated for Assists/Points, Raw for others).
- **DuckDB Schema:** The structure of `nhl_backtest.duckdb` and its core fact tables.

## 2. Generated Artifacts (Transient)
These can be moved or cleaned up without affecting system integrity:
- `*.csv`, `*.xlsx`, `*.jsonl`, `*.log` in root.
- `3_EV_Analysis/` output files.
- `4_Backtesting/60_reports/` files.
- `duckdb_temp/`

## 3. Experimental / Sandbox
- `debug_*.py` scripts in root.
- `4_Backtesting/50_metrics/` experiment scripts (preserve but move to experimental folder).

## 4. Disallowed Changes
- Changing distribution parameters (e.g., alpha values for SOG/BLK).
- Removing calibration layers.
- Modifying the `run_workflow.py` sequence (Sync -> Features -> Export -> Scrape -> Project -> Analyze).
## 5. Migration & Provenance
- **2026-01-05:** `docs/MODEL_PROJECTION_THEORY.md` was copied to `2_Projections/MODEL_PROJECTION_THEORY.md` to serve as the project's authoritative binding governance path as per Phase 10 compliance. The original in `docs/` remains as a backup/reference but `2_Projections/` is the active source of truth.

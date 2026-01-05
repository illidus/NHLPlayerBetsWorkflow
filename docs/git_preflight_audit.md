# Git-Preflight Audit Report (Phase 10)
**Date:** 2026-01-05
**Status:** READ-ONLY PRE-COMMIT AUDIT

## 1. Directory Size Breakdown
The following sizes represent the local environment. Items marked with (!!) MUST be excluded via `.gitignore`.

| Directory | Size | Git Policy |
| :--- | :--- | :--- |
| `data/` | 3.1 GB | (!!) EXCLUDE |
| `data/raw/` | 1.8 GB | (!!) EXCLUDE |
| `data/db/` | 1.2 GB | (!!) EXCLUDE |
| `outputs/` | 242.1 MB | (!!) EXCLUDE (Binary/CSV) |
| `src/` | 2.1 MB | INCLUDE |
| `pipelines/` | 1.2 MB | INCLUDE |
| `docs/` | 981.4 KB | INCLUDE |
| `sandbox/` | 341.6 KB | INCLUDE (Clean before push) |
| `scripts/` | 38.6 KB | INCLUDE |

## 2. Top 50 Codebase Files (By Complexity/Size)
Identified as high-priority for review before initial commit:
1. `src/nhl_bets/projections/single_game_probs.py` (Core math logic)
2. `src/nhl_bets/analysis/audit.py` (Audit trace generation)
3. `src/nhl_bets/scrapers/scrape_playnow_api.py` (API Interface)
4. `pipelines/backtesting/build_player_features.py` (Feature engineering)
5. `pipelines/production/run_production_pipeline.py` (Main Orchestrator)
6. `scripts/generate_best_bets.py` (Selection logic)
7. `src/nhl_bets/analysis/ev.py` (EV Calculation)
8. `src/nhl_bets/analysis/runner.py` (Batch execution)

## 3. Recommended .gitignore Entries
```gitignore
# --- Data and Database (Hard Exclude) ---
data/db/
data/raw/
duckdb_temp/
*.duckdb

# --- Large/Transient Outputs ---
outputs/**/*.csv
outputs/**/*.xlsx
outputs/**/*.jsonl
outputs/**/*.log
outputs/**/*.joblib
outputs/archived_bets/

# --- Models and Calibrators ---
data/models/
*.joblib

# --- Python Environment ---
.pytest_cache/
__pycache__/
.ipynb_checkpoints/
venv/
.env

# --- IDE/OS Specific ---
.vscode/
.idea/
.DS_Store
Thumbs.db
```

## 4. MUST NEVER COMMIT List
The following artifacts contain PII, proprietary data, or are too large for standard Git hosting:
- **`data/db/nhl_backtest.duckdb`**: Binary database containing full historical context.
- **`data/raw/`**: Thousands of JSON/CSV files from MoneyPuck and Odds providers.
- **`outputs/ev_analysis/*.xlsx`**: Final bet candidates (proprietary/transient).
- **`outputs/projections/*.csv`**: Raw distributional probabilities.
- **`data/models/calibrators/`**: Binary model weights (joblib).
- **Hardcoded API Keys**: Ensure no keys exist in `scrape_playnow_api.py` (use Env Vars).
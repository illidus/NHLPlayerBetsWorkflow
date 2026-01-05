# Project: NHL Player Bets Workflow (NHLPlayerBetsWorkflow)

This file is the **single source of truth** for a fresh Gemini CLI agent session. It defines the project’s
current state, non-negotiable constraints, and how to execute common tasks.

---

## 1. Environment & Status
- **Repo root:** `C:\Users\Ryisa\Documents\Scripts\NHLPlayerBetsWorkflow`
- **DuckDB Path:** `data/db/nhl_backtest.duckdb`
- **Core Orchestrator:** `pipelines/production/run_production_pipeline.py`
- **Current Phase:** Phase 10 (Production Stability) — *Active*
- **Scraper:** Direct API Client (`src/nhl_bets/scrapers/scrape_playnow_api.py`)

---

## 2. Statistical Model Definitions (Hard Constraints)
All projection logic and probability calculations must strictly adhere to the authoritative reference:
**[MODEL_PROJECTION_THEORY.md](docs/MODEL_PROJECTION_THEORY.md)**.

### Governance Documentation
- **Target Architecture**: [refactor_plan.md](docs/refactor_plan.md)
- **Constraint Enforcement**: [refactor_governance_notes.md](docs/refactor_governance_notes.md)
- **Mathematical Constitution**: [MODEL_PROJECTION_THEORY.md](docs/MODEL_PROJECTION_THEORY.md)

- **Distributions:**
    - **Poisson markets:** GOALS, ASSISTS, POINTS (discrete counts)
    - **Negative Binomial markets:** SOG, BLOCKS (over-dispersed; variance > mean)
      - Default alphas: `alpha_sog = 0.35`, `alpha_blk = 0.60`
- **Rolling Windows:**
    - **Assists/Points:** Use **Last 40 Games (L40)** for rate-based features to minimize Log Loss.
    - **Goals (xG):** Use **Last 10 Games (L10)** to capture current shot quality and form.
    - **Usage (TOI):** Use **Last 20 Games (L20)** to reflect recent deployment changes.
- **SOG Logic (Corsi-Split):** Mu is derived via `(Corsi L20 Rate * Thru% L40) * (Projected TOI / 60)`.
- **Governance:** Any deviation from the theory (e.g., changing distributions, altering base Mu formulas, or modifying multiplier logic) must be treated as a **formal feature request** and verified via backtesting. Silent edits or unverified "tweaks" are prohibited.
- **Blacklist:** Do not model or scrape "Goal Scorer" markets (settlement ambiguity).

---

## 3. Probability Outputs & Policy
The model generates two variants for specific markets to optimize calibration:

- **Columns:**
    - `p_over`: Raw distributional probability.
    - `p_over_calibrated`: Post-hoc calibrated probability (Platt/Isotonic).
- **MARKET_POLICY (Default Selection):**
    - **ASSISTS, POINTS:** Calibrated (`p_over_calibrated`) if available, else Raw.
    - **GOALS, SOG, BLOCKS:** Raw (`p_over`).
- **Debug Override:**
    - Set `DISABLE_CALIBRATION=1` to force the use of Raw probabilities for all markets.

---

## 4. Backtesting / Evaluation Standard
The system is evaluated on forecast quality, not ROI:

- **Metrics:** Brier Score, Log Loss, ECE (Expected Calibration Error), ROC AUC, Top-K Hit Rate.
- **Scope:** Accuracy and calibration only. No ROI/odds-based evaluation is performed as historical odds are unavailable.
- **Artifacts:** `forecast_accuracy.md` and `forecast_accuracy_bins.csv`.

---

## 5. Change Control & Experimentation
- **The Constitution:** `docs/MODEL_PROJECTION_THEORY.md` defines the allowed math.
- **Modification Protocol:** Any change to Mu definitions, distributions, calibration policy, or evaluation metrics requires:
    1. A formal feature request.
    2. A before/after accuracy report comparison showing improvement in scoring rules (Log Loss/Brier).
    3. An entry in a `PROJECTION_EXPERIMENT_REPORT_YYYY-MM-DD.md` following the standard in Section 12.

---

## 6. Workflow Topology
The standard run (`python pipelines/production/run_production_pipeline.py`) executes:
1. **Sync**: MoneyPuck data → DuckDB foundation.
2. **Features**: Rebuild player/goalie/team features.
3. **Export Base**: Generate `BaseSingleGameProjections.csv` from DuckDB features.
4. **Scrape**: API-based capture of live events and markets (`src/nhl_bets/scrapers/scrape_playnow_api.py`).
5. **Project**: Generate adjusted Mu and probabilities (Raw + Calibrated) via `src/nhl_bets/projections/single_game_probs.py`.
6. **Analyze**: EV computation using `MARKET_POLICY` with console reporting and audit generation.
7. **Audit**: Generate `ev_prob_audit_YYYY-MM-DD.*` reports with full math trace.
    - Fields: `ProbSource` (Calibrated/Raw), `source_prob_column`.
8. **Accuracy (Optional)**: If `RUN_ACCURACY_BACKTEST=1`, evaluate forecast quality.
9. **Candidates**: `python scripts/generate_best_bets.py`
    - Filter: EV% >= 2.0%, p_model >= 0.05, High-Odds (>15.0) require EV% >= 10.0.
    - Ranking: Market Priority (GOALS > ASSISTS > POINTS) then EV% Descending.
    - Artifact: `outputs/ev_analysis/BestCandidatesFiltered.xlsx`.

---

## 7. Operational Modes
- **Default (Production)**: `python pipelines/production/run_production_pipeline.py`
  - Uses `MARKET_POLICY` (Calibrated for Assists/Points, Raw for others).
- **Debug Raw**: `DISABLE_CALIBRATION=1 python pipelines/production/run_production_pipeline.py`
  - Forces the use of Raw probabilities for all markets in EV analysis.
- **Accuracy Evaluation**: `RUN_ACCURACY_BACKTEST=1 python pipelines/production/run_production_pipeline.py`
  - Runs accuracy metrics (Log Loss, Brier, ECE) after the main analysis.

---

## 8. Storage Conventions (DuckDB)
- **Raw API Responses:** `raw_playnow_responses` (traceability)
- **Normalized Markets:** `fact_playnow_markets` (ingestion foundation)
- **Backtesting Facts:** `fact_odds_props`, `fact_player_game_features`, `fact_skater_game_all`, etc.

---

## 9. Performance Pragmas (Anti-Freeze)
Execute immediately on every DB connection:
1. `SET memory_limit = '8GB';`
2. `SET threads = 8;`
3. `SET temp_directory = './duckdb_temp/';`

## 10. Context Management & Token Efficiency
- **Large Files:** NEVER use `read_file` on large artifacts (CSV, XLSX, Log files, or DuckDB binaries). 
- **Sampling:** Use `run_shell_command` with `head`, `tail`, or `grep` to inspect large text files.
- **SQL:** Query DuckDB directly via CLI for data inspection rather than loading dataframes.
- **Clean Slate:** If a script becomes cluttered with failed edits, prefer `write_file` with a fresh, clean version over multiple `replace` calls.

## 11. Custom Commands / Shortcuts
- `/backtest:compare` — full baseline vs. calibrated backtest (heavy)
- `/report:view` — quick view of forecast accuracy results.
- `/plan` — generate strategic blueprints for new features.

---

## 12. Experiment Recording Standard
All model logic experiments must be documented in a dedicated markdown file with the following sections:
1. **Metadata:** Date, Objective, Logic version tags, and **Data Scope** (Seasons used, Date range, Total player-game sample size).
2. **Logic Definition:** Clear pseudocode or Python snippet of the change (e.g., L20 vs L40 window).
3. **Comparative Metrics:** Side-by-side table of Log Loss, Brier, and ECE.
4. **Calibration Sanity:** Confirmation of monotonicity in reliability diagrams.
5. **Verdict:** Final decision to merge or discard based on scoring rule improvements.

---

## 13. Audit & Verification Guidelines

### Audit Interpretation Notes
- **Duplicate Markets:** Markets may appear twice in audit logs due to explicit **Over/Under evaluation**.
- **Symmetry Check:** If `p_over` and `p_under` sum to approximately ~1.0, the system is behaving correctly.
- **Isotonic Bucketing:** Isotonic calibration is **step-quantized** by design. Multiple different raw probabilities mapping to the exact same calibrated probability (e.g., 0.8542) is expected behavior, not a bug.

### Post-Run Checklist
1. **GOALS Integrity:** GOALS EV% can be positive but must be verified if it deviates significantly from implied odds (no unexplained extreme outliers).
2. **High EV Validation:** Any ASSISTS/POINTS EV > 10% → Verify sample size and confirm it aligns with a known calibrator bucket.
3. **Row Duplication:** If duplicate `market_key` rows exist, first check for Over/Under symmetry before investigating join defects.

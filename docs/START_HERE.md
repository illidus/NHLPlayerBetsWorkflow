# NHL Model: Start Here

**Current Era:** Generation 3 (Experiment B)
**Status:** Production Active

Welcome to the NHL Player Prop Modeling repository. This document is the primary entry point for developers and operators.

## 1. Quick Links
*   **📂 [Docs Index](DOCS_INDEX.md):** Full inventory of documentation.
*   **📜 [System History & Rollback](SYSTEM_HISTORY_AND_ROLLBACK.md):** The "Truth" about what version is running.
*   **📊 [Leaderboard](../outputs/eval/MASTER_BACKTEST_LEADERBOARD.md):** The official record of model accuracy.
*   **📐 [Model Theory](MODEL_PROJECTION_THEORY.md):** The mathematical rules (Negative Binomial, Calibration).

## 2. Production State (The "Truth")
*   **Active Profile:** `production_experiment_b` (Gen 3) (File: `config/production_profile.json`)
    *   *Note: Do not edit `config/production_experiment_b.json` directly. Edit `config/production_profile.json` and run `python scripts/sync_profile_aliases.py` to keep them in sync.*
*   **Key Invariants:**
    *   **Variance:** `all_nb` (Negative Binomial for Scoring/SOG/Blocks).
    *   **Calibration:** `tail_bucket` (Targeted for probabilities > 30%).
    *   **Interactions:** Enabled (Goalie vs Shooter types).

## 3. Standard Commands

### Run Production (Gen 3)
```powershell
python pipelines/production/run_production_pipeline.py --profile production_profile
```

### Run Baseline (Gen 2 / Comparison)
```powershell
python pipelines/production/run_production_pipeline.py --profile baseline_profile
```

### Rollback (Emergency)
**Soft:** Use the Baseline command above.
**Hard:** If profile loader breaks, use environment variables:
```powershell
$env:DISABLE_CALIBRATION="1"; $env:NHL_BETS_VAR_MODE="off"
python pipelines/production/run_production_pipeline.py
```

## 4. Locked vs. Experimental
**🔒 LOCKED FILES** (Do not edit without strict governance):
*   `pipelines/production/run_production_pipeline.py`
*   `src/nhl_bets/projections/produce_game_context.py`
*   `src/nhl_bets/projections/single_game_probs.py`

**🧪 EXPERIMENTAL:**
*   `docs/phase11_historical_odds/`
*   `docs/phase12_odds_api/`
*   *Note: Do not import experimental code into locked paths.*

## 5. Testing
To run the test suite:
```powershell
pytest
```
*Note: If collection fails, ensure `src` is in your PYTHONPATH or use `python -m pytest`.*

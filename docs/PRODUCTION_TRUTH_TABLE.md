# Production Truth Table

**Version:** 1.0
**Date:** 2026-01-11

This table maps the functional differences between the Legacy/Baseline system and the Current Production system. Use this to verify environment configuration and debug regressions.

| Feature / Component | **Old Baseline (Gen 2 / Exp A)** | **Current Production (Gen 3 / Exp B)** | Source of Truth |
| :--- | :--- | :--- | :--- |
| **Profile Name** | (None/Implicit) | `production_experiment_b` | `config/production_profile.json` |
| **Pipeline Command** | `python run_production_pipeline.py` (No flags) | `python run_production_pipeline.py --profile production_profile` | `pipelines/production/` |
| **Goals Distribution** | Poisson | **Negative Binomial** | `variance_mode='all_nb'` |
| **Assists Distribution** | Poisson | **Negative Binomial** | `variance_mode='all_nb'` |
| **SOG Distribution** | Negative Binomial | Negative Binomial | `variance_mode='all_nb'` |
| **Calibration Mode** | `raw` (None) | **`tail_bucket`** | `calibration_mode` flag |
| **Interactions** | Disabled | **Enabled** (Goalie vs Shooter) | `use_interactions=True` |
| **Scoring Alphas** | Fixed Defaults (0.10/0.15) | **Optimized** (Load from file) | `data/models/alpha_overrides/` |
| **Betas (Multipliers)** | Fixed Defaults | **Optimized** (Load from file) | `outputs/beta_optimization/` |
| **Logic Window (Goals)**| L10 (Process) | L10 (Process) | `MODEL_PROJECTION_THEORY.md` |
| **Logic Window (SOG)** | L20 Vol / L40 Eff | L20 Vol / L40 Eff | `MODEL_PROJECTION_THEORY.md` |
| **Evaluation Metric** | Log Loss (Global) | Log Loss (Global) | `evaluate_forecast_accuracy.py` |
| **Key Benchmark** | ~0.2564 | **~0.2553** | `outputs/eval/` |

---

## File & Path Mapping

| Component | Path |
| :--- | :--- |
| **Profile Config** | `config/production_profile.json` |
| **Logic Script** | `src/nhl_bets/projections/single_game_probs.py` |
| **Theory Doc** | `docs/MODEL_PROJECTION_THEORY.md` |
| **Alpha Overrides** | `data/models/alpha_overrides/best_scoring_alphas.json` |
| **Calibrators** | `data/models/calibrators_posthoc/*.joblib` |

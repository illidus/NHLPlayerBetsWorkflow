# Artifacts Index

This document maps key output files to their locations, bridging the gap between documentation and artifacts.

## 🏆 Evaluation & Backtesting
*   **Master Leaderboard:** `outputs/eval/MASTER_BACKTEST_LEADERBOARD.md`
    *   *The single source of truth for model performance.*
*   **Run Manifests:** `outputs/runs/run_manifest_*.json`
    *   *Proof of configuration for every official run.*
*   **Eval Manifests:** `outputs/eval/eval_manifest_*.json`
    *   *Detailed metrics linked to a specific run.*

## 🧠 Model Assets (Gen 3)
*   **Production Profile:** `config/production_profile.json` (Logical Name: `production_experiment_b`)
*   **Scoring Alphas:** `data/models/alpha_overrides/best_scoring_alphas.json`
*   **Betas (Coefficients):** `outputs/beta_optimization/final_betas.json`
*   **Interactions:** `outputs/beta_optimization/optimized_interactions.json`
*   **Tail Calibrators:** `data/models/calibrators_posthoc/*.joblib`

## 🔮 Projections
*   **Latest Single Game Probs:** `outputs/projections/SingleGamePropProbabilities.csv`
*   **Latest EV Analysis:** `outputs/ev_analysis/ev_bets_ranked.xlsx`
*   **Game Context:** `outputs/projections/GameContext.csv`

## 🗄️ Database
*   **DuckDB:** `data/db/nhl_backtest.duckdb`
    *   *Contains all historical stats, odds, and results.*

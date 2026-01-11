# NHL Player Prop Projection Theory

**Effective Date:** 2026-01-11
**Status:** Active (Generation 3 / Experiment B)

## Purpose

This document defines the statistically sound methodology used to generate player-level base projections (`mu_base`) and adjusted game projections (`mu_adj`) for all NHL player prop markets in this repository.

It exists to:
- Prevent high-variance modeling mistakes.
- Ensure consistency across agents and pipeline phases.
- Separate repeatable process from outcome noise.
- Provide a defensible theoretical foundation for EV analysis.

---

## 1. Generation Overlays (Versioning)

The model logic is versioned by "Generation" to track significant theoretical shifts.

### **Generation 3: "All-NB + Tail Calibration" (Current Production)**
- **Profile:** `production_experiment_b` (or `production_profile.json`)
- **Core Change:** Switched **GOALS, ASSISTS, and POINTS** from Poisson to Negative Binomial (`variance_mode='all_nb'`) to better model the over-dispersion observed in elite player outcomes.
- **Calibration:** Introduced "Tail Bucket" calibration (`calibration_mode='tail_bucket'`) which uses specific calibrators for high-probability tails (>30%), improving fit for best-bet candidates.
- **Interactions:** Enabled interaction terms between Shooter Types and Goalie Quality.
- **Optimization:** Uses externally optimized Scoring Alphas and Betas loaded from JSON.

### **Generation 2: "Baseline" (Legacy)**
- **Profile:** `baseline_profile.json`
- **Core Logic:** Poisson distribution for Goals/Assists/Points; NB only for SOG/Blocks.
- **Calibration:** None (Raw probabilities).
- **Interactions:** Disabled.
- **Reference:** See [docs/archive/MODEL_PROJECTION_THEORY_PREGEN3.md](docs/archive/MODEL_PROJECTION_THEORY_PREGEN3.md) for the pure Gen 2 spec.

---

## 2. Base Modeling Principles (Universal)

Low-frequency events (e.g., goals) must be modeled using **process-based rates** rather than short-window realized outcomes.

The model explicitly separates:
1. **Opportunity & Quality** (repeatable process).
2. **Finishing Variance** (secondary, shrunk, optional).

Discrete realized outcomes over small windows must never be used as Poisson lambdas for low-frequency events.

---

## 3. Market-by-Market Projection Definitions

### GOALS — Player 1+ Goals

**Base mean (process):**
```text
mu_base_goals = xg_per_60_L10 * (avg_toi_minutes_L10 / 60)
```

**Environmental adjustments:**
```text
Mult_opp = (opp_xga60 / lg_avg_xga60) ** beta_opp
Mult_goalie = (1 - (gsax60 / xga60)) ** beta_goalie
mu_adj_goals = mu_base_goals * Mult_opp * Mult_goalie * Mult_itt * Mult_b2b
```

**Gen 3 Distribution:** Negative Binomial
- **Alpha:** Optimized per-market (loaded from `best_scoring_alphas.json`).
- **Rationale:** Poisson under-estimates the variance of elite goal scorers who tend to score in bunches.

### SHOTS ON GOAL (SOG)

**Base mean (Corsi-Split Logic):**
```text
mu_base_sog = (corsi_per_60_L20 * thru_pct_L40) * (avg_toi_minutes_L10 / 60)
```

**Gen 3 Distribution:** Negative Binomial (`alpha = 0.35`)

### ASSISTS & POINTS

**Base mean:**
Uses **Last 40 Games (L40)** window for rates to filter noise, applied to L20-projected TOI split (EV vs PP).

**Gen 3 Distribution:** Negative Binomial
- **Alpha:** Optimized per-market.
- **Rationale:** Poisson assumes variance = mean, but star playmakers exhibit "hot streaks" (variance > mean).

---

## 4. Calibration Layer (Gen 3)

### Tail Bucket Calibration
Instead of calibrating the entire probability curve (which can over-fit low-probability noise), Gen 3 focuses on the **"investable tail"** (Prob > 30%).

- **Method:** Isotonic Regression trained only on high-mu samples.
- **Implementation:** `calib_tail_{MARKET}_{TYPE}.joblib`
- **Control:** Enabled via `calibration_mode='tail_bucket'`.

---

## 5. Interactions (Gen 3)
Gen 3 introduces interaction terms to model how specific player archetypes perform against specific goalie types.

- **Example:** "Volume Shooter vs Poor Goalie" might get a `1.05` multiplier, while "Sniper vs Elite Goalie" gets `0.92`.
- **Source:** Coefficients loaded from `optimized_interactions.json`.

---

## 6. Reproducibility

To reproduce specific model behaviors, use the appropriate profile:

**Run Generation 3 (Production):**
```powershell
python pipelines/production/run_production_pipeline.py --profile production_profile
```

**Run Generation 2 (Baseline):**
```powershell
python pipelines/production/run_production_pipeline.py --profile baseline_profile
```
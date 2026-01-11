# NHL Player Prop Projection Theory

## Purpose

This document defines the statistically sound methodology used to generate player-level base projections (`mu_base`) and adjusted game projections (`mu_adj`) for all NHL player prop markets in this repository.

It exists to:
- Prevent high-variance modeling mistakes.
- Ensure consistency across agents and pipeline phases.
- Separate repeatable process from outcome noise.
- Provide a defensible theoretical foundation for EV analysis.

All projection logic must conform to this document unless a feature request explicitly modifies it.

---

## Core Modeling Principle

Low-frequency events (e.g., goals) must be modeled using **process-based rates** rather than short-window realized outcomes.

The model explicitly separates:
1. **Opportunity & Quality** (repeatable process).
2. **Finishing Variance** (secondary, shrunk, optional).

Discrete realized outcomes over small windows must never be used as Poisson lambdas for low-frequency events.

---

## Definitions & Notation

- `mu_base`: Expected count for a player in a single game in a neutral environment.
- `mu_adj`: Final game-specific mean after environmental adjustments.
- `p_over`: Final probability of exceeding a specific line.
- `xG`: Expected goals, the summed probability of unblocked shots becoming goals.
- `TOI`: Expected time on ice (minutes).
- `L10`: Rolling window using the previous 10 games only (no lookahead).
- `Per-60`: Rate normalized to 60 minutes of ice time.
- `Mult`: Multiplier applied to base Mu.

---

## Market-by-Market Projection Definitions

### 1. GOALS — Player 1+ Goals

#### ❌ Prohibited Inputs
The following MUST NOT be used directly or indirectly as the Poisson lambda:
- Goals in last N games.
- Goals per game over small windows (L5, L10, etc.).
- Discrete recent goal counts.
- Raw shooting percentage as a primary multiplier.

#### ✅ Base Process Signal (Required)
Goals must be modeled from expected goals (xG).

**Base mean (process):**
```text
mu_base_goals = xg_per_60_L10 * (avg_toi_minutes_L10 / 60)
```

**Environmental adjustments (Phase 8 "Brain"):**

**Opponent multiplier:**
```text
Mult_opp = (opp_xga60 / lg_avg_xga60) ** beta_opp
```

**Goalie quality multiplier:**
```text
GSAx60 = (sum_xGA_L10 - sum_GA_L10) / (sum_TOI_L10 / 3600)
Mult_goalie = (1 - (gsax60 / xga60)) ** beta_goalie
Mult_goalie = min(1.5, max(0.5, Mult_goalie))
```

**Final mean:**
```text
mu_adj_goals = 
    mu_base_goals 
    * Mult_opp 
    * Mult_goalie 
    * Mult_itt 
    * Mult_b2b
```

**Probability:**
```text
p_1plus = 1 - exp(-mu_adj_goals)
```

**Distribution:** Poisson

#### Optional: Finishing Skill Residual (Disabled by Default)
Finishing skill MAY be added only as a residual over xG (never via raw shooting %).

```text
R = career_goals / career_xg
R_tilde = (career_xg * R + K * 1.0) / (career_xg + K)
mu_adj_goals = mu_adj_goals * (R_tilde ** beta_finish)
```

**Defaults:**
- `K = 30.0`
- `beta_finish = 0.5`
- Disabled unless explicitly enabled via configuration.

---

### 2. SHOTS ON GOAL (SOG)

**Base mean:**
```text
mu_base_sog = sog_per_60_L10 * (avg_toi_minutes_L10 / 60)
```

**Adjustments:**
```text
Mult_opp = (opp_sa60 / lg_avg_sa60) ** beta_opp
mu_adj_sog = mu_base_sog * Mult_opp * Mult_b2b
```

**Distribution:** Negative Binomial (`alpha = 0.35`)

---

### 3. BLOCKED SHOTS (BLK)

**Base mean:**
```text
mu_base_blk = blocks_per_60_L10 * (avg_toi_minutes_L10 / 60)
```

**Adjustments:**
```text
Mult_opp = (opp_sa60 / lg_avg_sa60) ** beta_opp
mu_adj_blk = mu_base_blk * Mult_opp * Mult_b2b
```

**Distribution:** Negative Binomial (`alpha = 0.60`)

---

### 4. ASSISTS — Total Assists 0.5

**Base mean (Enhanced Split-Process):**
Assists are modeled by splitting projected TOI into Even Strength (EV) and Power Play (PP) components, applying process-driven rates for each.

```text
proj_pp_toi = proj_toi * (pp_toi_L20 / (ev_toi_L20 + pp_toi_L20))
proj_ev_toi = proj_toi - proj_pp_toi

mu_ev_ast = ev_ast_60_L20 * (proj_ev_toi / 60)
mu_pp_ast = pp_ast_60_L20 * (proj_pp_toi / 60)
mu_base_ast = mu_ev_ast + mu_pp_ast
```

**Adjustments:**
```text
Mult_opp = (opp_xga60 / lg_avg_xga60) ** beta_opp
mu_adj_ast = mu_base_ast * Mult_opp * Mult_goalie * Mult_itt * Mult_b2b
```

**Distribution:** Poisson

---

### 5. POINTS — Total Points 0.5

**Base mean (Enhanced Split-Process):**
Points utilize a process-driven approach combining on-ice expected goals (`on_ice_xG`) and player involvement (`IPP`).

```text
mu_ev_pts = (ev_ipp_x_L20 * ev_on_ice_xg_60_L20) * (proj_ev_toi / 60)
mu_pp_pts = (pp_ipp_x_L20 * pp_on_ice_xg_60_L20) * (proj_pp_toi / 60)
mu_base_pts = mu_ev_pts + mu_pp_pts
```

**Adjustments:**
```text
Mult_opp = (opp_xga60 / lg_avg_xga60) ** beta_opp
mu_adj_pts = mu_base_pts * Mult_opp * Mult_goalie * Mult_itt * Mult_b2b
```

**Distribution:** Poisson

---

## Post-hoc Calibration Layer

To optimize probability scale for markets with complex variance structures (specifically **ASSISTS** and **POINTS**), the model applies an optional post-hoc calibration layer after the initial distributional probability calculation.

- **Method**: **Isotonic Regression** (default) or Platt Scaling. Isotonic is preferred for its non-parametric flexibility in capturing non-linear calibration curves.
- **Mapping**:
  - `ASSISTS`: Calibrated via `calib_posthoc_ASSISTS.joblib`
  - `POINTS`: Calibrated via `calib_posthoc_POINTS.joblib`
- **Objective**: Correct systematic miscalibration (e.g., over/under-predicting specific probability bins) to improve proper scoring rules (Log Loss and Brier Score).
- **Independence**: Calibration does not change the ordinal ranking of players by probability; it only transforms the scale to better match observed frequencies.
- **Auditing**: Raw probabilities remain available in the pipeline for auditing and diagnostics.
- **Control**: The calibration layer can be bypassed by setting the environment variable `DISABLE_CALIBRATION=1`.

---

## Probability Clipping & Numerical Stability

To prevent infinite log-loss during evaluation and ensure stability in calibration transforms, the following clipping rules are applied:

1. **Calibration Layer Clipping**: Calibrated probabilities are clipped to `[1e-6, 1 - 1e-6]`.
2. **Log Loss Evaluation Clipping**: During accuracy evaluation, all probabilities are clipped to `[1e-15, 1 - 1e-15]` before computing `log_loss`.
3. **EV Analysis**: Probabilities used in EV analysis are not clipped unless they come from the calibration layer.

---

## Cross-Market Design Rules

- All `mu` values must be **continuous**.
- All low-frequency events must be **process-driven** (xG-based for goals).
- Finishing adjustments must be **secondary and shrunk**.
- Short-window discrete outcomes must not be used as lambdas.
- Distribution choices (Poisson vs. Negative Binomial) are fixed per market.

---

## Operational Guardrails

- `mu_adj` precision reports must be monitored daily.
- Excessive clustering of probabilities is a warning sign of stale features.
- Extreme EV values (>25%) should trigger audit review, not blind acceptance.

---

## Model Accuracy

In environments where historical betting odds are unavailable, model performance is evaluated using **accuracy and calibration metrics only**. This ensures the model remains a reliable probability estimator regardless of market availability.

Key metrics include:
- **Brier Score**: Measures overall forecast error.
- **Log Loss**: Penalizes confident but wrong predictions.
- **Expected Calibration Error (ECE)**: Measures how well predicted probabilities match observed frequencies.
- **ROC AUC**: Evaluates the model's discriminative power.
- **Top-K Hit Rate**: Validates the ordinal ranking of players by probability.

---

## Change Control & Governance

Any deviation from this document requires:
1. A written rationale.
2. Backtesting or calibration evidence.
3. Explicit documentation updates in this file.

Silent changes to multipliers or distributions are prohibited.

# Production Model Justification (Accuracy Proof)

This document provides the statistical evidence for the current "Production" model configuration. It explains why we use specific probability variants for each market.

## 1. Summary of Accuracy by Market

| Market | Best Model Variant | Production Policy | Log Loss Improvement | Justification |
| :--- | :--- | :--- | :---: | :--- |
| **ASSISTS** | **Isotonic Calibration** | `p_over_calibrated` | **+10.1%** | The raw Poisson distribution underestimates the "long tail" of elite playmakers. Calibration corrects this structural bias. |
| **POINTS** | **Isotonic Calibration** | `p_over_calibrated` | **+4.6%** | High-scoring lines create correlations that standard Poisson distributions miss. Calibration brings the model closer to reality. |
| **GOALS** | **Raw Poisson** | `p_over` (Raw) | **0.0%** | Goal scoring is a rare, independent event that fits the standard Poisson distribution almost perfectly. Calibration adds no value here. |
| **SOG** | **Raw Negative Binomial** | `p_over` (Raw) | **0.0%** | Our NegBinom model (Alpha=0.35) is already highly accurate for "clumpy" shot data. Post-processing doesn't yield measurable gains. |
| **BLOCKS** | **Raw Negative Binomial** | `p_over` (Raw) | **0.0%** | Similar to SOG, the dispersion-adjusted distribution already handles the variance effectively. |

---

## 2. Why "Production" is the Global Optimal
The **Production Model** is a hybrid policy. It selects the winner from all backtested variants:

1.  **Baseline:** The raw mathematical distribution (Poisson/NegBinom).
2.  **Calibrated:** Post-hoc adjustment using Isotonic Regression.
3.  **Experimental:** Feature-based machine learning (HGB/LogReg).

**Current Log Loss Comparison:**
- **Production Hybrid:** **0.3255** (Lowest/Best)
- **Pure Raw:** 0.3325
- **Experimental (HGB):** 0.6647 (Poor - Overfit)

*Note: A Log Loss difference of >0.01 is considered a massive "win" in sports betting forecasting.*

---

## 3. How to Verify
This logic is hard-coded in the project configuration to prevent accidental "regression" to less accurate models:

- **Config File:** `src/nhl_bets/projections/config.py`
- **Variable:** `MARKET_POLICY`

To audit a specific live bet and see these models in action, use the forensic tool:
```bash
python scripts/forensics/run_topx_forensic_audit.py --top-x 20
```
This will generate an Excel file showing exactly which variant was selected and the math behind it.

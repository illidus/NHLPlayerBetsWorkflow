# System History & Rollback Procedures

**Last Updated:** 2026-01-11
**Status:** Active

This document tracks the evolution of the NHL Betting Model's production configuration ("Truth"), identifying official generations, their performance justifications, and safe rollback procedures.

---

## 1. System Timeline

### Generation 3: "All-NB + Tail Calibration" (Current Production)
- **Active Since:** Jan 2026
- **Profile Name:** `production_experiment_b` (File: `config/production_profile.json`)
- **Key Changes:**
  - **Distributions:** Switched ALL markets to Negative Binomial (`variance_mode='all_nb'`).
  - **Calibration:** "Tail Bucket" calibration (`calibration_mode='tail_bucket'`).
  - **Interactions:** Enabled (`use_interactions=True`).
- **Justification:** 
  - Reduced Global Log Loss to **0.2553**.
  - Improved Brier Score to **0.0771**.

### Generation 2: "Experiment A" (Baseline)
- **Status:** Baseline Reference
- **Profile Name:** `baseline_profile` (File: `config/baseline_profile.json`)
- **Configuration:**
  - **Distributions:** Poisson for Goals/Assists/Points.
  - **Calibration:** Raw (Uncalibrated).
  - **Interactions:** Disabled.

---

## 2. Running Specific Versions

### Run Current Production (Gen 3)
```powershell
python pipelines/production/run_production_pipeline.py --profile production_experiment_b
```

### Run Baseline (Gen 2)
```powershell
python pipelines/production/run_production_pipeline.py --profile baseline_profile
```

### Side-by-Side Comparison
To run both sequentially (useful for verifying deltas):
```powershell
python pipelines/production/run_production_pipeline.py --profile baseline_profile
python pipelines/production/run_production_pipeline.py --profile production_profile
```

---

## 3. Rollback Procedure

If the current production model shows critical instability, revert to the baseline profile.

### Standard Rollback
**Action:** Switch the active profile flag in your daily operations script.

```powershell
python pipelines/production/run_production_pipeline.py --profile baseline_profile
```

### Emergency Bypass (Profile System Failure)
**Action:** If the profile loader itself is broken, use environment variables to force a raw state.

```powershell
$env:DISABLE_CALIBRATION="1"
$env:NHL_BETS_VAR_MODE="off"
python pipelines/production/run_production_pipeline.py
```

---

## 4. Regression Gates

Before promoting any new generation (Gen 4+):
1. **Must beat Gen 3** in Global Log Loss on the standard hold-out set.
2. **Must produce** a valid `run_manifest_*.json`.

---

## 5. Rollback Verification Checklist

Use this to confirm you have successfully reverted behavior.

### Baseline Verification (Gen 2)
1.  Run: `python pipelines/production/run_production_pipeline.py --profile baseline_profile`
2.  Check Log Output:
    *   `Variance Mode: off`
    *   `Calibration Mode: raw`
    *   `Use Interactions: False`
3.  Check Manifest (`outputs/runs/latest...`):
    *   `"profile": "baseline_profile"`

### Production Verification (Gen 3)
1.  Run: `python pipelines/production/run_production_pipeline.py --profile production_experiment_b`
2.  Check Log Output:
    *   `Variance Mode: all_nb`
    *   `Calibration Mode: tail_bucket`
    *   `Use Interactions: True`
3.  Check Manifest:
    *   `"profile": "production_experiment_b"`

### Hard Rollback Verification (Emergency)
1.  Set Env Vars: `$env:DISABLE_CALIBRATION="1"; $env:NHL_BETS_VAR_MODE="off"`
2.  Run without profile: `python pipelines/production/run_production_pipeline.py`
3.  Check Log Output:
    *   Should warn "No Profile Specified"
    *   Should show "CALIBRATION DISABLED BY ENVIRONMENT VARIABLE"
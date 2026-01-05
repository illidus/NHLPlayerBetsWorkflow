# EV Outlier Investigation Report (2026-01-05)

## 1. Executive Summary
Investigated implausibly high EV% (e.g., > +100%) in the "Player 1+ Goals" market for the January 6, 2026 slate. The issue was identified as a plumbing defect in the live projection data builder which caused extreme over-extrapolation of single-game performance for returning players.

## 2. Root Cause Determination
**Category B: Team/Date Disambiguation & Filter Failure**

The production script `2_Projections/produce_live_base_projections.py` had a restrictive date filter:
```sql
WHERE b.game_date >= '2025-09-01'
```
This filter excluded all history from the 2024-2025 season. For players like **Nico Sturm** (MIN) or **Tyler Kleven** (OTT), who have extensive career data but had only played a handful of games in the *current* season window (since Sept 2025), the "Last 10 Games" (L10) window was being calculated from a **sample size of 1 game**.

In Nico Sturm's case, he produced 1.186 xG in his lone season game. The model treated this as his stable per-game mean, leading to an elite projection (~0.25 Mu) and +109% EV against odds of 9.50.

## 3. Resolution (Plumbing Only)
The fix involved widening the data retrieval window to ensure the model has access to sufficient history to fill the L10/L20 windows even for returning or low-usage players.

**Changes made to `2_Projections/produce_live_base_projections.py`:**
1.  **Date Filter:** Widened `WHERE b.game_date >= '2025-09-01'` to `'2024-09-01'`.
2.  **Observability:** Updated the `GP` column mapping from a hardcoded `1` to `games_used_L10`. This allows the EV Analysis phase to see exactly how many games informed the projection.

## 4. Before vs. After Comparison (Top 10 Outliers)

| Player | Market | Odds | Before EV% | **After EV%** | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Nico Sturm | 1+ Goals | 9.50 | +109.38% | **-75.40%** | ✅ Fixed |
| Tyler Kleven | 1+ Goals | 16.00 | +96.00% | **-79.20%** | ✅ Fixed |
| Ryan Winterton | 1+ Goals | 10.00 | +64.80% | **-83.09%** | ✅ Fixed |
| Emmitt Finnie | 1+ Goals | 5.25 | +63.01% | **-83.58%** | ✅ Fixed |
| Ben Meyers | 1+ Goals | 10.00 | +63.00% | **-85.92%** | ✅ Fixed |
| Fabian Zetterlund| 1+ Goals | 4.25 | +49.56% | **-81.98%** | ✅ Fixed |
| Jack McBain | 1+ Goals | 7.50 | +40.70% | **-85.58%** | ✅ Fixed |
| Andrew Copp | 1+ Goals | 5.00 | +40.00% | **-83.85%** | ✅ Fixed |
| Nick DeSimone | 1+ Goals | 20.00 | +37.00% | **-87.93%** | ✅ Fixed |
| Marcus Foligno | 1+ Goals | 9.50 | +36.04% | **-86.99%** | ✅ Fixed |

## 5. Verification Gate Status
- **Mu Precision Guardrail:** PASS (using 6-decimal precision)
- **Calibration Policy Enforcement:** PASS
    - ASSISTS/POINTS -> Calibrated
    - GOALS/SOG/BLOCKS -> Raw
- **Golden Run Validation:** PASS (Default, Debug, and Backtest modes)
- **Artifacts Verified:** `BestCandidatesFiltered.xlsx` no longer contains the extreme outliers.

## 6. Commands Run
```powershell
python 2_Projections/produce_live_base_projections.py
python scripts/golden_run_validate.py
python run_workflow.py
```

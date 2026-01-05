# NHL Projection Experiment Report (Jan 5, 2026)

## 0. Metadata
- **Date:** 2026-01-05
- **Objective:** Validate rolling window stability (L40 vs L20) and Corsi-Split SOG logic.
- **Logic Version:** `v2.1-l40-corsi`
- **Data Scope:** Full 2023, 2024, and partial 2025 NHL Regular Seasons.
- **Sample Size:** 117,775 player-game snapshots.
- **Source:** MoneyPuck (fact_skater_game_all / fact_goalie_game_all).

## Executive Summary
This report summarizes the investigation into optimal statistical windows and methodologies for NHL player prop projections. The primary goal was to determine if **Last 10 (L10)** games is sufficient or if longer/weighted windows provide better predictive power (lower Log Loss).

**Key Finding:** For Assists and Points, a **Last 40 (L40)** rolling window significantly outperforms the previous L20 baseline and the volatile L5/L10 metrics.

---

## 1. Theoretical Research (Stabilization Points)
Research into NHL statistical reliability (Cronbach's Alpha > 0.7) suggests different "memory" requirements for different stats:
- **Corsi (Shot Attempts):** ~7-10 games. High volume, stabilizes quickly.
- **SOG / Hits / Blocks:** ~15-25 games.
- **Goals / Assists / Points:** High variance, low frequency. Requires **40-80 games** to filter noise from true talent.

---

## 2. Experimental Results (Assists)
We ran 8 trials using a sample of 5,000 player-games from the 2024 season. Metrics used: **Log Loss** (primary) and **Brier Score**.

| Rank | Experiment | Log Loss | Brier Score | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **1** | **L40 (Stability)** | **0.5355** | **0.1691** | **Winner.** Best balance of history and current role. |
| 2 | Weighted (50% L20 / 50% Season) | 0.5609 | 0.1720 | Strong, but L40 is cleaner. |
| 3 | Baseline (L20) | 0.5755 | 0.1718 | Previous standard. |
| 4 | Weighted (75% Season / 25% L20) | 0.6175 | 0.1743 | Too much weight on early-season noise. |
| 5 | Season Long (Cumulative) | 0.7572 | 0.1760 | Poor performance in early/mid season. |
| 6 | L5 (Recency/Hot Streak) | 0.9664 | 0.1842 | **Worst.** Chasing streaks leads to over-prediction. |
| 7 | IPP Model (L20) | 3.2356 | 0.2342 | Failed (Zero-Mu issues, requires investigation). |

## 3. Expanded Validation (Seasons 2023-2025)
A second, larger-scale validation was run on **117,775 player-games** spanning 3 complete seasons to confirm the initial findings. The results strongly validated the L40 hypothesis.

| Rank | Experiment | Log Loss | Brier Score | Difference vs Baseline |
| :--- | :--- | :--- | :--- | :--- |
| **1** | **L40 (Stability)** | **0.6015** | **0.1734** | **-18.1% Log Loss** (Winner) |
| 2 | Weighted (50/50) | 0.6679 | 0.1755 | -9.0% |
| 3 | Baseline (L20) | 0.7345 | 0.1757 | Reference |
| 4 | Weighted (75/25) | 0.8053 | 0.1775 | +9.6% (Worse) |
| 5 | Season Long | 1.2125 | 0.1790 | +65% (Worse) |
| 6 | L5 Only | 1.8793 | 0.1896 | +155% (Worst) |

**Conclusion:** The **L40 window** is unequivocally superior to the L20 baseline for Assists, reducing predictive error (Log Loss) by over 18% in the long run. The "Season Long" metric performs poorly because it lacks predictiveness in the first ~20 games of every season, whereas L40 (carrying over previous season data) remains stable.

---

## 5. SOG Validation (Corsi Integration)
We hypothesized that **Shot Attempts (Corsi)**, which stabilize faster than SOG, could predict future SOG better when combined with a long-term finishing rate.
Target: Predicting **2.5+ SOG**.

| Rank | Experiment | Log Loss | Brier Score | Method |
| :--- | :--- | :--- | :--- | :--- |
| **1** | **Corsi Split (Corsi L20 * Thru% L40)** | **0.4764** | **0.1543** | **Winner.** Best blend of recent volume and long-term skill. |
| 2 | Weighted SOG (50/50 L20/L40) | 0.4777 | 0.1539 | Very close second. |
| 3 | Baseline (Raw SOG L20) | 0.4808 | 0.1549 | Standard approach. |
| 4 | Corsi L20 * LgAvg (0.58) | 0.4910 | 0.1595 | Assuming league avg thru% ignores player skill too much. |

**Conclusion:**
Using **L20 Corsi** (Shot Attempts) to predict volume, multiplied by **L40 Thru%** (SOG/Corsi) to predict efficiency, yields the most accurate SOG projections. This confirms that SOG is a compound metric of "Volume x Efficiency," and separating them improves accuracy.

---

## 6. Implementation Plan (Phase 2)
Based on these results, the following changes were made to the production pipeline:

### A. Feature Engineering (`build_player_features.py`)
- Added **L40** and **Season-to-Date** rolling windows to the DuckDB pipeline.
- Added **IPP for Assists** (`ev_ipp_assists_L20`) and **On-Ice Goal** tracking.

### B. Live Projections (`produce_live_base_projections.py`)
- The "Zero Lag" live projection script now calculates and exports L40 rates for Assists and Points.
- The window for data lookback was increased to 40 games to ensure sufficient history.

### C. Statistical Model (`single_game_model.py`)
- The model now prefers **L40 rates** for Assists and Points.
- It falls back to L20 for players with < 40 games.
- **TOI** remains on a L20 window to reflect the coach's most recent usage patterns.

---

## 4. Advanced Metrics & Future Leads

### Corsi (Shot Attempts) & Fenwick
- **Corsi (Shot Attempts):** Available in `fact_skater_game_situation`. 
- **Opportunity:** Since Corsi stabilizes in < 10 games, we could use a **Weighted Corsi (L10 vs L40)** to predict SOG more accurately than raw SOG.
- **Fenwick (Unblocked Attempts):** Also available; provides a better signal for scoring-chance generation.

### IPP (Individual Points Percentage)
- While the trial failed due to implementation details, the theory remains sound: `Mu = IPP * Team_OnIce_Goals`.
- **Next Step:** Refine the IPP model by using **Season-Long IPP** (high stability) multiplied by **L10 Team On-Ice xG** (current offensive environment).

### Matchup Specifics
- The baseline experiments were "neutral." The current production model already applies **Opponent xGA** and **Goalie GSAx** multipliers. 
- **Lead:** We could test if the "Opponent Multiplier" should also be calculated over different windows (e.g., is an opponent's recent 5-game defensive slump more predictive than their 40-game average?).

---
**Report generated by Gemini CLI Agent.**

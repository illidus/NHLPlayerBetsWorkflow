# Full System Audit: NHL Betting Pipeline

**Date:** January 4, 2026  
**Auditor:** Gemini Agent

## 1. Layer 1: Raw Data Foundations

### Source Tables
The system is built upon three primary data streams ingested into DuckDB:

1.  **MoneyPuck Data** (Gameplay Stats)
    *   **Tables:** `fact_skater_game_all`, `fact_goalie_game_situation`, `dim_players`, `dim_games`.
    *   **Source:** CSV files structured by Year/Type/PlayerType.
    *   **Granularity:**
        *   `fact_skater_game_all`: One row per **player per game**. It aggregates situation-specific data (5v5, PP, etc.) into a single record.
        *   `fact_goalie_game_situation`: One row per **goalie per game** (aggregated to `situation='all'` for features).

2.  **API Odds Data** (New Production Layer)
    *   **Tables:** `raw_playnow_responses`, `fact_playnow_markets`.
    *   **Source:** Direct JSON API calls to PlayNow content-service.
    *   **Granularity:** Outcome-level pricing (decimal) for Goals, SOG, Points, and Assists.

3.  **Legacy Odds Data** (Consensus Foundation)
    *   **Table:** `fact_odds_props`.
    *   **Source:** `1_PlayerProps/nhl_player_props_all.csv`.
    *   **Granularity:** One row per **market offer** (e.g., Crosby Over 0.5 Assists). Includes `odds_decimal`, `line`, and `side`.

## 2. Layer 2: Feature Engineering (The Inputs)

Features are calculated using strict "Past-Only" rolling windows to prevent lookahead bias.

### Skater Features (`build_player_features.py`)
*   **Method:** Window function `ROWS BETWEEN N PRECEDING AND 1 PRECEDING`.
*   **Key Metrics (L10):**
    *   `goals_per_game_L10`, `assists_per_game_L10`, `points_per_game_L10`.
    *   `sog_per_60_L10`, `blocks_per_game_L10`.
    *   `avg_toi_minutes_L10` (Used as the base projection).

### Team Defense (`build_team_defense_features.py`)
*   **Method:** Aggregates goalie stats by team, then applies L10 rolling sum.
*   **Key Metrics:**
    *   `opp_sa60_L10`: (Sum SA L10) / (Sum TOI L10 / 3600).
    *   `opp_xga60_L10`: (Sum xGA L10) / (Sum TOI L10 / 3600).

### Goalie Features (`build_goalie_features.py`)
*   **Method:** Rolling sums of xGA, GA, and TOI.
*   **Key Metric:**
    *   `goalie_gsax60_L10`: `(Sum_xGA_L10 - Sum_GA_L10) / (Sum_TOI_L10 / 3600)`.
    *   **Note:** Handled safely with `CASE WHEN sum_toi_L10 = 0` to avoid division by zero.

## 3. Layer 3: The Snapshot Builder (The Nexus)

**File:** `build_probability_snapshots.py`

### Merge Logic
The script joins three streams to create the `fact_model_mu` table:
1.  **Base:** `fact_player_game_features` (Skater).
2.  **Defense:** Left Join `fact_team_defense_features` on `(opp_team, game_date)`.
3.  **Goalie:** Left Join `fact_goalie_features` via a CTE.

### Primary Goalie Logic (Heuristic Selection)
*   **Goal:** Predict the "Likely Starter" using **only past data** (No Lookahead).
*   **Algorithm:**
    1.  **Context:** Determine if the game is a **Back-to-Back (B2B)** for the team.
    2.  **Depth Chart:** Rank active goalies by Volume (`sum_toi_L10` from `fact_goalie_features` in the past 14 days).
        *   Rank 1 = Implied Starter.
        *   Rank 2 = Implied Backup.
    3.  **Rotation Check:** Identify who started the *previous* game.
    4.  **Selection Rule:**
        *   **IF** B2B = True **AND** Rank 1 Goalie started yesterday: **Pick Rank 2 (Backup)**.
        *   **ELSE**: **Pick Rank 1 (Starter)**.

### Leakage Check
*   **Verdict:** **PASSED (Hardened)**.
*   **Evidence:**
    *   Features: derived from `N PRECEDING` windows.
    *   Goalie Selection: Now strictly uses `prev_game_date` and `fact_goalie_features` from *before* the target game. The "TOI Lookahead" bias (peeking at who actually played the target game) has been removed.

## 4. Layer 4: The Mathematical Core (The Brain)

**File:** `nhl_bets/probs/single_game_model.py`

### The `compute_game_probs` Function
This function transforms raw features into probabilities.

### Multipliers (The "Brain" Logic)
The code explicitly modifies the Base Mu (L10 Avg) using environmental factors:

1.  **Opponent Defense:**
    ```python
    mult_opp_g = (opp_xga60 / LG_XGA60) ** BETAS['opp_g']
    mult_opp_sog = (opp_sa60 / LG_SA60) ** BETAS['opp_sog']
    ```
2.  **Goalie Quality:**
    ```python
    raw_m = 1 - (goalie_gsax60 / goalie_xga60)
    mult_goalie = raw_m ** BETAS['goalie'] # Clamped [0.5, 1.5]
    ```
3.  **Application:**
    *   **SOG/BLK:** `base * mult_opp_sog`
    *   **G/A/PTS:** `base * mult_opp_g * mult_goalie`

### Distributions
*   **Poisson:** GOALS, ASSISTS, POINTS.
*   **Negative Binomial:** SOG, BLOCKS.
    *   **Alphas:** Defined in `config.py` (e.g., `alpha_sog = 0.35`).

## 5. Layer 5: Operational Safety & Date Resolution

**File:** `3_EV_Analysis/src/main.py`

### Strict Date Matching
The pipeline now enforces date consistency:
1.  **Scraper**: Extracts `Game_Date` from the live sportsbook.
2.  **Projections**: The Phase 8 model (`single_game_probs.py`) generates projections tagged with that specific `Game_Date`.
3.  **Match Logic**: Players are only matched if `(Name, Date)` exists in both the projections and the props. This prevents using stale projections for future/past games.

### The "October Rule"
*   **Recommendation:** Skip or reduced staking for the first 30 days of the season to account for roster volatility and lack of current-season L10 data.

## 6. Layer 6: Automated Daily Workflow

**File:** `run_workflow.py`

This orchestrator automates the entire daily production cycle:
1.  **Scrape**: Runs `1_PlayerProps/scrapers/nhl_props_scraper.py` to get latest odds.
2.  **Move**: Transfers raw output to `1_PlayerProps/nhl_player_props_all.csv`.
3.  **Project**: Runs `2_Projections/single_game_probs.py` to apply Phase 8 "Brain" adjustments (Goalie/Defense) to base stats.
4.  **Analyze**: Runs `3_EV_Analysis/main.py` to calculate EV using corrected statistical distributions.

## 7. Mathematical Verification (Audit Result)

*   **Distributions:** 
    *   **Goals, Assists, Points**: Poisson.
    *   **SOG, Blocks**: Negative Binomial (Alpha 0.35/0.60).
    *   **Verdict:** **Correct**. The `3_EV_Analysis/src/distributions.py` has been updated to use `scipy.stats.nbinom` for over-dispersed markets.

*   **Environmental Adjustments:**
    *   Goalie GSAx and Opponent Defense (xGA60/SA60) are now baked into the `mu` before EV calculation.
    *   **Verdict:** **Correct**. Verified that `run_workflow.py` uses the `single_game_probs.py` script which utilizes the `nhl_bets` core engine.

## 8. Current Status & Production Readiness

1.  **Market Coverage**: 
    - The transition to **Direct API Scraping** has resolved the "Goal Scorer" bottleneck. The system now captures SOG, Assists, and Points reliably for all upcoming games.
2.  **Stat Availability**: 
    - API capture is significantly faster (< 5s per game) compared to Selenium (> 60s per game), allowing for more frequent updates.

## 9. Staking & Strategy
*   **The "October Rule"**: Apply a **0.25x unit size** for the first 30 days of the season due to early-season volatility in L10 rolling averages.

## Data Flow Diagram (Production)

```mermaid
graph TD
    subgraph 1. Data Collection
        SCR[nhl_props_scraper.py] -->|Game_Date| P_CSV[nhl_player_props_all.csv]
    end

    subgraph 2. Projection Enhancement (Phase 8 Brain)
        BASE[BaseSingleGameProjections.csv] -->|Process| SGP[single_game_probs.py]
        CTX[GameContext.csv] -->|Apply Goalie/Def| SGP
        SGP -->|Adjusted Mus| PROB_CSV[SingleGamePropProbabilities.csv]
    end

    subgraph 3. EV Analysis
        P_CSV -->|Parse| EV[3_EV_Analysis]
        PROB_CSV -->|Match by Name/Date| EV
        EV -->|Rank by EV| FINAL[ev_bets_ranked.xlsx]
    end
```

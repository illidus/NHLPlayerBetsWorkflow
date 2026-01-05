
import duckdb
import pandas as pd
import numpy as np
import time
from sklearn.metrics import log_loss, brier_score_loss

DB_PATH = "data/db/nhl_backtest.duckdb"

def load_data():
    con = duckdb.connect(DB_PATH)
    # Select games from 2023-2025 where we have full L40 history
    query = """
    SELECT * 
    FROM fact_player_game_features 
    WHERE season >= 2023
    AND ev_toi_minutes_L40 > 0
    ORDER BY game_date
    """
    df = con.execute(query).df()
    con.close()
    return df

def calculate_metrics(df, pred_col, target_col='assists'):
    # Clip for log loss stability
    y_pred = np.clip(df[pred_col], 1e-15, 1 - 1e-15)
    y_true = (df[target_col] > 0).astype(int)
    
    ll = log_loss(y_true, y_pred)
    bs = brier_score_loss(y_true, y_pred)
    return ll, bs

def run_vectorized_experiments(df):
    print(f"Running Vectorized Experiments on {len(df)} rows...")
    
    results = []
    
    # Pre-calc Targets
    # We want P(Assists >= 1)
    # Mu = Rate * Time
    # Prob = 1 - exp(-Mu)
    
    # 1. Baseline (L20)
    # Mu = (EV_Rate_L20 * EV_Time_L20 + PP_Rate_L20 * PP_Time_L20) / 60 (Since rates are per 60)
    # Wait, in the DB, rates are per 60. Time is minutes.
    # Mu = (Rate * Time) / 60? 
    # Let's check build_player_features.py:
    # ev_ast_60_L20 = (ev_assists_L20 / ev_toi_minutes_L20) * 60
    # So Mu = ev_ast_60_L20 * (ev_toi_minutes_L20 / 60) = ev_assists_L20 (The average count).
    # BUT we want to predict for *this* game using *current* usage.
    # In the loop experiment we did: 
    # proj_toi = L20 TOI
    # mu = rate * (proj_toi / 60)
    # So yes: Mu = Rate * (Time / 60)
    
    # Define generic calc helper
    def calc_prob(ev_rate_col, pp_rate_col, ev_time_col, pp_time_col):
        # Fill NA with 0
        r_ev = df[ev_rate_col].fillna(0)
        r_pp = df[pp_rate_col].fillna(0)
        t_ev = df[ev_time_col].fillna(0)
        t_pp = df[pp_time_col].fillna(0)
        
        # Calculate Base Mu
        mu = (r_ev * t_ev / 60.0) + (r_pp * t_pp / 60.0)
        
        # Calculate Prob (1 - exp(-mu))
        prob = 1 - np.exp(-mu)
        return prob

    # Exp 1: Baseline L20
    df['p_L20'] = calc_prob('ev_ast_60_L20', 'pp_ast_60_L20', 'ev_toi_minutes_L20', 'pp_toi_minutes_L20')
    ll, bs = calculate_metrics(df, 'p_L20')
    results.append({'Experiment': '1. Baseline (L20)', 'LogLoss': ll, 'Brier': bs})
    
    # Exp 2: L5 (Recency)
    # Note: Using L5 rates but L20 time for stability? 
    # In previous loop we used:
    # pd['ev_toi_minutes_L20'] = row['ev_toi_minutes_L5']
    # So we use L5 Time.
    # df['p_L5'] = calc_prob('ev_ast_60_L5', 'pp_pts_60_L5', 'ev_toi_minutes_L5', 'pp_toi_minutes_L5') # REMOVED
    
    # Let's stick to the previous map_L5 logic:
    # pd['pp_ast_60_L20'] = row['pp_ast_60_L20'] # Fallback
    # pd['ev_toi_minutes_L20'] = row['ev_toi_minutes_L5']
    # So: EV_Rate=L5, PP_Rate=L20, Time=L5
    df['p_L5'] = calc_prob('ev_ast_60_L5', 'pp_ast_60_L20', 'ev_toi_minutes_L5', 'pp_toi_minutes_L20') 
    ll, bs = calculate_metrics(df, 'p_L5')
    results.append({'Experiment': '2. L5 Only', 'LogLoss': ll, 'Brier': bs})

    # Exp 3: L40 (Stability)
    # L40 Rates, L40 Time? 
    # Previous loop: map_L40 used L40 Rates, L40 Time.
    # Note: Previous loop used pp_pts_60_L20 as proxy for pp_ast_60_L20 in map_L40? 
    # "pd['pp_ast_60_L20'] = row['pp_pts_60_L20'] # Keep PP L20 (sparse events)"
    # Let's verify build_player_features.py created pp_ast_60_L40. 
    # Yes: CASE WHEN a.pp_toi_minutes_L40 > 0 THEN (a.pp_assists_L40 / a.pp_toi_minutes_L40) * 60
    # So we can use pp_ast_60_L40 directly if available.
    # The DB has `pp_ast_60_L40`.
    df['p_L40'] = calc_prob('ev_ast_60_L40', 'pp_ast_60_L20', 'ev_toi_minutes_L40', 'pp_toi_minutes_L20')
    ll, bs = calculate_metrics(df, 'p_L40')
    results.append({'Experiment': '3. L40 (Stability)', 'LogLoss': ll, 'Brier': bs})

    # Exp 4: Season
    df['p_Season'] = calc_prob('ev_ast_60_Season', 'pp_ast_60_Season', 'ev_toi_minutes_Season', 'pp_toi_minutes_Season')
    ll, bs = calculate_metrics(df, 'p_Season')
    results.append({'Experiment': '4. Season Long', 'LogLoss': ll, 'Brier': bs})
    
    # Exp 5: Weighted 50/50 (L20 / Season)
    # Rate = (L20 + Season) / 2
    # Time = L20
    df['r_ev_5050'] = (df['ev_ast_60_L20'] + df['ev_ast_60_Season']) / 2
    df['r_pp_5050'] = (df['pp_ast_60_L20'] + df['pp_ast_60_Season']) / 2
    df['p_5050'] = calc_prob('r_ev_5050', 'r_pp_5050', 'ev_toi_minutes_L20', 'pp_toi_minutes_L20')
    ll, bs = calculate_metrics(df, 'p_5050')
    results.append({'Experiment': '5. Weighted 50/50', 'LogLoss': ll, 'Brier': bs})
    
    # Exp 6: Weighted 75 Season / 25 L20
    df['r_ev_7525'] = (0.75 * df['ev_ast_60_Season']) + (0.25 * df['ev_ast_60_L20'])
    # PP used Season
    df['p_7525'] = calc_prob('r_ev_7525', 'pp_ast_60_Season', 'ev_toi_minutes_Season', 'pp_toi_minutes_Season')
    ll, bs = calculate_metrics(df, 'p_7525')
    results.append({'Experiment': '6. Weighted 75/25', 'LogLoss': ll, 'Brier': bs})
    
    return pd.DataFrame(results).sort_values('LogLoss')

if __name__ == "__main__":
    start = time.time()
    print("Loading Data...")
    df = load_data()
    print(f"Loaded {len(df)} rows in {time.time() - start:.2f}s")
    
    res = run_vectorized_experiments(df)
    print("\n--- FINAL RESULTS (Assists - Full 3 Season History) ---")
    print(res.to_string(index=False))

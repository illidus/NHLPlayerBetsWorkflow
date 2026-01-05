
import duckdb
import pandas as pd
import numpy as np
import sys
import os
from sklearn.metrics import log_loss, brier_score_loss
import time

# Add root to path
sys.path.append(os.getcwd())

from nhl_bets.projections.single_game_model import compute_game_probs

DB_PATH = "data/db/nhl_backtest.duckdb"

def load_data(limit=None):
    con = duckdb.connect(DB_PATH)
    # Select games from 2023-2025 where we have full L40 history
    # This ensures a fair comparison between L20 and L40
    query = """
    SELECT *
    FROM fact_player_game_features
    WHERE season >= 2023
    AND ev_toi_minutes_L40 > 0
    ORDER BY game_date
    """
    if limit:
        query += f" LIMIT {limit}"
        
    df = con.execute(query).df()
    con.close()
    return df

def run_experiment(name, df, mapper_func):
    print(f"Running Experiment: {name}...")
    start_time = time.time()
    
    y_true = []
    y_pred = []
    
    debug_limit = 5
    
    for i, row in df.iterrows():
        # Base Player Data (Metadata)
        player_data = {
            'Player': row['player_id'],
            'G': 0, # Placeholder
            'A': 0, # Placeholder
            'PTS': 0, # Placeholder
            'SOG': 0, # Placeholder
            'BLK': 0, # Placeholder
            'TOI': row.get('ev_toi_minutes_L20', 15) + row.get('pp_toi_minutes_L20', 2), # Base TOI
            'proj_toi': row.get('ev_toi_minutes_L20', 15) + row.get('pp_toi_minutes_L20', 2) # Assume proj = L20 for fair comparison
        }
        
        # Apply Strategy
        player_data = mapper_func(row, player_data)
        
        # Compute Probs
        res = compute_game_probs(player_data, context_data={
            'opp_sa60': 30.0, 
            'opp_xga60': 2.5,
            'implied_team_total': 3.0,
            'is_b2b': 0
        })
        
        # Target: Assists
        prob = res['probs_assists'].get(1, 0.0) # 1+ Assists (P(X>=1))
        mu = res['mu_assists']
        actual = 1 if row['assists'] > 0 else 0
        
        if i < debug_limit:
            print(f"  [DEBUG] Mu: {mu:.3f}, Prob: {prob:.3f}, Actual: {actual}, TOI: {player_data['proj_toi']:.1f}")
            
        y_true.append(actual)
        y_pred.append(prob)
        
    ll = log_loss(y_true, y_pred, labels=[0,1])
    bs = brier_score_loss(y_true, y_pred)
    
    elapsed = time.time() - start_time
    print(f"  > Log Loss: {ll:.4f} | Brier: {bs:.4f} | Time: {elapsed:.2f}s")
    return {'Experiment': name, 'LogLoss': ll, 'Brier': bs}

# --- Strategy Mappers ---

def map_control_L20(row, pd):
    pd['ev_ast_60_L20'] = row['ev_ast_60_L20']
    pd['pp_ast_60_L20'] = row['pp_ast_60_L20']
    pd['ev_toi_minutes_L20'] = row['ev_toi_minutes_L20']
    pd['pp_toi_minutes_L20'] = row['pp_toi_minutes_L20']
    return pd

def map_L5(row, pd):
    pd['ev_ast_60_L20'] = row['ev_ast_60_L5'] # Inject L5 into L20 slot
    pd['pp_ast_60_L20'] = row['pp_ast_60_L20'] # Fallback to L20 for PP
    pd['ev_toi_minutes_L20'] = row['ev_toi_minutes_L5']
    pd['pp_toi_minutes_L20'] = row['pp_toi_minutes_L20'] 
    return pd
def map_L40(row, pd):
    pd['ev_ast_60_L20'] = row['ev_ast_60_L40']
    pd['pp_ast_60_L20'] = row['pp_pts_60_L20'] # Keep PP L20 (sparse events)
    pd['ev_toi_minutes_L20'] = row['ev_toi_minutes_L40']
    pd['pp_toi_minutes_L20'] = row['pp_toi_minutes_L20']
    return pd

def map_season(row, pd):
    pd['ev_ast_60_L20'] = row['ev_ast_60_Season']
    pd['pp_ast_60_L20'] = row['pp_ast_60_Season']
    pd['ev_toi_minutes_L20'] = row['ev_toi_minutes_Season']
    pd['pp_toi_minutes_L20'] = row['pp_toi_minutes_Season']
    return pd

def map_weighted_50_50(row, pd):
    # 50% L20, 50% Season
    pd['ev_ast_60_L20'] = (row['ev_ast_60_L20'] + row['ev_ast_60_Season']) / 2
    pd['pp_ast_60_L20'] = (row['pp_ast_60_L20'] + row['pp_ast_60_Season']) / 2
    pd['ev_toi_minutes_L20'] = row['ev_toi_minutes_L20']
    pd['pp_toi_minutes_L20'] = row['pp_toi_minutes_L20']
    return pd

def map_weighted_marcels(row, pd):
    # "Marcels-ish": Heavily weight Season (say 3x) vs L20 (1x)? 
    # Actually Marcels is 3 seasons. Here we have Season vs L20.
    # Let's try 75% Season, 25% L20 (Stabilized)
    pd['ev_ast_60_L20'] = (0.75 * row['ev_ast_60_Season']) + (0.25 * row['ev_ast_60_L20'])
    pd['pp_ast_60_L20'] = row['pp_ast_60_Season']
    pd['ev_toi_minutes_L20'] = row['ev_toi_minutes_Season']
    pd['pp_toi_minutes_L20'] = row['pp_toi_minutes_Season']
    return pd

def map_ipp_model(row, pd):
    # Use IPP * OnIceGoals
    pd['ev_ipp_ast'] = row['ev_ipp_assists_L20']
    
    # Debug IPP
    # if row['ev_ipp_assists_L20'] == 0:
    #     print(f"DEBUG: IPP is 0. AssistsL20: {row['ev_assists_L20']}, OnIceGoals: {row['ev_on_ice_goals_L20']}")

    # Calc rate: OnIceGoals / TOI * 60
    if row['ev_toi_minutes_L20'] > 0:
        pd['ev_on_ice_goals_60'] = (row['ev_on_ice_goals_L20'] / row['ev_toi_minutes_L20']) * 60
    else:
        pd['ev_on_ice_goals_60'] = 0
        
    pd['pp_ipp_ast'] = row['pp_ipp_assists_L20']
    if row['pp_toi_minutes_L20'] > 0:
        pd['pp_on_ice_goals_60'] = (row['pp_on_ice_goals_L20'] / row['pp_toi_minutes_L20']) * 60
    else:
         pd['pp_on_ice_goals_60'] = 0

    pd['ev_toi_minutes_L20'] = row['ev_toi_minutes_L20']
    pd['pp_toi_minutes_L20'] = row['pp_toi_minutes_L20']
    return pd
    
def map_ipp_season(row, pd):
    # Use IPP * OnIceGoals (Season Stats)
    pd['ev_ipp_ast'] = row['ev_ipp_assists_Season']
    
    if row['ev_toi_minutes_Season'] > 0:
        pd['ev_on_ice_goals_60'] = (row['ev_on_ice_goals_Season'] / row['ev_toi_minutes_Season']) * 60
    else:
        pd['ev_on_ice_goals_60'] = 0
        
    pd['pp_ipp_ast'] = 0.5 # Fallback or calc? 
    pd['pp_on_ice_goals_60'] = 0
    
    pd['ev_toi_minutes_L20'] = row['ev_toi_minutes_Season']
    pd['pp_toi_minutes_L20'] = row['pp_toi_minutes_Season']
    return pd

if __name__ == "__main__":
    print("Loading Data (Seasons 2023-2025)...")
    df = load_data()
    print(f"Loaded {len(df)} rows.")
    
    results = []
    
    results.append(run_experiment("1. Baseline (L20)", df, map_control_L20))
    results.append(run_experiment("2. L5 Only (Recency)", df, map_L5))
    results.append(run_experiment("3. L40 (Stability)", df, map_L40))
    results.append(run_experiment("4. Season Long", df, map_season))
    results.append(run_experiment("5. Weighted (50/50)", df, map_weighted_50_50))
    results.append(run_experiment("6. Weighted (75 Season/25 L20)", df, map_weighted_marcels))
    results.append(run_experiment("7. IPP Model (L20)", df, map_ipp_model))
    results.append(run_experiment("8. IPP Model (Season)", df, map_ipp_season))
    
    res_df = pd.DataFrame(results).sort_values('LogLoss')
    print("\n--- FINAL RESULTS (Assists) ---")
    print(res_df.to_string(index=False))

import duckdb
import pandas as pd
import numpy as np
import joblib
import os
import argparse
from sklearn.isotonic import IsotonicRegression

# Config
DB_PATH = 'data/db/nhl_backtest.duckdb'
MODEL_DIR = 'data/models/calibrators_posthoc'

def train_tail_calibrators(min_samples=1000):
    print("Connecting to DB...")
    con = duckdb.connect(DB_PATH)
    
    # Fetch Training Data
    # We need outcomes, probabilities, and bucket features
    # Since we don't have a single 'fact_calibration' table with all features pre-bucketed,
    # we need to join fact_probabilities (which has p_over) with fact_player_game_features (outcome, vol).
    # Assuming 'variance_v1' or similar model version is available with 'sog_std_L20' (we added it to context, but did we store it in probabilities table?)
    # We didn't explicitly store 'sog_std_L20' in 'fact_probabilities'.
    # We need to re-join 'fact_player_game_features' to get it.
    
    # We'll use the 'variance_v1' or 'zscore_optim_v1' table if available, or just join the latest large run.
    # Let's assume 'fact_probabilities_variance_v1' exists from the previous turn.
    
    table_name = "fact_probabilities_variance_v1"
    
    print(f"Fetching data from {table_name}...")
    query = f"""
    SELECT 
        p.market,
        p.line,
        p.p_over as prob,
        CASE 
            WHEN p.market = 'SOG' THEN (CASE WHEN f.sog > p.line THEN 1 ELSE 0 END)
            WHEN p.market = 'BLOCKS' THEN (CASE WHEN f.blocks > p.line THEN 1 ELSE 0 END)
            ELSE NULL
        END as y,
        dp.primary_position as pos,
        
        -- Volatility Bucket Logic (Using SOG per Game as proxy)
        CASE 
            WHEN f.sog_per_game_L10 < 1.5 THEN 'low'
            WHEN f.sog_per_game_L10 > 2.5 THEN 'high'
            ELSE 'mid'
        END as vol_bucket
        
    FROM {table_name} p
    JOIN fact_player_game_features f ON p.player_id = f.player_id AND p.game_id = f.game_id
    LEFT JOIN dim_players dp ON p.player_id = dp.player_id
    WHERE p.market IN ('SOG', 'BLOCKS')
    """
    
    try:
        df = con.execute(query).df()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return

    print(f"Loaded {len(df)} rows. Processing...")
    
    # Bucket Lines
    # SOG: <2.5, 2.5, 3.5, 4.5, >4.5
    def get_line_bucket(row):
        m = row['market']
        l = row['line']
        if m == 'SOG':
            if l <= 1.5: return '1.5'
            if l == 2.5: return '2.5'
            if l == 3.5: return '3.5'
            if l == 4.5: return '4.5'
            return '5.5plus'
        return str(l)

    df['line_bucket'] = df.apply(get_line_bucket, axis=1)
    
    os.makedirs(MODEL_DIR, exist_ok=True)
    
    # Train Hierarchy
    # 1. (Market, Pos, Line, Vol)
    # 2. (Market, Pos, Line)
    # 3. (Market, Pos)
    # 4. (Market)
    
    # We save specific files for specific keys. 
    # Logic in application will look for specific file.
    
    # Iterate unique keys
    unique_markets = df['market'].unique()
    
    for m in unique_markets:
        df_m = df[df['market'] == m]
        
        # Level 4: Market Global
        train_and_save(df_m, f"calib_tail_{m}_GLOBAL", MODEL_DIR, min_samples)
        
        for pos in df_m['pos'].unique():
            if not pos: continue
            df_p = df_m[df_m['pos'] == pos]
            
            # Level 3: Market + Pos
            train_and_save(df_p, f"calib_tail_{m}_{pos}_GLOBAL", MODEL_DIR, min_samples)
            
            for lb in df_p['line_bucket'].unique():
                df_l = df_p[df_p['line_bucket'] == lb]
                
                # Level 2: Market + Pos + Line
                train_and_save(df_l, f"calib_tail_{m}_{pos}_{lb}_GLOBAL", MODEL_DIR, min_samples)
                
                for vb in df_l['vol_bucket'].unique():
                    df_v = df_l[df_l['vol_bucket'] == vb]
                    
                    # Level 1: Full Key
                    train_and_save(df_v, f"calib_tail_{m}_{pos}_{lb}_{vb}", MODEL_DIR, min_samples)

def train_and_save(df, name, model_dir, min_samples):
    if len(df) < min_samples:
        return
        
    X = df['prob'].values
    y = df['y'].values
    
    iso = IsotonicRegression(y_min=0, y_max=1, out_of_bounds='clip', increasing=True)
    try:
        iso.fit(X, y)
        path = os.path.join(model_dir, f"{name}.joblib")
        joblib.dump({'model': iso, 'n': len(df)}, path)
        # print(f"Saved {name} (n={len(df)})")
    except Exception:
        pass

if __name__ == "__main__":
    train_tail_calibrators()

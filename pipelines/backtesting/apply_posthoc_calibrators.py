import duckdb
import pandas as pd
import numpy as np
import os
import joblib
from scipy.special import logit

def apply_calibrators(db_path, model_dir):
    print(f"Connecting to {db_path}...")
    con = duckdb.connect(db_path)
    
    # 1. Load probabilities + position
    query = """
    SELECT 
        p.*,
        CASE WHEN dp.primary_position = 'D' THEN 'D' ELSE 'F' END as segment
    FROM fact_probabilities p
    LEFT JOIN dim_players dp ON p.player_id = dp.player_id
    """
    df = con.execute(query).df()
    print(f"Loaded {len(df)} rows.")
    
    # Initialize calibrated column with raw values
    df['p_over_calibrated'] = df['p_over']
    df['is_calibrated'] = 0
    
    unique_markets = df['market'].unique()
    
    for market in unique_markets:
        # We try to apply segmented first, then global
        for segment in ['F', 'D']:
            # Try segmented model
            model_filename = f"calib_posthoc_{market}_{segment}.joblib"
            model_path = os.path.join(model_dir, model_filename)
            
            # Fallback to global if segmented missing
            if not os.path.exists(model_path):
                model_path = os.path.join(model_dir, f"calib_posthoc_{market}.joblib")
            
            if not os.path.exists(model_path):
                continue
                
            print(f"Applying {os.path.basename(model_path)} to {market} (Segment: {segment})...")
            try:
                calib_data = joblib.load(model_path)
                model = calib_data['model']
                method = calib_data['method']
                
                mask = (df['market'] == market) & (df['segment'] == segment)
                if not mask.any():
                    continue
                    
                p_raw = df.loc[mask, 'p_over'].values
                
                if method == 'Isotonic':
                    p_calib = model.transform(p_raw)
                elif method == 'Platt':
                    eps = 1e-10
                    p_raw_clamped = np.clip(p_raw, eps, 1-eps)
                    logits = logit(p_raw_clamped).reshape(-1, 1)
                    p_calib = model.predict_proba(logits)[:, 1]
                else:
                    continue
                    
                p_calib = np.clip(p_calib, 1e-6, 1 - 1e-6)
                df.loc[mask, 'p_over_calibrated'] = p_calib
                df.loc[mask, 'is_calibrated'] = 1
                
            except Exception as e:
                print(f"Error applying {model_filename}: {e}")

    # Drop the temporary segment column
    df = df.drop(columns=['segment'])
    
    # Write back to DuckDB
    print("Writing calibrated probabilities to fact_probabilities...")
    con.execute("CREATE OR REPLACE TABLE fact_probabilities AS SELECT * FROM df")
    con.close()
    print("Done.")

if __name__ == "__main__":
    apply_calibrators(
        "data/db/nhl_backtest.duckdb",
        "data/models/calibrators_posthoc/"
    )
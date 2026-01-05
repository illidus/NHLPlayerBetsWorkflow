import duckdb
import pandas as pd
import numpy as np
import os
import joblib
from scipy.special import logit

def apply_calibrators(db_path, model_dir):
    con = duckdb.connect(db_path)
    
    # 1. Load probabilities
    df = con.execute("SELECT * FROM fact_probabilities").df()
    
    # Initialize calibrated column with raw values
    df['p_over_calibrated'] = df['p_over']
    df['is_calibrated'] = 0
    
    for market in ['ASSISTS', 'POINTS']:
        model_path = os.path.join(model_dir, f"calib_posthoc_{market}.joblib")
        if not os.path.exists(model_path):
            print(f"Warning: Calibrator for {market} not found at {model_path}")
            continue
            
        print(f"Applying calibrator to {market}...")
        calib_data = joblib.load(model_path)
        method = calib_data['method']
        model = calib_data['model']
        
        mask = (df['market'] == market) & (df['line'] == 1)
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
            print(f"Unknown method {method} for {market}")
            continue
            
        # Numerical safety (prevent log-loss pathologies)
        p_calib = np.clip(p_calib, 1e-6, 1 - 1e-6)
        
        df.loc[mask, 'p_over_calibrated'] = p_calib
        df.loc[mask, 'is_calibrated'] = 1
        
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

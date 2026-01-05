import duckdb
import pandas as pd
import numpy as np
import joblib
import argparse
import sys

DB_PATH = 'data/db/nhl_backtest.duckdb'

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', type=str, default='v1')
    args = parser.parse_args()
    
    con = duckdb.connect(DB_PATH)
    
    print(f"Loading calibrators metadata for version {args.version}...")
    calibs = con.execute("SELECT * FROM dim_calibrators WHERE calibrator_version = ?", [args.version]).df()
    
    if len(calibs) == 0:
        print("No calibrators found.")
        return
        
    print(f"Loading base probabilities...")
    # We only need relevant columns
    df = con.execute("""
        SELECT 
            player_id, game_id, game_date, market, line, p_over as p_over_baseline
        FROM fact_probabilities
    """).df()
    
    df['p_over_calibrated'] = df['p_over_baseline'] # Default fallback
    df['calibrator_version'] = args.version
    
    for _, row in calibs.iterrows():
        market = row['market']
        model_path = row['serialized_model_path']
        model_type = row['model_type']
        
        print(f"Applying {model_type} calibrator for {market}...")
        
        try:
            model = joblib.load(model_path)
        except Exception as e:
            print(f"Error loading model {model_path}: {e}")
            continue
            
        mask = df['market'] == market
        if not mask.any():
            continue
            
        X = df.loc[mask, 'p_over_baseline'].values
        
        if model_type == 'isotonic':
            p_calib = model.predict(X)
        elif model_type == 'platt':
            # LogisticRegression expects 2D
            p_calib = model.predict_proba(X.reshape(-1, 1))[:, 1]
        else:
            print(f"Unknown model type {model_type}")
            continue
            
        df.loc[mask, 'p_over_calibrated'] = p_calib
        
    print("Writing fact_probabilities_calibrated to DuckDB...")
    con.execute("DROP TABLE IF EXISTS fact_probabilities_calibrated")
    con.execute("CREATE TABLE fact_probabilities_calibrated AS SELECT * FROM df")
    
    # Validation
    print("Sample of calibrated data:")
    print(con.execute("SELECT * FROM fact_probabilities_calibrated LIMIT 5").df())
    
    con.close()

if __name__ == "__main__":
    main()

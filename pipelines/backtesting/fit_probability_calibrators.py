import duckdb
import pandas as pd
import numpy as np
import argparse
import os
import joblib
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss

DB_PATH = 'data/db/nhl_backtest.duckdb'
MODELS_DIR = 'data/models/calibrators'

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--train-end-date', type=str, default='2023-06-30')
    parser.add_argument('--val-end-date', type=str, default='2024-06-30')
    parser.add_argument('--version', type=str, default='v1')
    args = parser.parse_args()
    
    con = duckdb.connect(DB_PATH)
    
    # Create dim_calibrators if not exists
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_calibrators (
            calibrator_version VARCHAR,
            market VARCHAR,
            line_bucket VARCHAR,
            train_start_date DATE,
            train_end_date DATE,
            model_type VARCHAR,
            serialized_model_path VARCHAR
        )
    """)
    
    print(f"Loading data from fact_calibration_dataset...")
    df = con.execute("SELECT * FROM fact_calibration_dataset").df()
    df['game_date'] = pd.to_datetime(df['game_date'])
    
    # Sort just in case
    df = df.sort_values('game_date')
    
    train_mask = df['game_date'] <= args.train_end_date
    val_mask = (df['game_date'] > args.train_end_date) & (df['game_date'] <= args.val_end_date)
    
    train_df = df[train_mask]
    val_df = df[val_mask]
    
    print(f"Train size: {len(train_df)}")
    print(f"Val size: {len(val_df)}")
    
    if len(train_df) == 0 or len(val_df) == 0:
        print("Error: Empty train or val set.")
        return

    markets = df['market'].unique()
    
    # Clear existing entries for this version
    con.execute("DELETE FROM dim_calibrators WHERE calibrator_version = ?", [args.version])
    
    for market in markets:
        print(f"\nProcessing market: {market}")
        
        # Filter by market
        t_m = train_df[train_df['market'] == market]
        v_m = val_df[val_df['market'] == market]
        
        if len(t_m) < 100 or len(v_m) < 100:
            print(f"Skipping {market}: Insufficient data ({len(t_m)} train, {len(v_m)} val)")
            continue
            
        X_train = t_m['p_over_baseline'].values
        y_train = t_m['y'].values
        
        X_val = v_m['p_over_baseline'].values
        y_val = v_m['y'].values
        
        # 1. Isotonic
        iso = IsotonicRegression(out_of_bounds='clip', y_min=0, y_max=1)
        # Isotonic expects 1D X
        try:
            iso.fit(X_train, y_train)
            p_iso = iso.predict(X_val)
            loss_iso = log_loss(y_val, p_iso)
        except Exception as e:
            print(f"Isotonic failed: {e}")
            loss_iso = 999
            
        # 2. Platt (Logistic)
        # Logistic expects 2D X
        lr = LogisticRegression(solver='lbfgs')
        try:
            lr.fit(X_train.reshape(-1, 1), y_train)
            p_lr = lr.predict_proba(X_val.reshape(-1, 1))[:, 1]
            loss_lr = log_loss(y_val, p_lr)
        except Exception as e:
            print(f"Platt failed: {e}")
            loss_lr = 999
            
        print(f"  Isotonic Log Loss: {loss_iso:.5f}")
        print(f"  Platt Log Loss:    {loss_lr:.5f}")
        
        # Select best
        if loss_iso < loss_lr:
            best_model = iso
            model_type = 'isotonic'
            best_loss = loss_iso
        else:
            best_model = lr
            model_type = 'platt'
            best_loss = loss_lr
            
        print(f"  Selected: {model_type}")
        
        # Save model
        filename = f"calib_{args.version}_{market}.joblib"
        filepath = os.path.join(MODELS_DIR, filename)
        joblib.dump(best_model, filepath)
        
        # Record in DB
        con.execute("""
            INSERT INTO dim_calibrators VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            args.version,
            market,
            'ALL', # bucket
            df['game_date'].min().date(), # train_start
            pd.to_datetime(args.train_end_date).date(), # train_end
            model_type,
            filepath
        ))
        
    print("\nCalibration fitting complete. Models saved.")
    con.close()

if __name__ == "__main__":
    main()

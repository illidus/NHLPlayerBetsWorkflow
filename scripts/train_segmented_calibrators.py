import duckdb
import pandas as pd
import numpy as np
import joblib
import os
import argparse
import logging
from sklearn.isotonic import IsotonicRegression

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_PATH = 'data/db/nhl_backtest.duckdb'
MODEL_DIR = 'data/models/calibrators_posthoc'

def train_calibrators(db_path, model_dir, min_samples=500, force_refresh=False):
    logger.info(f"Connecting to {db_path}...")
    con = duckdb.connect(db_path)
    
    # Fetch Data
    # We define 'segment' here. Currently strictly Position-based (F vs D).
    # F includes C, L, R. D is D.
    query = """
    SELECT 
        market,
        CASE WHEN position = 'D' THEN 'D' ELSE 'F' END as segment,
        p_over_baseline as prob,
        y as outcome
    FROM fact_calibration_dataset
    WHERE p_over_baseline IS NOT NULL 
      AND y IS NOT NULL
      AND market IN ('ASSISTS', 'POINTS', 'GOALS', 'SOG', 'BLOCKS')
    """
    
    logger.info("Executing query to fetch training data...")
    try:
        df = con.execute(query).df()
    except Exception as e:
        logger.error(f"Failed to query database: {e}")
        con.close()
        return
    
    con.close()
    logger.info(f"Loaded {len(df)} rows.")

    unique_markets = df['market'].unique()
    # unique_segments = df['segment'].unique() # ['F', 'D']
    
    os.makedirs(model_dir, exist_ok=True)
    
    for market in unique_markets:
        market_df = df[df['market'] == market]
        
        # 1. Global Calibrator (Standard)
        # Filename: calib_posthoc_{MARKET}.joblib
        train_and_save(market_df, market, "", model_dir, min_samples)
        
        # 2. Segmented Calibrators
        # Filename: calib_posthoc_{MARKET}_{SEGMENT}.joblib
        # Segments: F, D
        for segment in ['F', 'D']:
            seg_df = market_df[market_df['segment'] == segment]
            train_and_save(seg_df, market, f"_{segment}", model_dir, min_samples)

def train_and_save(df, market, suffix, model_dir, min_samples):
    if len(df) < min_samples:
        logger.warning(f"Skipping {market}{suffix}: {len(df)} samples < {min_samples}")
        return

    X = df['prob'].values
    y = df['outcome'].values
    
    # Isotonic Regression
    # Increasing=True ensures monotonicity (higher raw prob -> higher calibrated prob)
    iso = IsotonicRegression(y_min=0, y_max=1, out_of_bounds='clip', increasing=True)
    
    try:
        iso.fit(X, y)
        
        # Save
        filename = f"calib_posthoc_{market.upper()}{suffix}.joblib"
        path = os.path.join(model_dir, filename)
        
        data = {
            'method': 'Isotonic',
            'model': iso,
            'samples': len(df),
            'timestamp': pd.Timestamp.now().isoformat()
        }
        joblib.dump(data, path)
        logger.info(f"Saved {filename} (Samples: {len(df)})")
        
    except Exception as e:
        logger.error(f"Failed to train {market}{suffix}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Train Segmented Post-Hoc Calibrators")
    parser.add_argument("--min_samples", type=int, default=500, help="Minimum samples required to train a calibrator")
    parser.add_argument("--db_path", default=DB_PATH, help="Path to DuckDB")
    parser.add_argument("--model_dir", default=MODEL_DIR, help="Output directory for models")
    
    args = parser.parse_args()
    
    train_calibrators(args.db_path, args.model_dir, args.min_samples)

if __name__ == "__main__":
    main()

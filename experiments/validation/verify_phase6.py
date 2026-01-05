import joblib
import glob
import duckdb
import pandas as pd
import math
import os

DB_PATH = r'data/db/nhl_backtest.duckdb'
CALIB_DIR = r'data/models/calibrators'

def verify_artifacts():
    print("--- 1. Verifying Artifacts ---")
    fs = glob.glob(os.path.join(CALIB_DIR, '*.joblib'))
    print(f"Found {len(fs)} calibrator models.")
    if len(fs) == 0:
        print("FAIL: No calibrator models found.")
        return False
    
    try:
        m = joblib.load(fs[0])
        print(f"Loaded first model: {type(m)}")
    except Exception as e:
        print(f"FAIL: Could not load model: {e}")
        return False
    return True

def verify_db_counts():
    print("\n--- 2. Verifying DB Counts ---")
    con = duckdb.connect(DB_PATH)
    tables = ['fact_calibration_dataset', 'dim_calibrators', 'fact_probabilities_calibrated', 'fact_probabilities']
    counts = {}
    for t in tables:
        try:
            counts[t] = con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
            print(f"{t}: {counts[t]}")
        except Exception as e:
            print(f"Error querying {t}: {e}")
            con.close()
            return False
            
    con.close()
    
    if counts['fact_probabilities_calibrated'] != counts['fact_probabilities']:
        print("FAIL: Row count mismatch between baseline and calibrated probabilities.")
        return False
    if counts['dim_calibrators'] < 5:
        print("FAIL: Too few calibrators in dim_calibrators.")
        return False
        
    return True

def verify_holdout_eval():
    print("\n--- 3. Verifying Hold-out Evaluation (Log Loss) ---")
    # Strict held-out range check
    con = duckdb.connect(DB_PATH)
    start='2024-10-01'
    end='2025-06-30'
    
    q=f'''
    WITH base AS (
      SELECT c.market, c.p_over_baseline AS p, c.y
      FROM fact_calibration_dataset c
      WHERE c.game_date BETWEEN '{start}' AND '{end}'
    ),
    cal AS (
      SELECT d.market, d.p_over_calibrated AS p, c.y
      FROM fact_probabilities_calibrated d
      JOIN fact_calibration_dataset c
        ON d.player_id=c.player_id AND d.game_id=c.game_id AND d.market=c.market AND d.line=c.line
      WHERE c.game_date BETWEEN '{start}' AND '{end}'
    )
    SELECT 
      b.market,
      AVG(- (b.y*LN(GREATEST(1e-6,LEAST(1-1e-6,b.p))) + (1-b.y)*LN(GREATEST(1e-6,LEAST(1-1e-6,1-b.p))))) AS logloss_baseline,
      AVG(- (k.y*LN(GREATEST(1e-6,LEAST(1-1e-6,k.p))) + (1-k.y)*LN(GREATEST(1e-6,LEAST(1-1e-6,1-k.p))))) AS logloss_calibrated
    FROM base b
    JOIN cal k USING (market)
    GROUP BY 1
    ORDER BY 1
    '''
    try:
        res = con.execute(q).df()
        print(res)
        
        improved = res[res['logloss_calibrated'] <= res['logloss_baseline']]
        print(f"Markets improved on hold-out: {len(improved)} / {len(res)}")
    except Exception as e:
        print(f"Error in holdout check: {e}")
        con.close()
        return False
    con.close()
    return True

def verify_deltas():
    print("\n--- 4. Verifying Probability Deltas ---")
    con = duckdb.connect(DB_PATH)
    q='''
    SELECT
      AVG(ABS(d.p_over_calibrated - b.p_over)) AS mean_abs_delta,
      QUANTILE_CONT(ABS(d.p_over_calibrated - b.p_over), 0.5) AS median_abs_delta,
      SUM(CASE WHEN d.p_over_calibrated < 0.001 OR d.p_over_calibrated > 0.999 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS pct_near_extremes
    FROM fact_probabilities_calibrated d
    JOIN fact_probabilities b
      ON d.player_id=b.player_id AND d.game_id=b.game_id AND d.market=b.market AND d.line=b.line
    '''
    try:
        res = con.execute(q).df()
        print(res)
    except Exception as e:
        print(f"Error in delta check: {e}")
        con.close()
        return False
    con.close()
    return True

if __name__ == "__main__":
    if verify_artifacts() and verify_db_counts() and verify_holdout_eval() and verify_deltas():
        print("\nPASSED: Phase 6 verification successful.")
    else:
        print("\nFAILED: Phase 6 verification failed.")

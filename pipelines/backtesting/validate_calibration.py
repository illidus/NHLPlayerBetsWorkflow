import duckdb
import pandas as pd
import numpy as np
import sys

DB_PATH = 'data/db/nhl_backtest.duckdb'

def main():
    con = duckdb.connect(DB_PATH)
    
    print("Validating fact_probabilities_calibrated...")
    
    # 1. Check range
    min_p, max_p = con.execute("SELECT min(p_over_calibrated), max(p_over_calibrated) FROM fact_probabilities_calibrated").fetchone()
    print(f"Range: [{min_p}, {max_p}]")
    if min_p < 0 or max_p > 1:
        print("FAIL: Probability out of range [0,1]")
        sys.exit(1)
        
    # 2. Check if calibration changed anything
    diff = con.execute("SELECT avg(abs(p_over_calibrated - p_over_baseline)) FROM fact_probabilities_calibrated").fetchone()[0]
    print(f"Mean Absolute Change: {diff}")
    if diff == 0:
        print("FAIL: Calibration did not change any probabilities")
        sys.exit(1)
        
    # 3. Check nulls
    nulls = con.execute("SELECT count(*) FROM fact_probabilities_calibrated WHERE p_over_calibrated IS NULL").fetchone()[0]
    if nulls > 0:
        print(f"FAIL: Found {nulls} NULL probabilities")
        sys.exit(1)
        
    print("Validation passed.")
    con.close()

if __name__ == "__main__":
    main()

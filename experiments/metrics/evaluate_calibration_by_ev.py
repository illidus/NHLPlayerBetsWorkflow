
import duckdb
import pandas as pd
import numpy as np

DB_PATH = 'data/db/nhl_backtest.duckdb'

def evaluate_calibration():
    con = duckdb.connect(DB_PATH)
    
    df = con.execute("SELECT * FROM fact_backtest_bets WHERE result != 'PENDING'").df()
    
    if df.empty:
        print("No settled bets.")
        return
        
    # Bin by Model Probability
    # 0-10%, 10-20%, ...
    bins = np.arange(0, 1.1, 0.1)
    labels = [f"{int(x*100)}-{int((x+0.1)*100)}%" for x in bins[:-1]]
    
    df['prob_bin'] = pd.cut(df['model_prob'], bins=bins, labels=labels)
    
    # Calculate Actual Win Rate per bin
    calib = df.groupby('prob_bin').agg({
        'bet_id': 'count',
        'result': lambda x: (x == 'WIN').sum()
    }).rename(columns={'bet_id': 'count', 'result': 'wins'})
    
    calib['actual_win_rate'] = calib['wins'] / calib['count']
    calib['expected_win_rate'] = df.groupby('prob_bin')['model_prob'].mean()
    
    print("\nCalibration by Probability:")
    print(calib)
    
    calib.to_csv('outputs/backtest_reports/calibration_by_prob.csv')
    
    # Bin by EV
    # Does higher EV correlate with higher ROI? (Checked in profitability, but checking realized vs expected here)
    
    con.close()

if __name__ == "__main__":
    evaluate_calibration()

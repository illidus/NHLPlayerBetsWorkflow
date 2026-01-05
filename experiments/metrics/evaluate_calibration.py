import duckdb
import pandas as pd
import numpy as np
import os
from sklearn.metrics import log_loss, brier_score_loss

DB_PATH = 'data/db/nhl_backtest.duckdb'
REPORTS_DIR = 'outputs/backtest_reports'

def main():
    con = duckdb.connect(DB_PATH)
    
    # Define splits (hardcoded for now to match fit script, or passed via args - sticking to defaults)
    TRAIN_END = '2023-06-30'
    VAL_END = '2024-06-30'
    
    print("Joining calibrated probs with outcomes...")
    
    query = """
    SELECT
        c.player_id,
        c.game_id,
        c.game_date,
        c.market,
        c.line,
        c.p_over_baseline,
        c.p_over_calibrated,
        d.y
    FROM fact_probabilities_calibrated c
    JOIN fact_calibration_dataset d 
        ON c.player_id = d.player_id 
        AND c.game_id = d.game_id 
        AND c.market = d.market 
        AND c.line = d.line
    """
    
    df = con.execute(query).df()
    df['game_date'] = pd.to_datetime(df['game_date'])
    
    # Assign splits
    df['split'] = 'test'
    df.loc[df['game_date'] <= TRAIN_END, 'split'] = 'train'
    df.loc[(df['game_date'] > TRAIN_END) & (df['game_date'] <= VAL_END), 'split'] = 'val'
    
    print(f"Total rows: {len(df)}")
    print(df['split'].value_counts())
    
    results = []
    
    markets = df['market'].unique()
    splits = ['train', 'val', 'test']
    
    print("\nCalculating metrics...")
    
    for market in markets:
        for split in splits:
            subset = df[(df['market'] == market) & (df['split'] == split)]
            if len(subset) == 0:
                continue
                
            y = subset['y'].values
            p_base = subset['p_over_baseline'].values
            p_calib = subset['p_over_calibrated'].values
            
            # Log Loss
            ll_base = log_loss(y, p_base)
            ll_calib = log_loss(y, p_calib)
            
            # Brier Score
            bs_base = brier_score_loss(y, p_base)
            bs_calib = brier_score_loss(y, p_calib)
            
            results.append({
                'market': market,
                'split': split,
                'n_samples': len(subset),
                'log_loss_base': ll_base,
                'log_loss_calib': ll_calib,
                'log_loss_pct_imp': (ll_base - ll_calib) / ll_base * 100,
                'brier_base': bs_base,
                'brier_calib': bs_calib,
                'brier_pct_imp': (bs_base - bs_calib) / bs_base * 100
            })
            
    res_df = pd.DataFrame(results)
    print(res_df.round(4))
    
    res_df.to_csv(os.path.join(REPORTS_DIR, 'calibration_metrics_before_after.csv'), index=False)
    res_df.to_csv(os.path.join(REPORTS_DIR, 'calibration_by_market.csv'), index=False) # Same for now
    
    # Reliability Table
    print("\nGenerating reliability table...")
    bins = np.linspace(0, 1, 21) # 0.05 bins
    
    rel_results = []
    
    # Reliability on TEST set (or Val+Test if Test is small? Let's use Val+Test for robustness)
    # Actually prompt asked for reliability table (bins: 0-0.05 etc). Usually on validation or test.
    # Let's do it on 'val' + 'test' combined to show generalization.
    
    eval_df = df[df['split'].isin(['val', 'test'])]
    
    # Bin indices
    eval_df['bin'] = pd.cut(eval_df['p_over_calibrated'], bins, include_lowest=True)
    
    # Group by bin
    grouped = eval_df.groupby('bin', observed=False).agg({
        'y': ['mean', 'count'],
        'p_over_calibrated': 'mean',
        'p_over_baseline': 'mean'
    }).reset_index()
    
    grouped.columns = ['bin_range', 'actual_rate', 'count', 'mean_pred_calib', 'mean_pred_base']
    
    grouped['calibration_error'] = grouped['mean_pred_calib'] - grouped['actual_rate']
    
    print(grouped)
    grouped.to_csv(os.path.join(REPORTS_DIR, 'calibration_reliability_table.csv'), index=False)
    
    con.close()
    print("\nReports generated.")

if __name__ == "__main__":
    main()

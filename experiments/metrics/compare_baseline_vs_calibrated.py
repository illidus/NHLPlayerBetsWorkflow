import pandas as pd
import numpy as np
import os
import duckdb

REPORTS_DIR = 'outputs/backtest_reports'

def load_ledger(prob_source):
    path = os.path.join(REPORTS_DIR, f'backtest_bets_{prob_source}.csv')
    if not os.path.exists(path):
        print(f"Warning: {path} not found.")
        return pd.DataFrame()
    return pd.read_csv(path)

def calculate_metrics(df):
    if df.empty:
        return {
            'bets': 0,
            'stake': 0,
            'profit': 0,
            'roi': 0,
            'win_rate': 0
        }
    
    # Filter completed bets
    completed = df[df['result'].isin(['WIN', 'LOSS'])]
    
    n_bets = len(df)
    stake = df['stake'].sum()
    profit = df['profit'].sum()
    roi = (profit / stake * 100) if stake > 0 else 0
    win_rate = (completed['result'] == 'WIN').mean() * 100 if len(completed) > 0 else 0
    
    return {
        'bets': n_bets,
        'stake': stake,
        'profit': profit,
        'roi': roi,
        'win_rate': win_rate
    }

def main():
    print("Loading ledgers...")
    base_df = load_ledger('baseline')
    calib_df = load_ledger('calibrated')
    
    print(f"Baseline bets: {len(base_df)}")
    print(f"Calibrated bets: {len(calib_df)}")
    
    metrics_base = calculate_metrics(base_df)
    metrics_calib = calculate_metrics(calib_df)
    
    # Overall Comparison
    comparison = pd.DataFrame([
        {'metric': 'Total Bets', 'baseline': metrics_base['bets'], 'calibrated': metrics_calib['bets']},
        {'metric': 'Total Stake', 'baseline': metrics_base['stake'], 'calibrated': metrics_calib['stake']},
        {'metric': 'Total Profit', 'baseline': metrics_base['profit'], 'calibrated': metrics_calib['profit']},
        {'metric': 'ROI %', 'baseline': metrics_base['roi'], 'calibrated': metrics_calib['roi']},
        {'metric': 'Win Rate %', 'baseline': metrics_base['win_rate'], 'calibrated': metrics_calib['win_rate']},
    ])
    
    comparison['diff'] = comparison['calibrated'] - comparison['baseline']
    print("\n--- Overall Comparison ---")
    print(comparison.round(2))
    comparison.to_csv(os.path.join(REPORTS_DIR, 'backtest_comparison_summary.csv'), index=False)
    
    # ROI by Market
    print("\n--- ROI by Market ---")
    def get_market_roi(df):
        if df.empty: return pd.DataFrame()
        g = df.groupby('market').agg({'profit': 'sum', 'stake': 'sum', 'bet_id': 'count'}).reset_index()
        g['roi'] = g['profit'] / g['stake'] * 100
        return g[['market', 'roi', 'bet_id']]
        
    m_base = get_market_roi(base_df).rename(columns={'roi': 'roi_base', 'bet_id': 'bets_base'})
    m_calib = get_market_roi(calib_df).rename(columns={'roi': 'roi_calib', 'bet_id': 'bets_calib'})
    
    if not m_base.empty and not m_calib.empty:
        merged = pd.merge(m_base, m_calib, on='market', how='outer').fillna(0)
        merged['roi_diff'] = merged['roi_calib'] - merged['roi_base']
        print(merged.round(2))
        merged.to_csv(os.path.join(REPORTS_DIR, 'backtest_market_comparison.csv'), index=False)
        
    # Monthly ROI Stability
    print("\n--- Monthly ROI Stability ---")
    def get_monthly_roi(df, name):
        if df.empty: return pd.DataFrame()
        df['month'] = pd.to_datetime(df['game_date']).dt.to_period('M')
        g = df.groupby('month').agg({'profit': 'sum', 'stake': 'sum'}).reset_index()
        g['roi'] = g['profit'] / g['stake'] * 100
        g['source'] = name
        return g[['month', 'roi', 'source']]

    monthly_base = get_monthly_roi(base_df, 'baseline')
    monthly_calib = get_monthly_roi(calib_df, 'calibrated')
    
    monthly = pd.concat([monthly_base, monthly_calib])
    monthly['month'] = monthly['month'].astype(str)
    pivot = monthly.pivot(index='month', columns='source', values='roi').fillna(0)
    print(pivot.round(2))
    pivot.to_csv(os.path.join(REPORTS_DIR, 'backtest_monthly_stability.csv'))
    
    # EV Distribution Stats
    print("\n--- EV Distribution Stats ---")
    if not base_df.empty:
        print("Baseline EV stats:")
        print(base_df['ev'].describe())
    if not calib_df.empty:
        print("Calibrated EV stats:")
        print(calib_df['ev'].describe())

if __name__ == "__main__":
    main()

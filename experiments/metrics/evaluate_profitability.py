
import duckdb
import pandas as pd

DB_PATH = 'data/db/nhl_backtest.duckdb'

def evaluate_profitability():
    con = duckdb.connect(DB_PATH)
    
    df = con.execute("SELECT * FROM fact_backtest_bets WHERE result != 'PENDING'").df()
    
    if df.empty:
        print("No settled bets to evaluate.")
        return

    # Overall Metrics
    total_bets = len(df)
    total_staked = df['stake'].sum()
    total_profit = df['profit'].sum()
    roi = total_profit / total_staked if total_staked > 0 else 0
    win_rate = len(df[df['result'] == 'WIN']) / total_bets
    
    print(f"Total Bets: {total_bets}")
    print(f"Total Profit: {total_profit:.2f}")
    print(f"ROI: {roi:.2%}")
    print(f"Win Rate: {win_rate:.2%}")
    
    # By Market
    by_market = df.groupby('market').agg({
        'profit': 'sum',
        'stake': 'sum',
        'bet_id': 'count'
    }).reset_index()
    by_market['roi'] = by_market['profit'] / by_market['stake']
    
    print("\nBy Market:")
    print(by_market)
    
    by_market.to_csv('outputs/backtest_reports/backtest_profitability_summary.csv', index=False)
    
    # By EV Bucket
    df['ev_bucket'] = pd.cut(df['ev'], bins=[0, 0.05, 0.10, 0.20, 0.50, 100], labels=['0-5%', '5-10%', '10-20%', '20-50%', '50%+'])
    by_ev = df.groupby('ev_bucket').agg({
        'profit': 'sum',
        'stake': 'sum',
        'bet_id': 'count'
    }).reset_index()
    by_ev['roi'] = by_ev['profit'] / by_ev['stake']
    
    print("\nBy EV Bucket:")
    print(by_ev)
    
    con.close()

if __name__ == "__main__":
    evaluate_profitability()

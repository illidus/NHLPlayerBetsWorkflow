import duckdb
import pandas as pd
import argparse
import sys
import os
import numpy as np
from datetime import datetime

# Add src to path if needed (though usually handled by env)
sys.path.append(os.path.join(os.getcwd()))
from src.nhl_bets.eval.metrics import compute_log_loss

DB_PATH = 'data/db/nhl_backtest.duckdb'

def parse_args():
    parser = argparse.ArgumentParser(description="Check for Model Regression")
    # ... (args same as before) ...
    parser.add_argument("--candidate_table", required=True, help="New model table")
    parser.add_argument("--baseline_table", required=True, help="Baseline model table")
    parser.add_argument("--max_logloss_regression", type=float, default=0.0005, help="Max allowed global regression")
    parser.add_argument("--max_market_regression", type=float, default=0.0010, help="Max allowed per-market regression")
    parser.add_argument("--output_report", default=None, help="Path to markdown report")
    return parser.parse_args()

def get_metrics(con, table):
    # Retrieve raw data to compute metrics in Python for consistency
    query = f"""
        SELECT 
            p.market,
            CASE 
                WHEN p.market = 'GOALS' THEN (f.goals > p.line)::INT
                WHEN p.market = 'ASSISTS' THEN (f.assists > p.line)::INT
                WHEN p.market = 'POINTS' THEN (f.points > p.line)::INT
                WHEN p.market = 'SOG' THEN (f.sog > p.line)::INT
                WHEN p.market = 'BLOCKS' THEN (f.blocks > p.line)::INT
            END as y_true,
            COALESCE(p.p_over_calibrated, p.p_over) as y_prob
        FROM {table} p
        JOIN fact_player_game_features f ON p.player_id = f.player_id AND p.game_id = f.game_id
        WHERE p.market IN ('GOALS', 'ASSISTS', 'POINTS', 'SOG', 'BLOCKS')
    """
    
    df = con.execute(query).df()
    
    results = []
    
    # Per Market
    for market, group in df.groupby('market'):
        ll = compute_log_loss(group['y_true'].values, group['y_prob'].values)
        results.append({
            'market': market,
            'n': len(group),
            'log_loss': ll
        })
        
    # Global
    global_ll = compute_log_loss(df['y_true'].values, df['y_prob'].values)
    results.append({
        'market': 'Global',
        'n': len(df),
        'log_loss': global_ll
    })
    
    return pd.DataFrame(results).set_index('market')

def main():
    args = parse_args()
    
    print(f"--- Regression Gate ---")
    print(f"Candidate: {args.candidate_table}")
    print(f"Baseline:  {args.baseline_table}")
    
    con = duckdb.connect(DB_PATH)
    
    try:
        df_cand = get_metrics(con, args.candidate_table)
        df_base = get_metrics(con, args.baseline_table)
    except Exception as e:
        print(f"Error computing metrics: {e}")
        con.close()
        sys.exit(1)
        
    con.close()
    
    # Compare
    comparison = df_cand.join(df_base, lsuffix='_cand', rsuffix='_base')
    comparison['delta_log_loss'] = comparison['log_loss_cand'] - comparison['log_loss_base'] 
    # Positive delta = Candidate is WORSE (Higher Log Loss)
    
    failed = False
    report_lines = []
    report_lines.append(f"# Regression Gate Report")
    report_lines.append(f"**Date:** {datetime.now()}")
    report_lines.append(f"**Candidate:** {args.candidate_table}")
    report_lines.append(f"**Baseline:** {args.baseline_table}")
    report_lines.append(f"**Filter Scope:** Market IN ('GOALS', 'ASSISTS', 'POINTS', 'SOG', 'BLOCKS')")
    report_lines.append(f"**Probability Column:** `COALESCE(p.p_over_calibrated, p.p_over)` (Best Available)")
    report_lines.append(f"**Thresholds:** Global < {args.max_logloss_regression}, Market < {args.max_market_regression}\n")
    
    report_lines.append("| Market | N | Base LL | Cand LL | Delta | Status |")
    report_lines.append("|---|---|---|---|---|---|")
    
    for market, row in comparison.iterrows():
        delta = row['delta_log_loss']
        threshold = args.max_logloss_regression if market == 'Global' else args.max_market_regression
        
        status = "PASS"
        if delta > threshold:
            status = "**FAIL**"
            failed = True
            
        report_lines.append(f"| {market} | {row['n_cand']} | {row['log_loss_base']:.5f} | {row['log_loss_cand']:.5f} | {delta:+.5f} | {status} |")

    report_content = "\n".join(report_lines)
    print(report_content)
    
    if args.output_report:
        with open(args.output_report, 'w') as f:
            f.write(report_content)
        print(f"\nReport written to {args.output_report}")
        
    if failed:
        print("\n!!! REGRESSION DETECTED !!!")
        sys.exit(1)
    else:
        print("\nRegression Gate Passed.")
        sys.exit(0)

if __name__ == "__main__":
    main()

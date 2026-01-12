import duckdb
import pandas as pd
import numpy as np
import argparse
import json
import os
import sys
import matplotlib.pyplot as plt
from datetime import datetime
from sklearn.calibration import calibration_curve

# Add src to path
sys.path.append(os.path.join(os.getcwd()))
from src.nhl_bets.eval.metrics import compute_log_loss, compute_brier_score

DB_PATH = 'data/db/nhl_backtest.duckdb'
OUTPUT_DIR = 'outputs/eval'

def parse_args():
    parser = argparse.ArgumentParser(description="Evaluate Log Loss for NHL Player Projections")
    parser.add_argument("--start_date", type=str, default=None, help="YYYY-MM-DD")
    parser.add_argument("--end_date", type=str, default=None, help="YYYY-MM-DD")
    parser.add_argument("--table", type=str, required=True, help="Table name for probabilities (REQUIRED)")
    parser.add_argument("--calibration", choices=['raw', 'calibrated'], default='raw', help="Which probability column to evaluate (only affects sorting/filtering if extended, mostly for compatibility)")
    parser.add_argument("--use_interactions", action="store_true", help="Flag to indicate interactions were used (for reporting)")
    parser.add_argument("--variance_mode", default="off", help="Variance mode used (for reporting)")
    parser.add_argument("--interaction_config_path", type=str, default=None, help="Path to interaction config for override simulation (not fully implemented in this script yet)")
    parser.add_argument("--generate_plots", action="store_true", help="Generate reliability diagrams")
    return parser.parse_args()

def load_data(start_date=None, end_date=None, table="fact_probabilities"):
    print(f"--- Loading Data from {table} ---")
    con = duckdb.connect(DB_PATH)
    
    # Check if table exists
    try:
        con.execute(f"SELECT 1 FROM {table} LIMIT 1")
    except Exception:
        print(f"Error: Table '{table}' does not exist in {DB_PATH}")
        sys.exit(1)
        
    date_filter = ""
    if start_date:
        date_filter += f" AND p.game_date >= '{start_date}'"
    if end_date:
        date_filter += f" AND p.game_date <= '{end_date}'"

    # Check if matchup_type exists
    has_matchup = False
    has_assist_cluster = False
    try:
        cols = [c[0] for c in con.execute(f"DESCRIBE {table}").fetchall()]
        if 'matchup_type' in cols:
            has_matchup = True
        if 'assist_cluster' in cols:
            has_assist_cluster = True
    except:
        pass

    matchup_sel = "p.matchup_type," if has_matchup else "'none' as matchup_type,"
    assist_sel = "p.assist_cluster," if has_assist_cluster else "'none' as assist_cluster,"

    query = f"""
    SELECT 
        p.player_id,
        p.game_id,
        p.game_date,
        p.market,
        p.line,
        p.p_over as p_raw,
        p.p_over_calibrated as p_calib,
        {matchup_sel}
        {assist_sel}
        
        -- Outcomes
        f.goals,
        f.assists,
        f.points,
        f.sog,
        f.blocks,
        f.toi_minutes,
        
        -- Dimensions
        dp.primary_position as position
        
    FROM {table} p
    JOIN fact_player_game_features f ON p.player_id = f.player_id AND p.game_id = f.game_id
    LEFT JOIN dim_players dp ON p.player_id = dp.player_id
    WHERE 1=1
    {date_filter}
    AND p.market IN ('GOALS', 'ASSISTS', 'POINTS', 'SOG', 'BLOCKS')
    """
    
    print(f"Executing query...")
    try:
        df = con.execute(query).df()
    except Exception as e:
        print(f"Error executing query: {e}")
        con.close()
        return pd.DataFrame()
        
    con.close()
    print(f"Loaded {len(df)} rows.")
    return df

def process_outcomes(df):
    """Adds 'y_true' column based on market and line."""
    print("--- Processing Outcomes ---")
    df['y_true'] = np.nan
    
    market_map = {
        'GOALS': 'goals',
        'ASSISTS': 'assists',
        'POINTS': 'points',
        'SOG': 'sog',
        'BLOCKS': 'blocks'
    }
    
    for market, col in market_map.items():
        mask = df['market'] == market
        if mask.sum() > 0:
            df.loc[mask, 'y_true'] = (df.loc[mask, col] > df.loc[mask, 'line']).astype(int)
            
    df = df.dropna(subset=['y_true'])
    return df

def calculate_metrics(y_true, y_pred):
    if len(y_true) == 0: return {}
    
    # Use shared metric functions
    ll = compute_log_loss(y_true, y_pred)
    bs = compute_brier_score(y_true, y_pred)
    
    return {
        'log_loss': ll,
        'brier_score': bs,
        'avg_prob': np.mean(y_pred),
        'obs_freq': np.mean(y_true),
        'n': len(y_true)
    }

def plot_reliability_curve(y_true, y_prob, label, ax):
    frac_pos, mean_pred = calibration_curve(y_true, y_prob, n_bins=10)
    ax.plot(mean_pred, frac_pos, "s-", label=label)
    ax.plot([0, 1], [0, 1], "k--", label="Perfectly Calibrated")
    ax.set_ylabel("Fraction of positives")
    ax.set_xlabel("Mean predicted value")
    ax.legend(loc="lower right")

def run_evaluation(args):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    df = load_data(args.start_date, args.end_date, args.table)
    if df.empty:
        print("No data found.")
        return

    df = process_outcomes(df)
    
    # Dimensions
    df['toi_bucket'] = pd.cut(df['toi_minutes'], bins=[0, 13, 16, 19, 100], labels=['<13', '13-16', '16-19', '19+'])
    
    slices = []
    
    def evaluate_slice(sub_df, slice_type, slice_val):
        # Raw
        m_raw = calculate_metrics(sub_df['y_true'].values, sub_df['p_raw'].values)
        row = {'slice_type': slice_type, 'slice_value': slice_val, 'model': 'Raw'}
        row.update(m_raw)
        slices.append(row)
        
        # Calibrated
        # Handle case where p_calib might be null (fallback to raw)
        p_calib = sub_df['p_calib'].fillna(sub_df['p_raw']).values
        m_cal = calculate_metrics(sub_df['y_true'].values, p_calib)
        row = {'slice_type': slice_type, 'slice_value': slice_val, 'model': 'Calibrated'}
        row.update(m_cal)
        slices.append(row)

    # 1. Global
    evaluate_slice(df, 'Global', 'All')
    
    # 2. Market
    for m in df['market'].unique():
        evaluate_slice(df[df['market'] == m], 'Market', m)
        
    # 3. Position
    for p in df['position'].unique():
        if p: evaluate_slice(df[df['position'] == p], 'Position', p)
        
    # 4. TOI Bucket
    for b in df['toi_bucket'].unique():
        if pd.isna(b): continue
        evaluate_slice(df[df['toi_bucket'] == b], 'TOI Bucket', str(b))

    # 5. Matchup Type (if available)
    if 'matchup_type' in df.columns:
        for m in df['matchup_type'].unique():
            if m and m != 'none':
                evaluate_slice(df[df['matchup_type'] == m], 'Matchup', m)

    # 6. Assist Cluster (if available)
    if 'assist_cluster' in df.columns:
        for c in df['assist_cluster'].unique():
            if c and c != 'none' and c != 'unclustered':
                evaluate_slice(df[df['assist_cluster'] == c], 'Assist Cluster', c)

    results_df = pd.DataFrame(slices)
    csv_path = os.path.join(OUTPUT_DIR, f'eval_results_{timestamp}.csv')
    results_df.to_csv(csv_path, index=False)
    print(f"Results saved to {csv_path}")
    
    # Generate Markdown Report
    md_path = os.path.join(OUTPUT_DIR, f'eval_report_{timestamp}.md')
    with open(md_path, 'w') as f:
        f.write("# Forecast Evaluation Report\n\n")
        f.write(f"**Date:** {datetime.now()}\n\n")
        
        # Pivot table for Raw vs Calibrated comparison
        pivot = results_df.pivot(index=['slice_type', 'slice_value'], columns='model', values=['log_loss', 'brier_score'])
        f.write("## Comparison (Raw vs Calibrated)\n")
        f.write(pivot.to_markdown())
        f.write("\n\n")
        
    print(f"Report saved to {md_path}")

    # Generate Plots
    if args.generate_plots:
        print("Generating reliability plots...")
        plot_dir = os.path.join(OUTPUT_DIR, f'plots_{timestamp}')
        os.makedirs(plot_dir, exist_ok=True)
        
        for market in df['market'].unique():
            sub = df[df['market'] == market]
            fig, ax = plt.subplots(figsize=(8, 6))
            
            plot_reliability_curve(sub['y_true'], sub['p_raw'], f"Raw ({market})", ax)
            
            p_calib = sub['p_calib'].fillna(sub['p_raw'])
            plot_reliability_curve(sub['y_true'], p_calib, f"Calibrated ({market})", ax)
            
            ax.set_title(f"Reliability Curve - {market}")
            plt.savefig(os.path.join(plot_dir, f'reliability_{market}.png'))
            plt.close()

    # Save Manifest
    import subprocess
    git_sha = "unknown"
    try:
        git_sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    except:
        pass

    # Scoring Alphas
    alphas = {}
    alpha_path = os.environ.get('NHL_BETS_SCORING_ALPHA_OVERRIDE_PATH')
    if alpha_path and os.path.exists(alpha_path):
        with open(alpha_path, 'r') as f:
            alphas = json.load(f)

    manifest = {
        'timestamp': timestamp,
        'git_sha': git_sha,
        'table_evaluated': args.table,
        'resolved_logic': {
            'scoring_alphas': alphas,
            'variance_mode': args.variance_mode,
            'calibration_mode': args.calibration
        },
        'metrics_global': results_df[results_df['slice_type'] == 'Global'].to_dict(orient='records'),
        'row_count': len(df)
    }
    json_path = os.path.join(OUTPUT_DIR, f'run_manifest_{timestamp}.json')
    eval_manifest_path = os.path.join(OUTPUT_DIR, f'eval_manifest_{args.table}.json')
    
    with open(json_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    with open(eval_manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)
        
    print(f"Eval manifest saved to {eval_manifest_path}")

if __name__ == "__main__":
    args = parse_args()
    run_evaluation(args)

import os
import subprocess
import sys
import pandas as pd
import json
import duckdb
import argparse
from datetime import datetime

# Configuration
WINDOWS = [
    {"id": "Season23-24_Full", "start": "2023-10-10", "end": "2024-04-18"},
    {"id": "Season23-24_Nov", "start": "2023-11-01", "end": "2023-12-01"},
    {"id": "Season23-24_Jan", "start": "2024-01-01", "end": "2024-02-01"},
    {"id": "Season23-24_Mar", "start": "2024-03-01", "end": "2024-04-01"}
]

# Paths
BUILDER_SCRIPT = "pipelines/backtesting/build_probability_snapshots.py"
EVAL_SCRIPT = "pipelines/backtesting/evaluate_forecast_accuracy.py"
PROD_ALPHAS = "data/models/alpha_overrides/best_scoring_alphas.json"
PROD_BETAS = "outputs/beta_optimization/final_betas.json"
PROD_INTERACTIONS = "outputs/beta_optimization/optimized_interactions.json"

def run_command(cmd, env=None, description=""):
    print(f"--- {description} ---")
    print(f"CMD: {' '.join(cmd)}")
    try:
        subprocess.check_call(cmd, env=env)
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dev", action="store_true", help="Run with reduced windows for development/testing")
    parser.add_argument("--scope", choices=["primary_lines", "all_lines"], default="primary_lines", help="Evaluation scope: 'primary_lines' (0.5 for scoring) or 'all_lines'")
    args = parser.parse_args()

    if args.dev:
        print("!!! RUNNING IN DEV MODE (REDUCED WINDOWS) !!!")
        WINDOWS = [
            {"id": "Dev_Week", "start": "2023-11-01", "end": "2023-11-07"}
        ]
    else:
        WINDOWS = [
            {"id": "Season23-24_Full", "start": "2023-10-10", "end": "2024-04-18"},
            {"id": "Season23-24_Nov", "start": "2023-11-01", "end": "2023-12-01"},
            {"id": "Season23-24_Jan", "start": "2024-01-01", "end": "2024-02-01"},
            {"id": "Season23-24_Mar", "start": "2024-03-01", "end": "2024-04-01"}
        ]
    
    # ... (rest of imports and setup)

    results = []
    
    # Base Env
    base_env = os.environ.copy()
    
    # Ensure Prod Assets Exist
    if not os.path.exists(PROD_ALPHAS):
        print(f"Error: Missing Prod Alphas at {PROD_ALPHAS}")
        sys.exit(1)

    prod_env = base_env.copy()
    prod_env["NHL_BETS_SCORING_ALPHA_OVERRIDE_PATH"] = PROD_ALPHAS
    if os.path.exists(PROD_BETAS):
        prod_env["NHL_BETS_BETA_OVERRIDE_PATH"] = PROD_BETAS
    if os.path.exists(PROD_INTERACTIONS):
        prod_env["NHL_BETS_INTERACTIONS_PATH"] = PROD_INTERACTIONS

    os.makedirs("outputs/robustness", exist_ok=True)
    os.makedirs("outputs/eval", exist_ok=True)

    for window in WINDOWS:
        w_id = window['id']

        start = window['start']
        end = window['end']
        
        print(f"\n=== Processing Window: {w_id} ({start} to {end}) ===\n")
        
        # 1. Baseline Run
        # Config: nb_dynamic, global calib, no interactions (defaults)
        tbl_base = f"robust_base_{w_id}"
        cmd_base = [
            sys.executable, BUILDER_SCRIPT,
            "--start_date", start, "--end_date", end,
            "--model-version", "baseline_v1",
            "--calibration", "global",
            "--variance_mode", "nb_dynamic",
            "--output_table", tbl_base
        ]
        run_command(cmd_base, env=base_env, description=f"Building Baseline for {w_id}")
        
        # 2. Prod Run
        # Config: all_nb, tail_bucket calib, interactions, alphas
        tbl_prod = f"robust_prod_{w_id}"
        cmd_prod = [
            sys.executable, BUILDER_SCRIPT,
            "--start_date", start, "--end_date", end,
            "--model-version", "prod_exp_b",
            "--calibration", "tail_bucket",
            "--variance_mode", "all_nb",
            "--use_interactions",
            "--output_table", tbl_prod
        ]
        run_command(cmd_prod, env=prod_env, description=f"Building Prod for {w_id}")
        
        # 3. Evaluate Both
        for variant, tbl in [("Baseline", tbl_base), ("Prod", tbl_prod)]:
            out_csv = f"outputs/robustness/{w_id}_{variant}.csv"
            out_md = f"outputs/robustness/{w_id}_{variant}.md"
            cmd_eval = [
                sys.executable, EVAL_SCRIPT,
                "--table", tbl,
                "--out-csv", out_csv,
                "--out-md", out_md
            ]
            run_command(cmd_eval, env=base_env, description=f"Evaluating {variant} for {w_id}")
            
            # Read Results
            if os.path.exists(out_csv):
                df = pd.read_csv(out_csv)
                
                # Filter for Primary Lines if requested
                if args.scope == "primary_lines" and not df.empty and 'Line' in df.columns and 'Market' in df.columns:
                    # Logic: Scoring props (Goals, Assists, Points) -> Line 0.5
                    # Other props (SOG, Blocks) -> Keep all (lines vary too much)
                    scoring_mask = df['Market'].isin(['player_anytime_goal', 'player_assists', 'player_points'])
                    primary_mask = (scoring_mask & (df['Line'] == 0.5)) | (~scoring_mask)
                    df = df[primary_mask]

                # Aggregate to Global Level
                # Strategy: Group by (Market, Line) and select the best Variant (Calibrated > Raw)
                # Then compute weighted average across all unique Market/Line tuples.
                
                selected_rows = []
                if not df.empty:
                    # Ensure we have the columns we expect
                    req_cols = ['Market', 'Line', 'Variant', 'Count', 'Log Loss', 'Brier Score']
                    if all(c in df.columns for c in req_cols):
                        groups = df.groupby(['Market', 'Line'])
                        for name, group in groups:
                            # Prioritize Calibrated
                            if 'Calibrated' in group['Variant'].values:
                                best = group[group['Variant'] == 'Calibrated'].iloc[0]
                            else:
                                best = group[group['Variant'] == 'Raw'].iloc[0]
                            selected_rows.append(best)
                        
                        if selected_rows:
                            df_clean = pd.DataFrame(selected_rows)
                            total_count = df_clean['Count'].sum()
                            
                            if total_count > 0:
                                weighted_ll = (df_clean['Log Loss'] * df_clean['Count']).sum() / total_count
                                weighted_brier = (df_clean['Brier Score'] * df_clean['Count']).sum() / total_count
                            else:
                                weighted_ll = float('nan')
                                weighted_brier = float('nan')
                        else:
                             weighted_ll = float('nan')
                             weighted_brier = float('nan')
                             total_count = 0
                    else:
                        print(f"Warning: Missing expected columns in {out_csv}")
                        weighted_ll = float('nan')
                        weighted_brier = float('nan')
                        total_count = 0
                else:
                    weighted_ll = float('nan')
                    weighted_brier = float('nan')
                    total_count = 0

                results.append({
                    "Window": w_id,
                    "Variant": variant,
                    "Log Loss": weighted_ll,
                    "Brier Score": weighted_brier,
                    "Samples": total_count
                })

    # Summary Report
    df_res = pd.DataFrame(results)
    
    # Pivot
    df_pivot = df_res.pivot(index="Window", columns="Variant", values=["Log Loss", "Brier Score"])
    
    # Calculate Deltas
    # Positive Delta = Prod is Better (Baseline was Higher Error)
    df_pivot[('Delta', 'Log Loss')] = df_pivot[('Log Loss', 'Baseline')] - df_pivot[('Log Loss', 'Prod')]
    df_pivot[('Delta', 'Brier')] = df_pivot[('Brier Score', 'Baseline')] - df_pivot[('Brier Score', 'Prod')]

    
    print("\n=== Robustness Sweep Results ===\n")
    print(df_pivot)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = f"outputs/eval/robustness_leaderboard_{args.scope}_{timestamp}.md"
    
    with open(out_path, "w") as f:
        f.write(f"# Robustness Sweep Leaderboard ({args.scope})\n\n")
        f.write(f"Generated: {datetime.now()}\n\n")
        f.write("Positive Delta = Prod Improved (Lower Error)\n\n")
        f.write(df_pivot.to_markdown())
        
        f.write("\n\n## Raw Data\n")
        f.write(df_res.to_markdown(index=False))
        
    print(f"Saved leaderboard to {out_path}")
    
    # --- Integration: Run Regression Gate ---
    # We aggregate all windows to check if the prod profile is strictly worse overall
    # But for now, let's just pick the largest window (Season23-24_Full) as the gatekeeper
    
    # Use the first window in the list as the primary gate (Full season in prod, Dev_Week in dev)
    gate_window = WINDOWS[0]['id']
    gate_cand = f"robust_prod_{gate_window}"
    gate_base = f"robust_base_{gate_window}"
    
    gate_script = "scripts/check_regression_gate.py"
    gate_report = f"outputs/eval/regression_gate_report_{args.scope}_{timestamp}.md"
    
    print(f"\n=== Running Regression Gate on {gate_window} ===\n")
    try:
        cmd_gate = [
            sys.executable, gate_script,
            "--candidate_table", gate_cand,
            "--baseline_table", gate_base,
            "--output_report", gate_report
        ]
        run_command(cmd_gate, env=base_env, description="Regression Gate Check")
        print(">> Regression Gate PASSED.")
    except SystemExit:
        print(">> Regression Gate FAILED.")
        sys.exit(1)
        
    # --- Persistence: Update Latest Reference ---
    # If we passed, we can update LATEST pointers (handled by nightly script mostly, but we can dump a json here)
    latest_meta = {
        "timestamp": timestamp,
        "leaderboard": out_path,
        "gate_report": gate_report,
        "baseline_table": gate_base,
        "prod_table": gate_cand,
        "scope": args.scope
    }
    with open("outputs/eval/latest_robustness_run.json", "w") as f:
        json.dump(latest_meta, f, indent=2)

if __name__ == "__main__":
    main()

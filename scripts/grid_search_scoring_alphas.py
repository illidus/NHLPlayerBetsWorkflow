import os
import json
import itertools
import subprocess
import argparse
import pandas as pd
from datetime import datetime

# Config
START_DATE = "2023-11-01"
END_DATE = "2024-02-01"
OUTPUT_DIR = "outputs/alpha_tuning"
TEMP_CONFIG_FILE = os.path.join(OUTPUT_DIR, "temp_scoring_alphas.json")
RESULTS_FILE = os.path.join(OUTPUT_DIR, "scoring_alpha_grid.csv")
BEST_CONFIG_FILE = os.path.join(OUTPUT_DIR, "best_scoring_alphas.json")

# Search Space
GRID = {
    'GOALS': [0.02, 0.05, 0.10, 0.20], # Skip 0.35 to save time for now
    'ASSISTS': [0.05, 0.10, 0.20],
    'POINTS': [0.05, 0.10, 0.20]
}

def run_grid_search(quick=False):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Generate combinations
    keys = list(GRID.keys())
    values = list(GRID.values())
    combinations = list(itertools.product(*values))
    
    if quick:
        print("Quick mode: limiting combinations")
        combinations = combinations[:2]
    
    results = []
    
    print(f"Starting Grid Search with {len(combinations)} combinations...")
    
    for i, combo in enumerate(combinations):
        config = dict(zip(keys, combo))
        print(f"\nIter {i+1}/{len(combinations)}: {config}")
        
        # 1. Write temp config
        with open(TEMP_CONFIG_FILE, 'w') as f:
            json.dump(config, f)
            
        # 2. Run Backtest
        # We need to run build_probability_snapshots with variance_mode='all_nb'
        # The script will pick up the env var
        env = os.environ.copy()
        env['NHL_BETS_SCORING_ALPHA_OVERRIDE_PATH'] = TEMP_CONFIG_FILE
        
        model_version = f"alpha_search_{i}"
        
        cmd_build = [
            "python", "pipelines/backtesting/build_probability_snapshots.py",
            "--start_date", START_DATE,
            "--end_date", END_DATE,
            "--model-version", model_version,
            "--variance_mode", "all_nb",
            "--calibration", "none" # Raw LL optimization
        ]
        
        try:
            subprocess.check_call(cmd_build, env=env, stdout=subprocess.DEVNULL)
        except subprocess.CalledProcessError:
            print("Backtest failed.")
            continue
            
        # 3. Evaluate
        cmd_eval = [
            "python", "scripts/evaluate_log_loss.py",
            "--start_date", START_DATE,
            "--end_date", END_DATE,
            "--table", f"fact_probabilities_{model_version}"
        ]
        
        try:
            # We need to parse the JSON output from a file or stdout
            # evaluate_log_loss saves a manifest. Let's find it.
            # Actually, evaluate_log_loss saves results to CSV.
            # We can also parse stdout if we modify evaluate_log_loss to output JSON or similar.
            # But relying on the latest manifest in outputs/eval is the current pattern.
            
            subprocess.check_call(cmd_eval, env=env, stdout=subprocess.DEVNULL)
            
            # Find latest manifest
            manifest_files = [
                os.path.join("outputs/eval", f) 
                for f in os.listdir("outputs/eval") 
                if f.startswith("run_manifest_")
            ]
            if not manifest_files:
                continue
                
            latest = max(manifest_files, key=os.path.getctime)
            with open(latest, 'r') as f:
                data = json.load(f)
                
            # Need market-specific Log Loss. 
            # The manifest currently only has global metrics or we need to read the CSV.
            # Reading the CSV produced by evaluate_log_loss is safer.
            # The CSV filename is in the log or we can find the latest csv in outputs/eval.
            
            csv_files = [
                os.path.join("outputs/eval", f) 
                for f in os.listdir("outputs/eval") 
                if f.startswith("logloss_by_slice_")
            ]
            latest_csv = max(csv_files, key=os.path.getctime)
            df_res = pd.read_csv(latest_csv)
            
            # Extract Market LLs
            ll_goals = df_res[df_res['slice_value'] == 'GOALS']['log_loss'].iloc[0]
            ll_assists = df_res[df_res['slice_value'] == 'ASSISTS']['log_loss'].iloc[0]
            ll_points = df_res[df_res['slice_value'] == 'POINTS']['log_loss'].iloc[0]
            
            # Weighted Score
            score = 0.4*ll_goals + 0.3*ll_assists + 0.3*ll_points
            
            res_row = config.copy()
            res_row.update({
                'll_goals': ll_goals,
                'll_assists': ll_assists,
                'll_points': ll_points,
                'weighted_score': score,
                'model_version': model_version
            })
            results.append(res_row)
            
            print(f"  Score: {score:.5f} (G:{ll_goals:.4f}, A:{ll_assists:.4f}, P:{ll_points:.4f})")
            
        except Exception as e:
            print(f"Evaluation failed: {e}")
            
    # Save Results
    df_results = pd.DataFrame(results).sort_values('weighted_score')
    df_results.to_csv(RESULTS_FILE, index=False)
    print(f"\nResults saved to {RESULTS_FILE}")
    
    # Save Best
    if not df_results.empty:
        best = df_results.iloc[0]
        best_config = {k: best[k] for k in keys}
        with open(BEST_CONFIG_FILE, 'w') as f:
            json.dump(best_config, f, indent=4)
        print(f"Best Configuration saved to {BEST_CONFIG_FILE}")
        print(best_config)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--start_date", type=str, default=START_DATE, help="YYYY-MM-DD")
    parser.add_argument("--end_date", type=str, default=END_DATE, help="YYYY-MM-DD")
    args = parser.parse_args()
    
    # Update Globals from Args
    START_DATE = args.start_date
    END_DATE = args.end_date
    
    run_grid_search(args.quick)

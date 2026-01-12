import os
import sys
import json
import argparse
import subprocess
import pandas as pd
import numpy as np
from datetime import datetime
from scipy.optimize import minimize

# --- Configuration ---
# Reduce scope to make optimization loop faster
# Start Date: 2023-10-01 (Skip early season noise)
# End Date: 2024-04-01 (Mid season stable)
START_DATE = "2023-10-01"
END_DATE = "2024-04-01"
MODEL_VERSION = "zscore_v1"
OUTPUT_DIR = "outputs/beta_optimization_zscore"
TEMP_BETA_FILE = os.path.join(OUTPUT_DIR, "temp_betas.json")
HISTORY_FILE = os.path.join(OUTPUT_DIR, "opt_logloss_history.csv")
FINAL_FILE = os.path.join(OUTPUT_DIR, "final_betas_zscore.json")

# Initial Guesses (Perturbed)
INITIAL_BETAS = {
    'opp_sog': 0.18, # perturbed from 0.15
    'opp_g': 0.18, # perturbed from 0.15
    'goalie': 0.25, # perturbed from 0.20
    'itt': 0.50,
    'b2b': -0.05,
    'pace': 0.55 # perturbed from 0.50
}

# Parameter Keys (Ordered for vectorization)
PARAM_KEYS = ['opp_sog', 'opp_g', 'goalie', 'pace', 'b2b'] # Fixed ITT for now to reduce dims

def run_pipeline(beta_dict):
    """
    Runs the build_probability_snapshots.py pipeline with the given betas.
    Returns: Parsed Log Loss (Global).
    """
    # 1. Write temp betas
    with open(TEMP_BETA_FILE, 'w') as f:
        json.dump(beta_dict, f)
        
    # 2. Set Env Var
    env = os.environ.copy()
    env['NHL_BETS_BETA_OVERRIDE_PATH'] = TEMP_BETA_FILE
    
    # 3. Run Build Snapshots (Raw Probabilities Generation)
    # We use 'raw' (calibration='none' or just raw column evaluation) for optimization
    # The evaluation script calculates log loss on 'p_raw' (p_over).
    # build_probability_snapshots supports --calibration argument, but it mainly affects p_calib column.
    # We care about minimizing Raw Log Loss primarily to improve the core model.
    # Or should we minimize Calibrated Log Loss?
    # Prompt says: "Use only raw (pre-calibrated) log loss during optimization"
    
    cmd_build = [
        "python", "pipelines/backtesting/build_probability_snapshots.py",
        "--start_date", START_DATE,
        "--end_date", END_DATE,
        "--model-version", MODEL_VERSION,
        "--calibration", "none" # Speed up, no need to apply calibrators
    ]
    
    try:
        # Run silently
        subprocess.check_call(cmd_build, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        print("Error in build_probability_snapshots pipeline.")
        return 999.0 # High penalty

    # 4. Run Evaluation
    cmd_eval = [
        "python", "scripts/evaluate_log_loss.py",
        "--start_date", START_DATE,
        "--end_date", END_DATE,
        "--table", f"fact_probabilities_{MODEL_VERSION}",
        "--calibration", "raw"
    ]
    
    # Capture output to parse log loss
    try:
        # result = subprocess.run(cmd_eval, env=env, capture_output=True, text=True)
        # Just run it, we read manifest
        subprocess.check_call(cmd_eval, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # Hack: Parse stdout or find latest manifest
        # Stdout usually contains markdown summary.
        # Let's rely on manifest reading.
        manifest_files = [
            os.path.join("outputs/eval", f) 
            for f in os.listdir("outputs/eval") 
            if f.startswith("run_manifest_")
        ]
        if not manifest_files:
            return 999.0
            
        latest_manifest = max(manifest_files, key=os.path.getctime)
        
        with open(latest_manifest, 'r') as f:
            data = json.load(f)
            
        ll = data.get('metrics_global', {}).get('log_loss', 999.0)
        brier = data.get('metrics_global', {}).get('brier_score', 999.0)
        
        return ll, brier
        
    except Exception as e:
        print(f"Error in evaluation: {e}")
        return 999.0, 999.0

iteration_count = 0

def objective_function(x):
    global iteration_count
    iteration_count += 1
    
    # Construct Beta Dict
    betas = INITIAL_BETAS.copy()
    for i, key in enumerate(PARAM_KEYS):
        betas[key] = float(x[i])
        
    print(f"Iter {iteration_count}: Testing Betas {betas}...", end="", flush=True)
    
    ll, brier = run_pipeline(betas)
    
    print(f" LogLoss: {ll:.8f}")
    
    # Log History
    with open(HISTORY_FILE, 'a') as f:
        # Iter, LogLoss, Brier, Betas...
        line = f"{iteration_count},{ll},{brier},{datetime.now()},".join([str(x[i]) for i in range(len(x))]) + "\n"
        f.write(line)
        
    return ll

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Initialize History File
    with open(HISTORY_FILE, 'w') as f:
        header = "Iteration,LogLoss,Brier,Timestamp,".join(PARAM_KEYS) + "\n"
        f.write(header)
        
    print(f"Starting Beta Optimization (Nelder-Mead)")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print(f"Parameters: {PARAM_KEYS}")
    
    x0 = [INITIAL_BETAS[k] for k in PARAM_KEYS]
    
    # Run Optimization
    # We use Nelder-Mead as it's robust for non-differentiable/noisy functions (like our simulation)
    # Bounds aren't strictly supported by standard Nelder-Mead implementation in scipy, 
    # but we can assume reasonable behavior or use Powell.
    # Powell is also good.
    
    res = minimize(objective_function, x0, method='Nelder-Mead', options={'maxiter': 20, 'disp': True})
    
    print("\nOptimization Complete!")
    print(f"Best Log Loss: {res.fun}")
    print(f"Best Parameters: {res.x}")
    
    # Save Final
    final_betas = INITIAL_BETAS.copy()
    for i, key in enumerate(PARAM_KEYS):
        final_betas[key] = float(res.x[i])
        
    with open(FINAL_FILE, 'w') as f:
        json.dump(final_betas, f, indent=4)
        
    print(f"Final Betas saved to {FINAL_FILE}")
    
    # Generate Leaderboard Entry
    with open("outputs/eval/logloss_leaderboard_optimized.md", "w") as f:
        f.write("# Optimized Beta Performance\n")
        f.write(f"**Date Range:** {START_DATE} to {END_DATE}\n\n")
        f.write(f"**Final Log Loss:** {res.fun:.5f}\n")
        f.write(f"**Parameters:**\n")
        f.write("```json\n")
        json.dump(final_betas, f, indent=4)
        f.write("\n```\n")

if __name__ == "__main__":
    main()

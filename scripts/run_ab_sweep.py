import subprocess
import os
import glob
import json
import pandas as pd
from datetime import datetime

# Script Paths
CTX_SCRIPT = "src/nhl_bets/projections/produce_game_context.py"
PROBS_SCRIPT = "src/nhl_bets/projections/single_game_probs.py"
EVAL_SCRIPT = "scripts/evaluate_log_loss.py"
EVAL_DIR = "outputs/eval"

# Variants
VARIANTS = [
    {
        'id': 'base',
        'clusters': 0,
        'team': 0,
        'calib': 'global',
        'notes': 'Baseline'
    },
    {
        'id': 'team',
        'clusters': 0,
        'team': 1,
        'calib': 'global',
        'notes': '+ Team metrics only'
    },
    {
        'id': 'cluster',
        'clusters': 1,
        'team': 0,
        'calib': 'global',
        'notes': '+ Clusters only'
    },
    {
        'id': 'calseg',
        'clusters': 0,
        'team': 0,
        'calib': 'segmented',
        'notes': 'Segmented calibrators'
    },
    {
        'id': 'full',
        'clusters': 1,
        'team': 1,
        'calib': 'segmented',
        'notes': 'All features on'
    }
]

def run_step(cmd, desc):
    print(f"--- Running {desc} ---")
    print(f"Command: {' '.join(cmd)}")
    try:
        subprocess.check_call(cmd, shell=False) 
    except subprocess.CalledProcessError as e:
        print(f"Error running {desc}: {e}")
        return False
    return True

def get_latest_manifest():
    files = glob.glob(os.path.join(EVAL_DIR, "run_manifest_*.json"))
    if not files:
        return None
    latest_file = max(files, key=os.path.getctime)
    try:
        with open(latest_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading manifest {latest_file}: {e}")
        return None

def main():
    print("Starting A/B Evaluation Sweep...")
    
    results = []
    
    for v in VARIANTS:
        vid = v['id']
        print(f"\n=== Evaluating Variant: {vid} ===")
        
        # 1. Produce Context
        cmd_ctx = [
            "python", CTX_SCRIPT,
            "--use_team_metrics", str(v['team']),
            "--use_clusters", str(v['clusters'])
        ]
        if not run_step(cmd_ctx, f"Context ({vid})"): continue
        
        # 2. Generate Projections
        cmd_probs = [
            "python", PROBS_SCRIPT,
            "--calibration_mode", v['calib']
        ]
        if not run_step(cmd_probs, f"Projections ({vid})"): continue
        
        # 3. Evaluate
        # Logic for eval mode:
        eval_mode = 'raw'
        if v['calib'] == 'segmented' or v['id'] == 'calseg' or v['id'] == 'full': 
             eval_mode = 'calibrated'
        
        cmd_eval = [
            "python", EVAL_SCRIPT,
            "--start_date", "2023-10-01",
            "--calibration", eval_mode
        ]
        
        if not run_step(cmd_eval, f"Evaluation ({vid})"): continue
        
        # 4. Harvest Metrics
        manifest = get_latest_manifest()
        if manifest:
            glob_metrics = manifest.get('metrics_global', {})
            res = {
                'Variant': vid,
                'Clusters': '✅' if v['clusters'] else '❌',
                'Team': '✅' if v['team'] else '❌',
                'Calibrator': v['calib'],
                'LogLoss': round(glob_metrics.get('log_loss', 0.0), 4),
                'Brier': round(glob_metrics.get('brier_score', 0.0), 4),
                'Notes': v['notes']
            }
            results.append(res)
        else:
            print("No manifest found for this run.")

    # Generate Leaderboard
    df = pd.DataFrame(results)
    
    # Save Markdown
    out_md = os.path.join(EVAL_DIR, "logloss_leaderboard.md")
    with open(out_md, 'w', encoding='utf-8') as f:
        f.write(f"# Log Loss Leaderboard (2023-10-01 to Present)\n\n")
        f.write(df.to_markdown(index=False))
        f.write("\n")
    
    print(f"\nLeaderboard generated at {out_md}")
    print(df.to_markdown(index=False))

if __name__ == "__main__":
    main()
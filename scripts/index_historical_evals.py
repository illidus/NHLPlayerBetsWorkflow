import os
import re
import json
import pandas as pd
from datetime import datetime

# Paths
OUTPUTS_DIR = "outputs"
EVAL_DIR = os.path.join(OUTPUTS_DIR, "eval")
LEADERBOARD_MD = os.path.join(EVAL_DIR, "MASTER_BACKTEST_LEADERBOARD.md")
LEADERBOARD_CSV = os.path.join(EVAL_DIR, "MASTER_BACKTEST_LEADERBOARD.csv")
EXCLUDED_MD = os.path.join(EVAL_DIR, "EXCLUDED_RUNS.md")

# Regex Patterns
# Matches: eval_report_YYYYMMDD_HHMMSS.md
REPORT_PATTERN = re.compile(r"eval_report_(\d{8}_\d{6})\.md")
# Matches: logloss_summary_YYYYMMDD_HHMMSS.md
SUMMARY_PATTERN = re.compile(r"logloss_summary_(\d{8}_\d{6})\.md")
# Matches: run_manifest_YYYYMMDD_HHMMSS.json
MANIFEST_PATTERN = re.compile(r"run_manifest_(\d{8}_\d{6})\.json")
# Matches: eval_manifest_*.json (often contains run info)
EVAL_MANIFEST_PATTERN = re.compile(r"eval_manifest_.*\.json")

def parse_markdown_metrics(file_path):
    """
    Attempts to extract Global Log Loss and Brier Score from markdown tables.
    """
    log_loss = None
    brier = None
    try:
        with open(file_path, 'r') as f:
            for line in f:
                # Naive parsing of markdown tables
                # | Global | All | Raw | 0.2564 | 0.0777 | ...
                if "| Global |" in line or "| All |" in line:
                    parts = [p.strip() for p in line.split('|')]
                    # Look for float-like things
                    floats = []
                    for p in parts:
                        try:
                            floats.append(float(p))
                        except:
                            pass
                    
                    # Typical structure: LogLoss is often the first or second float
                    # Heuristic: LogLoss usually ~0.20-0.30, Brier ~0.07-0.09
                    for val in floats:
                        if 0.15 < val < 0.40 and log_loss is None:
                            log_loss = val
                        elif 0.05 < val < 0.10 and brier is None:
                            brier = val
    except:
        pass
    return log_loss, brier

def scan_files():
    runs = {}
    
    # 1. Scan for Manifests (Highest Confidence)
    # Look in outputs/runs for run_manifests
    runs_dir = os.path.join(OUTPUTS_DIR, "runs")
    if os.path.exists(runs_dir):
        for f in os.listdir(runs_dir):
            match = MANIFEST_PATTERN.match(f)
            if match:
                run_id = match.group(1)
                full_path = os.path.join(runs_dir, f)
                try:
                    with open(full_path, 'r') as jf:
                        data = json.load(jf)
                        runs[run_id] = {
                            "run_id": run_id,
                            "date": data.get("timestamp", "")[:10],
                            "profile": data.get("profile", "custom"),
                            "log_loss": None, # Manifests usually don't have results, eval manifests do
                            "brier_score": None,
                            "scope": "Global",
                            "notes": "Manifest Found",
                            "lineage": "official",
                            "manifest_path": full_path
                        }
                except:
                    pass

    # 2. Scan Eval Directory for Eval Manifests and Reports
    if os.path.exists(EVAL_DIR):
        for f in os.listdir(EVAL_DIR):
            # Eval Manifests often contain the metrics directly
            if f.startswith("eval_manifest_") and f.endswith(".json"):
                 try:
                    with open(os.path.join(EVAL_DIR, f), 'r') as jf:
                        data = json.load(jf)
                        # Extract timestamp from filename or content
                        # Filename might not have timestamp if it is "eval_manifest_fact_prob_A.json"
                        # But data usually has "timestamp"
                        ts = data.get("timestamp", "unknown")
                        if ts == "unknown":
                             continue
                             
                        # Normalize TS to run_id format if possible: YYYYMMDD_HHMMSS
                        # If TS is ISO: 2026-01-11T00:20:06.123 -> 20260111_002006
                        run_id = ts.replace("-","").replace(":","").replace("T","")[:15]
                        
                        metrics = data.get("metrics_global", {})
                        
                        # Determine profile from args
                        args = data.get("args", {})
                        table = args.get("table", "")
                        profile_guess = "custom"
                        if "experiment_B" in table or "production" in table:
                            profile_guess = "production_experiment_b"
                        elif "experiment_A" in table:
                            profile_guess = "experiment_a_baseline"

                        entry = {
                            "run_id": run_id,
                            "date": ts[:10],
                            "profile": profile_guess,
                            "log_loss": metrics.get("log_loss"),
                            "brier_score": metrics.get("brier_score"),
                            "scope": "Global",
                            "notes": f"Eval Manifest ({table})",
                            "lineage": "official",
                            "manifest_path": os.path.join(EVAL_DIR, f)
                        }
                        
                        # Merge or Add
                        if run_id in runs:
                            runs[run_id].update(entry)
                        else:
                            runs[run_id] = entry
                 except:
                     pass

            # Fallback: Parse Markdown Reports for Legacy Runs
            match_rep = REPORT_PATTERN.match(f)
            if match_rep:
                run_id = match_rep.group(1)
                if run_id not in runs:
                    ll, br = parse_markdown_metrics(os.path.join(EVAL_DIR, f))
                    if ll:
                        runs[run_id] = {
                            "run_id": run_id,
                            "date": f"{run_id[:4]}-{run_id[4:6]}-{run_id[6:8]}",
                            "profile": "legacy_inferred",
                            "log_loss": ll,
                            "brier_score": br,
                            "scope": "Global",
                            "notes": "Legacy Report Import",
                            "lineage": "legacy_imported",
                            "manifest_path": None
                        }

    return runs.values()

def generate_leaderboard(runs):
    # Filter valid runs
    valid_runs = []
    excluded_runs = []
    
    seen_ids = set()

    for r in runs:
        if r['run_id'] in seen_ids:
            continue
        seen_ids.add(r['run_id'])

        # Exclusion Logic
        if r['log_loss'] is None:
            r['reason'] = "No Metrics Found"
            excluded_runs.append(r)
            continue
            
        # Suspiciously low/high metrics
        if r['log_loss'] < 0.20 or r['log_loss'] > 0.40:
             r['reason'] = "Metric Outlier"
             excluded_runs.append(r)
             continue
             
        valid_runs.append(r)

    # Sort by Log Loss Ascending
    valid_runs.sort(key=lambda x: x['log_loss'] if x['log_loss'] else 999)

    # Write CSV
    df = pd.DataFrame(valid_runs)
    cols = ["run_id", "date", "profile", "log_loss", "brier_score", "scope", "lineage", "notes"]
    # Ensure columns exist
    for c in cols:
        if c not in df.columns:
            df[c] = ""
            
    df[cols].to_csv(LEADERBOARD_CSV, index=False)
    print(f"Wrote {len(valid_runs)} runs to {LEADERBOARD_CSV}")

    # Write MD
    with open(LEADERBOARD_MD, 'w') as f:
        f.write("# Master Backtest Leaderboard\n\n")
        f.write("**Generated:** " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n")
        f.write("**Governance:** Only 'official' runs have full reproducibility guarantees.\n\n")
        
        f.write("| Rank | Run ID | Date | Profile | Log Loss | Brier | Lineage | Notes |\n")
        f.write("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
        
        for i, r in enumerate(valid_runs):
            rank = i + 1
            f.write(f"| {rank} | `{r['run_id']}` | {r['date']} | {r['profile']} | **{r['log_loss']:.4f}** | {r['brier_score']:.4f} | {r['lineage']} | {r['notes']} |\n")

    # Write Excluded
    with open(EXCLUDED_MD, 'w') as f:
        f.write("# Excluded Runs\n\n")
        f.write("| Run ID | Date | Reason | Notes |\n")
        f.write("| :--- | :--- | :--- | :--- |\n")
        for r in excluded_runs:
             f.write(f"| `{r['run_id']}` | {r['date']} | {r.get('reason','Unknown')} | {r['notes']} |\n")

if __name__ == "__main__":
    runs = scan_files()
    generate_leaderboard(runs)

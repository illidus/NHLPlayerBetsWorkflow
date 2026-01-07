import os
import subprocess
import sys
import pandas as pd
import glob
from datetime import datetime

def run_pipeline(env_vars=None):
    env = os.environ.copy()
    if env_vars:
        env.update(env_vars)
    
    # We use USE_LIVE_BASE_PROJECTIONS=0 to speed up validation
    env["USE_LIVE_BASE_PROJECTIONS"] = "0"
    
    pipeline_script = os.path.join("pipelines", "production", "run_production_pipeline.py")
    
    try:
        subprocess.check_call([sys.executable, pipeline_script], env=env)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Pipeline failed: {e}")
        return False

def get_latest_audit_csv():
    audit_dir = os.path.join("outputs", "audits")
    files = glob.glob(os.path.join(audit_dir, "ev_prob_audit_*.csv"))
    if not files:
        return None
    return max(files, key=os.path.getmtime)

def get_best_bets_xlsx():
    best_bets = os.path.join("outputs", "ev_analysis", "MultiBookBestBets.xlsx")
    return best_bets if os.path.exists(best_bets) else None

def load_validation_frame():
    best_bets = get_best_bets_xlsx()
    if best_bets:
        df = pd.read_excel(best_bets)
        if not df.empty:
            return df, best_bets

    audit_file = get_latest_audit_csv()
    if audit_file:
        df = pd.read_csv(audit_file)
        if not df.empty:
            return df, audit_file

    return None, None

def normalize_validation_columns(df):
    if 'ProbSource' not in df.columns and 'Prob_Source' in df.columns:
        df['ProbSource'] = df['Prob_Source']
    if 'market_key' not in df.columns and 'Market' in df.columns:
        df['market_key'] = df['Market']
    return df

def validate_run(mode_name, expected_calibrated=True):
    print(f"\n>>> Validating Mode: {mode_name}")
    
    df, source_path = load_validation_frame()
    if df is None:
        print(f"FAIL: No validation artifact found for {mode_name}")
        return False
    
    print(f"Checking artifact: {source_path}")
    df = normalize_validation_columns(df)
    
    if df.empty:
        print(f"FAIL: Validation artifact is empty for {mode_name}")
        return False

    # Check for calibrated presence
    has_calibrated = (df['ProbSource'] == 'Calibrated').any()
    
    # Assertions
    results = []
    
    if expected_calibrated:
        # Check ASSISTS and POINTS specifically
        mask_ast_pts = df['market_key'].fillna('').str.contains('Assists|Points', case=False)
        if mask_ast_pts.any():
            sub = df[mask_ast_pts]
            ast_pts_calib = (sub['ProbSource'] == 'Calibrated').any()
            if ast_pts_calib:
                results.append(("[PASS] ASSISTS/POINTS use Calibrated probabilities.", True))
            else:
                results.append(("[FAIL] ASSISTS/POINTS DID NOT use Calibrated probabilities.", False))
        else:
            results.append(("[INFO] No ASSISTS/POINTS bets found in this slate to verify calibration.", True))
            
        # Check GOALS/SOG specifically
        mask_raw = df['market_key'].fillna('').str.contains('Goals|Shots|SOG|Blocks|BLK', case=False)
        if mask_raw.any():
            sub = df[mask_raw]
            raw_is_raw = (sub['ProbSource'] == 'Raw').any()
            if raw_is_raw:
                results.append(("[PASS] GOALS/SOG/BLOCKS use Raw probabilities.", True))
            else:
                results.append(("[FAIL] GOALS/SOG/BLOCKS NOT using Raw probabilities.", False))
    else:
        if has_calibrated:
            results.append(("[FAIL] Calibrated probabilities found when they should be disabled.", False))
        else:
            results.append(("[PASS] No calibrated probabilities used (Disabled Mode).", True))

    for msg, success in results:
        print(msg)
        
    return all(s for m, s in results)

def main():
    print("=== STARTING GOLDEN RUN VALIDATION ===")
    
    # 1. Default Run
    print("\n--- TEST 1: Default Mode (Calibration ON) ---")
    if run_pipeline({"DISABLE_CALIBRATION": "0"}):
        res1 = validate_run("Default", expected_calibrated=True)
    else:
        res1 = False

    # 2. Debug Run
    print("\n--- TEST 2: Debug Mode (Calibration OFF) ---")
    if run_pipeline({"DISABLE_CALIBRATION": "1"}):
        res2 = validate_run("Debug", expected_calibrated=False)
    else:
        res2 = False

    # 3. Accuracy Backtest Existence
    print("\n--- TEST 3: Accuracy Backtest Artifacts ---")
    # Clean old report
    report_path = os.path.join("outputs", "backtest_reports", "forecast_accuracy.md")
    if os.path.exists(report_path):
        os.remove(report_path)
        
    if run_pipeline({"RUN_ACCURACY_BACKTEST": "1"}):
        if os.path.exists(report_path):
            print("[PASS] forecast_accuracy.md generated.")
            res3 = True
        else:
            print("[FAIL] forecast_accuracy.md NOT generated.")
            res3 = False
    else:
        res3 = False

    print("\n" + "="*40)
    print("GOLDEN RUN SUMMARY")
    print("="*40)
    print(f"Default Mode: {'PASS' if res1 else 'FAIL'}")
    print(f"Debug Mode:   {'PASS' if res2 else 'FAIL'}")
    print(f"Backtest Mode: {'PASS' if res3 else 'FAIL'}")
    print("="*40)
    
    if not (res1 and res2 and res3):
        sys.exit(1)

if __name__ == "__main__":
    main()

import os
import subprocess
import sys

def run_step(step_name, command, env=None):
    print(f"--- Starting {step_name} ---")
    try:
        # Using shell=False and command list is safer and better for path handling
        subprocess.check_call(command, shell=False, env=env)
        print(f"--- Finished {step_name} ---\n")
    except subprocess.CalledProcessError as e:
        print(f"!!! Error in {step_name}: {e}")
        sys.exit(1)

def main():
    # Setup Environment
    env = os.environ.copy()
    root_dir = os.getcwd()
    src_path = os.path.join(root_dir, "src")
    
    # Ensure src is in PYTHONPATH so nhl_bets is importable
    if "PYTHONPATH" in env:
        env["PYTHONPATH"] = f"{src_path}{os.pathsep}{env['PYTHONPATH']}"
    else:
        env["PYTHONPATH"] = src_path

    # Flags
    use_api = os.environ.get("USE_SELENIUM_SCRAPER", "0") == "0"
    use_live_base = os.environ.get("USE_LIVE_BASE_PROJECTIONS", "1") == "1"
    run_accuracy_backtest = os.environ.get("RUN_ACCURACY_BACKTEST", "0") == "1"
    
    # Paths to Scripts
    proj_dir = os.path.join("src", "nhl_bets", "projections")
    scrapers_dir = os.path.join("src", "nhl_bets", "scrapers")
    analysis_dir = os.path.join("src", "nhl_bets", "analysis")
    backtest_pipeline_dir = os.path.join("pipelines", "backtesting")
    
    # Data Paths
    data_raw_dir = os.path.join("data", "raw")
    props_path = os.path.join(data_raw_dir, "nhl_player_props_all.csv")
    
    # Output Paths
    output_ev_dir = os.path.join("outputs", "ev_analysis")
    output_proj_dir = os.path.join("outputs", "projections")
    out_xlsx = os.path.join(output_ev_dir, "ev_bets_ranked.xlsx")
    out_csv = os.path.join(output_ev_dir, "ev_bets_ranked.csv")
    
    # 0. Update MoneyPuck and Rebuild Features (Live Bridge)
    if use_live_base:
        print("--- Starting Live Base Projection Build ---")
        
        # A. Download latest MoneyPuck data
        downloader = os.path.join(backtest_pipeline_dir, "download_moneypuck_team_player_gbg.py")
        run_step("Download MoneyPuck", [sys.executable, downloader, "--end-season", "2025"], env)
        
        # B. Ingest to DuckDB
        ingestor = os.path.join(backtest_pipeline_dir, "ingest_moneypuck_to_duckdb.py")
        run_step("Ingest DuckDB", [sys.executable, ingestor, "--end-season", "2025"], env)
        
        # C. Rebuild Features
        for feature_script in ["build_player_features.py", "build_team_defense_features.py", "build_goalie_features.py"]:
            script_path = os.path.join(backtest_pipeline_dir, feature_script)
            run_step(f"Rebuild {feature_script}", [sys.executable, script_path, "--force"], env)
            
        # D. Produce Base Projections File
        producer = os.path.join(proj_dir, "produce_live_base_projections.py")
        run_step("Produce Base Projections", [sys.executable, producer], env)
        
        print("--- Finished Live Base Projection Build ---\n")

    # 1. Scraper
    print(f"--- Starting Scraper ---")
    real_scraper_script = os.path.join(scrapers_dir, "scrape_playnow_api.py")
    scraper_output = "nhl_player_props.csv" # The scraper outputs to CWD
    
    if use_api:
        run_step("API Scraper", [sys.executable, real_scraper_script], env)
    else:
        # Legacy fallback
        selenium_script = os.path.join(scrapers_dir, "nhl_props_scraper.py")
        run_step("Selenium Scraper", [sys.executable, selenium_script], env)
        
    if os.path.exists(scraper_output):
        print(f"Moving {scraper_output} to {props_path}...")
        if os.path.exists(props_path):
            os.remove(props_path)
        os.rename(scraper_output, props_path)
    else:
        print(f"Warning: {scraper_output} not found. Checking if {props_path} exists.")
        if not os.path.exists(props_path):
            print("Error: No props file found after scraping.")
            sys.exit(1)

    # 1.5 Build Game Context
    print(f"--- Building Game Context ---")
    context_script = os.path.join(proj_dir, "produce_game_context.py")
    if os.path.exists(context_script):
        run_step("Game Context", [sys.executable, context_script], env)
    else:
        print(f"Warning: Context script not found at {context_script}")
    print(f"--- Finished Game Context ---\n")

    # 2. Generate Projections
    print(f"--- Generating Projections ---")
    
    # Extract date from props file for projection script
    game_date = None
    try:
        import pandas as pd
        if os.path.exists(props_path):
            df_props = pd.read_csv(props_path)
            if 'Game_Date' in df_props.columns and not df_props.empty:
                dates = df_props['Game_Date'].dropna().unique()
                if len(dates) > 0:
                    game_date = dates[0]
                    print(f"Detected Game Date: {game_date}")
    except Exception as e:
        print(f"Warning: Could not extract date from props: {e}")

    proj_script = os.path.join(proj_dir, "single_game_probs.py")
    cmd_proj = [sys.executable, proj_script]
    if game_date:
        cmd_proj.extend(["--date", str(game_date)])
    
    run_step("Projections", cmd_proj, env)
    
    # Output of projections is expected in outputs/projections/SingleGamePropProbabilities.csv
    # but single_game_probs.py might still write to its own dir if not updated.
    # We'll check both.
    probs_output = os.path.join(output_proj_dir, "SingleGamePropProbabilities.csv")
    if not os.path.exists(probs_output):
        legacy_probs = os.path.join(proj_dir, "SingleGamePropProbabilities.csv")
        if os.path.exists(legacy_probs):
            print(f"Moving {legacy_probs} to {probs_output}")
            os.makedirs(output_proj_dir, exist_ok=True)
            if os.path.exists(probs_output): os.remove(probs_output)
            os.rename(legacy_probs, probs_output)
        else:
            # Check CWD
            if os.path.exists("SingleGamePropProbabilities.csv"):
                 os.rename("SingleGamePropProbabilities.csv", probs_output)

    print(f"--- Finished Projections ---\n")

    # 2.5 Accuracy Backtest (Optional)
    if run_accuracy_backtest:
        print("--- Starting Accuracy Backtest ---")
        snapshot_script = os.path.join(backtest_pipeline_dir, "build_probability_snapshots.py")
        run_step("Verify Snapshots", [sys.executable, snapshot_script], env)

        accuracy_script = os.path.join(backtest_pipeline_dir, "evaluate_forecast_accuracy.py")
        run_step("Evaluate Accuracy", [sys.executable, accuracy_script], env)
        print("--- Finished Accuracy Backtest ---\n")

    # 3. Run EV Analysis
    print(f"--- Running EV Analysis ---")
    runner_script = os.path.join(analysis_dir, "runner.py")
    base_proj_path = os.path.join(output_proj_dir, "BaseSingleGameProjections.csv")
    
    cmd_ev = [
        sys.executable, runner_script,
        "--base", base_proj_path,
        "--props", props_path,
        "--probs", probs_output,
        "--out_xlsx", out_xlsx,
        "--out_csv", out_csv
    ]
    
    if os.environ.get("DISABLE_CALIBRATION") == "1":
        print("!!! CALIBRATION DISABLED BY ENVIRONMENT VARIABLE !!!")
        
    run_step("EV Analysis", cmd_ev, env)
    
    print(f"Workflow Complete. Results: {out_xlsx}")

if __name__ == "__main__":
    main()

import os
import subprocess
import sys
from datetime import datetime, timezone

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
    use_live_base = os.environ.get("USE_LIVE_BASE_PROJECTIONS", "1") == "1"
    run_accuracy_backtest = os.environ.get("RUN_ACCURACY_BACKTEST", "0") == "1"
    run_odds_ingestion = os.environ.get("RUN_ODDS_INGESTION", "1") == "1"
    
    # Paths to Scripts
    proj_dir = os.path.join("src", "nhl_bets", "projections")
    analysis_dir = os.path.join("src", "nhl_bets", "analysis")
    backtest_pipeline_dir = os.path.join("pipelines", "backtesting")
    scripts_dir = os.path.join("scripts")
    
    # Output Paths
    output_ev_dir = os.path.join("outputs", "ev_analysis")
    output_proj_dir = os.path.join("outputs", "projections")
    
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

    # 1. Odds Ingestion (Phase 11 Unified Scraper)
    if run_odds_ingestion:
        print("--- Starting Phase 11 Odds Ingestion ---")
        ingest_script = os.path.join(backtest_pipeline_dir, "ingest_odds_to_duckdb.py")
        run_step("Odds Ingestion (DuckDB)", [sys.executable, ingest_script], env)
    else:
        print("--- Skipping Odds Ingestion (RUN_ODDS_INGESTION=0) ---\n")

    # 2. Build Game Context
    print("--- Building Game Context ---")
    context_script = os.path.join(proj_dir, "produce_game_context.py")
    run_step("Game Context", [sys.executable, context_script], env)

    # 3. Generate Projections
    print("--- Generating Projections ---")
    proj_script = os.path.join(proj_dir, "single_game_probs.py")
    run_step("Projections", [sys.executable, proj_script], env)
    
    # 4. Multi-Book EV Analysis
    print("--- Running Multi-Book EV Analysis ---")
    runner_script = os.path.join(analysis_dir, "runner_duckdb.py")
    run_step("Multi-Book EV Analysis", [sys.executable, runner_script], env)

    # 5. Audit & Forensic Walkthrough
    print("--- Running Model Audit ---")
    audit_script = os.path.join(scripts_dir, "analysis", "audit_model_prob.py")
    if os.path.exists(audit_script):
        run_step("Model Audit", [sys.executable, audit_script], env)
    
    # 6. Generate Best Bets Report
    print("--- Generating Best Bets Report ---")
    best_bets_script = os.path.join(scripts_dir, "generate_best_bets.py")
    run_step("Best Bets", [sys.executable, best_bets_script], env)

    # 7. Accuracy Backtest (Optional)
    if run_accuracy_backtest:
        print("--- Starting Accuracy Backtest ---")
        snapshot_script = os.path.join(backtest_pipeline_dir, "build_probability_snapshots.py")
        run_step("Verify Snapshots", [sys.executable, snapshot_script], env)

        accuracy_script = os.path.join(backtest_pipeline_dir, "evaluate_forecast_accuracy.py")
        run_step("Evaluate Accuracy", [sys.executable, accuracy_script], env)
        print("--- Finished Accuracy Backtest ---\n")

    print(f"\nWorkflow Complete.")
    print(f"Primary Report: {os.path.join(output_ev_dir, 'MultiBookBestBets.xlsx')}")
    print(f"Filtered Candidates: {os.path.join(output_ev_dir, 'BestCandidatesFiltered.xlsx')}")

if __name__ == "__main__":
    main()
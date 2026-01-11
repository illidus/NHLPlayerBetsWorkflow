import argparse
import sys
import os
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd

# Add repo root to path
sys.path.append(os.getcwd())

from src.nhl_bets.ingestion.providers.the_odds_api import TheOddsApiProvider
from src.nhl_bets.ingestion.schema import OddsSchemaManager

def parse_args():
    parser = argparse.ArgumentParser(description="Phase 12: Historical Odds Ingestion")
    parser.add_argument("--provider", type=str, default="THE_ODDS_API", choices=["THE_ODDS_API"], help="Provider to ingest from")
    parser.add_argument("--start_date", type=str, help="YYYY-MM-DD")
    parser.add_argument("--end_date", type=str, help="YYYY-MM-DD")
    parser.add_argument("--real_sample_date", type=str, help="YYYY-MM-DD (Shortcut for single day real sample)")
    parser.add_argument("--league", type=str, default="NHL", help="League key")
    parser.add_argument("--mock", action="store_true", help="Use mock data (no API cost)")
    parser.add_argument("--dry_run", action="store_true", help="Do not write to DuckDB (raw files still saved)")
    parser.add_argument("--db_path", type=str, default="data/db/nhl_backtest.duckdb")
    parser.add_argument("--limit_events", type=int, help="Limit number of events to process (default 3 for real mode)")
    parser.add_argument("--print_plan", action="store_true", help="Show estimated calls and events before executing")
    parser.add_argument("--yes", action="store_true", help="Confirm execution for non-mock runs")
    
    # Phase 12 Additions
    parser.add_argument("--mode", type=str, default="sportsbook", choices=["sportsbook", "dfs"], help="Ingestion mode")
    parser.add_argument("--regions", type=str, default="us", help="Odds API regions (e.g. us, us_dfs, eu)")
    parser.add_argument("--diagnose_vendor", action="store_true", help="Run diagnostic suite and exit")
    
    return parser.parse_args()

def main():
    args = parse_args()
    run_ts = datetime.now(timezone.utc)
    run_ts_str = run_ts.strftime("%Y%m%d_%H%M%S")
    
    # Init Provider (needed for diagnostics too)
    effective_limit = args.limit_events if args.limit_events is not None else (3 if not args.mock else 999)
    if args.provider == "THE_ODDS_API":
        provider = TheOddsApiProvider(mock_mode=args.mock, mode=args.mode, regions=args.regions, limit_events=effective_limit)
    else:
        print(f"Unknown provider: {args.provider}")
        sys.exit(1)

    if args.diagnose_vendor:
        provider.diagnose_vendor()
        return

    if args.real_sample_date:
        start_dt = datetime.strptime(args.real_sample_date, "%Y-%m-%d")
        end_dt = start_dt
    elif args.start_date and args.end_date:
        start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")
    else:
        print("Error: Must provide --real_sample_date OR --start_date and --end_date")
        sys.exit(1)

    # Real-run safety
    if not args.mock and not args.yes:
        print("SAFETY ERROR: Non-mock run requires --yes to proceed.")
        sys.exit(1)
    
    if args.print_plan:
        print("--- Execution Plan ---")
        print(f"Provider: {args.provider}")
        print(f"Mode: {args.mode}")
        print(f"Regions: {args.regions}")
        print(f"Date Range: {start_dt.date()} to {end_dt.date()}")
        print(f"Mock Mode: {args.mock}")
        print(f"Event Limit: {effective_limit}")
        print(f"DB Path: {args.db_path}")
        if not args.yes and not args.mock:
            print("Status: WILL FAIL (requires --yes)")
        else:
            print("Status: Ready to execute")
        return

    print(f"--- Phase 12 Ingestion: {args.provider} ---")
    print(f"Range: {start_dt.date()} to {end_dt.date()}")
    print(f"Mode: {args.mode}, Regions: {args.regions}, Mock: {args.mock}, Dry Run: {args.dry_run}, Limit: {effective_limit}")
    
    # 1. Setup Schema
    schema_mgr = OddsSchemaManager(args.db_path)
    if not args.dry_run:
        schema_mgr.ensure_schema()
        print(f"Schema checked/created at {args.db_path}")

    # 2. Ingest
    try:
        roi_df, unresolved_df = provider.ingest_date_range(start_dt, end_dt, league=args.league)
    except Exception as e:
        print(f"Ingestion failed: {e}")
        sys.exit(1)

    # 3. Handle DFS vs Sportsbook data
    dfs_df = pd.DataFrame()
    if not roi_df.empty:
        if 'is_dfs' in roi_df.columns:
            dfs_df = roi_df[roi_df['is_dfs'] == True].copy()
            roi_df = roi_df[roi_df['is_dfs'] == False].copy()

    total_rows = len(roi_df) + len(unresolved_df) + len(dfs_df)
    print(f"Fetched {total_rows} total rows.")
    print(f"  Sportsbook ROI Grade: {len(roi_df)}")
    print(f"  DFS Props: {len(dfs_df)}")
    print(f"  Unresolved: {len(unresolved_df)}")

    # 4. Insert (if not dry run)
    if not args.dry_run:
        try:
            if not roi_df.empty:
                schema_mgr.insert_idempotent(roi_df, table_name="fact_prop_odds")
                print("Successfully inserted Sportsbook ROI rows into fact_prop_odds.")
            
            if not dfs_df.empty:
                schema_mgr.insert_idempotent(dfs_df, table_name="fact_dfs_props")
                print("Successfully inserted DFS rows into fact_dfs_props.")
            
            if not unresolved_df.empty:
                schema_mgr.insert_idempotent(unresolved_df, table_name="stg_prop_odds_unresolved", 
                                            key_cols=["source_vendor", "capture_ts_utc", "event_id_vendor", "player_name_raw", "market_type", "book_id_vendor"])
                print("Successfully inserted Unresolved rows into stg_prop_odds_unresolved.")
                
        except Exception as e:
            print(f"Database insertion failed: {e}")
            sys.exit(1)
    else:
        print("Dry run: Skipping DB insert.")

    # 5. Generate Audit Reports
    report_dir = Path("outputs/phase12_odds_api") / run_ts_str
    report_dir.mkdir(parents=True, exist_ok=True)
    
    # Update LATEST.txt
    with open(Path("outputs/phase12_odds_api/LATEST.txt"), "w") as f:
        f.write(run_ts_str)

    # A) Run Summary
    summary = {
        "run_ts": run_ts.isoformat(),
        "provider": args.provider,
        "range_start": start_dt.isoformat(),
        "range_end": end_dt.isoformat(),
        "total_rows": total_rows,
        "roi_count": len(roi_df),
        "unresolved_count": len(unresolved_df),
        "requests_made": len(provider.request_log),
        "est_cost_units": sum(r.get('cost_est', 0) for r in provider.request_log),
        "mock_mode": args.mock,
        "dry_run": args.dry_run,
        "event_limit": effective_limit
    }
    with open(report_dir / "run_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    # B) Quota Audit
    with open(report_dir / "audit_quota_burn.md", "w") as f:
        f.write("# Quota Usage Audit\n\n")
        if args.mock:
            f.write("**Status:** Mock Mode (No HTTP requests made to vendor).\n")
        else:
            f.write("| Timestamp | Endpoint | Status | Est Cost | Quota Remaining |\n")
            f.write("|---|---|---|---|---|\n")
            for log in provider.request_log:
                f.write(f"| {log['ts']} | {log['endpoint']} | {log['status']} | {log['cost_est']} | {log['quota_remaining']} |\n")

    # C) Join Confidence Audit
    if not roi_df.empty:
        with open(report_dir / "audit_join_confidence.md", "w") as f:
            f.write("# Join Confidence Audit\n\n")
            f.write("## Distribution\n")
            f.write(roi_df[['join_conf_event', 'join_conf_player', 'join_conf_market']].describe().to_markdown())
            
            # Additional metric: % at join_conf_player == 1.0
            p100 = (roi_df['join_conf_player'] == 1.0).sum() / len(roi_df) * 100
            f.write(f"\n\n**Player ID Coverage (conf==1.0):** {p100:.2f}%\n")

            f.write("\n\n## Unresolved Reasons\n")
            if not unresolved_df.empty:
                f.write(unresolved_df['failure_reasons'].value_counts().to_markdown())
            else:
                f.write("No unresolved rows.")

    # D) Market & Book Coverage
    if not roi_df.empty:
        with open(report_dir / "audit_market_coverage.md", "w") as f:
            f.write("# Market Coverage\n")
            f.write(roi_df['market_type'].value_counts().reset_index().to_markdown())
        
        with open(report_dir / "audit_book_coverage.md", "w") as f:
            f.write("# Book Coverage\n")
            f.write(roi_df['book_name_raw'].value_counts().reset_index().to_markdown())
            
    # E) Ingestion Audit Log
    main_log_path = Path("outputs/phase12_odds_api/ingestion_audit.md")
    log_entry = f"\n## Ingestion Run: {run_ts.isoformat()}\n"
    log_entry += f"- **Provider:** {args.provider} | **Mock:** {args.mock} | **Limit:** {effective_limit}\n"
    log_entry += f"- **Rows:** {total_rows} (ROI: {len(roi_df)}, Unresolved: {len(unresolved_df)})\n"
    log_entry += f"- **Artifacts:** [{run_ts_str}/]({run_ts_str}/run_summary.json)\n"

    with open(main_log_path, "a") as f:
        f.write(log_entry)

    print(f"Audit reports generated in {report_dir}")

if __name__ == "__main__":
    main()
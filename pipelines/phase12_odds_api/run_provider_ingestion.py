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
from src.nhl_bets.identity.player_resolver import PlayerResolver

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
    parser.add_argument("--allow_unrostered_resolution", action="store_true", help="Allow fallback to all players (Exploration Only)")
    parser.add_argument("--prove_identity", action="store_true", help="Calculate and print identity proof metrics")
    
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
        print(f"Strict Identity: {not args.allow_unrostered_resolution}")
        if not args.yes and not args.mock:
            print("Status: WILL FAIL (requires --yes)")
        else:
            print("Status: Ready to execute")
        return

    print(f"--- Phase 12 Ingestion: {args.provider} ---")
    print(f"Range: {start_dt.date()} to {end_dt.date()}")
    print(f"Mode: {args.mode}, Regions: {args.regions}, Mock: {args.mock}, Dry Run: {args.dry_run}, Limit: {effective_limit}")
    print(f"Strict Identity: {not args.allow_unrostered_resolution}")
    
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

    # --- Phase 13: Player Resolution ---
    # Metrics containers
    identity_metrics = {
        "total_attempted": 0,
        "resolved_roi": 0,
        "roster_snapshot_hits": 0,
        "missing_roster": 0,
        "ambiguous_collisions": 0
    }

    if not roi_df.empty:
        print("Running Phase 13 Player Identity Resolution...")
        resolver = PlayerResolver(args.db_path, allow_unrostered_resolution=args.allow_unrostered_resolution)
        
        # Add resolution columns to dataframe if not present
        roi_df['player_id_canonical'] = None
        roi_df['player_resolve_method'] = None
        roi_df['player_resolve_conf'] = 0.0
        roi_df['player_resolve_notes'] = None
        
        resolved_indices = []
        resolution_failed_indices = []
        
        identity_metrics["total_attempted"] = len(roi_df)
        
        # Iterate and Resolve
        for idx, row in roi_df.iterrows():
            # If player_name_raw is missing, skip (should be caught by is_roi_grade)
            if not row.get('player_name_raw'):
                resolution_failed_indices.append(idx)
                continue

            pid, method, conf, notes = resolver.resolve(
                player_name_raw=row['player_name_raw'],
                event_id_vendor=row.get('event_id_vendor'),
                game_start_ts=row.get('event_start_ts_utc'),
                home_team_raw=row.get('home_team_raw', ''),
                away_team_raw=row.get('away_team_raw', '')
            )
            
            roi_df.at[idx, 'player_id_canonical'] = pid
            roi_df.at[idx, 'player_resolve_method'] = method
            roi_df.at[idx, 'player_resolve_conf'] = conf
            roi_df.at[idx, 'player_resolve_notes'] = notes
            
            # Metrics
            if notes == "MISSING_ROSTER_SNAPSHOT":
                identity_metrics["missing_roster"] += 1
            else:
                identity_metrics["roster_snapshot_hits"] += 1 # Rough proxy: if we didn't fail on snapshot, we hit one (or are in permissive mode)
            
            if "Ambiguous" in notes:
                identity_metrics["ambiguous_collisions"] += 1

            # ROI Grade Gating for Resolution: Conf >= 0.90
            if conf >= 0.90:
                resolved_indices.append(idx)
                identity_metrics["resolved_roi"] += 1
            else:
                resolution_failed_indices.append(idx)
                # Enqueue for manual review
                resolver.enqueue_unresolved(
                    row.to_dict(), 
                    failure_reason=f"Low Conf: {conf:.2f} ({method}) - {notes}"
                )

        # Split DataFrames
        failed_res_df = roi_df.loc[resolution_failed_indices].copy()
        roi_df = roi_df.loc[resolved_indices].copy()
        
        # Move failed rows to unresolved_df
        if not failed_res_df.empty:
            print(f"  {len(failed_res_df)} rows failed resolution (conf < 0.90). Moved to unresolved.")
            
            new_failures = []
            for _, row in failed_res_df.iterrows():
                # Map to stg_prop_odds_unresolved schema
                new_failures.append({
                    "source_vendor": row.get("source_vendor"),
                    "capture_ts_utc": row.get("capture_ts_utc"),
                    "ingested_at_utc": row.get("ingested_at_utc"),
                    "event_id_vendor": row.get("event_id_vendor"),
                    "player_name_raw": row.get("player_name_raw"),
                    "market_type": row.get("market_type"),
                    "line": row.get("line"),
                    "side": row.get("side"),
                    "book_id_vendor": row.get("book_id_vendor"),
                    "odds_american": row.get("odds_american"),
                    "raw_payload_path": row.get("raw_payload_path"),
                    "raw_payload_hash": row.get("raw_payload_hash"),
                    "failure_reasons": json.dumps([f"PLAYER_RESOLUTION_LOW_CONF: {row.get('player_resolve_conf', 0.0):.2f} - {row.get('player_resolve_method')} - {row.get('player_resolve_notes')}"]),
                    "raw_row_json": json.dumps(row.to_dict(), default=str),
                    "is_dfs": row.get("is_dfs", False)
                })
            
            if new_failures:
                unresolved_df = pd.concat([unresolved_df, pd.DataFrame(new_failures)], ignore_index=True)

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

    # F) Phase 13 Audits (Mandatory)
    print("Generating Phase 13 Identity Resolution Audits...")
    
    # audit_player_resolution.md
    with open(report_dir / "audit_player_resolution.md", "w") as f:
        f.write(f"# Player Identity Resolution Audit\n\n")
        f.write(f"**Run TS:** {run_ts.isoformat()}\n")
        f.write(f"**Strict Roster Mode:** {not args.allow_unrostered_resolution}\n\n")
        
        f.write("## Summary Metrics\n")
        f.write(f"- Total Attempted: {identity_metrics['total_attempted']}\n")
        f.write(f"- Resolved ROI-Grade: {identity_metrics['resolved_roi']} ({(identity_metrics['resolved_roi']/max(1,identity_metrics['total_attempted'])*100):.1f}%)\n")
        f.write(f"- Roster Snapshot Hits: {identity_metrics['roster_snapshot_hits']}\n")
        f.write(f"- Missing Roster Failures: {identity_metrics['missing_roster']}\n")
        f.write(f"- Ambiguous Collisions: {identity_metrics['ambiguous_collisions']}\n\n")
        
        f.write("## Confidence Histogram\n")
        if not roi_df.empty:
            f.write(roi_df['player_resolve_conf'].value_counts(bins=10, sort=False).to_markdown())
        elif identity_metrics['total_attempted'] > 0:
            f.write("All rows failed resolution.\n")
        else:
            f.write("No rows processed.\n")

    # audit_unresolved_reasons.md
    with open(report_dir / "audit_unresolved_reasons.md", "w") as f:
        f.write(f"# Unresolved Reasons Breakdown\n\n")
        if not unresolved_df.empty:
             # Explode reasons if list, or just count
             # Since we store as JSON list string, we might need to parse, but for now value_counts of the raw string is okay or slight cleanup
             f.write("## Top Failure Reasons\n")
             f.write(unresolved_df['failure_reasons'].value_counts().head(20).to_markdown())
             
             f.write("\n\n## Top Unresolved Player Names\n")
             f.write(unresolved_df['player_name_raw'].value_counts().head(20).to_markdown())
             
             f.write("\n\n## Failures by Market\n")
             f.write(unresolved_df['market_type'].value_counts().to_markdown())
             
             f.write("\n\n## Failures by Book\n")
             f.write(unresolved_df['book_id_vendor'].value_counts().to_markdown())
        else:
            f.write("No unresolved rows.")

    # G) Proof Identity Mode
    if args.prove_identity:
        proof_path = report_dir / "identity_proof.json"
        
        # Calculate rates
        total = identity_metrics['total_attempted']
        resolved_rate = identity_metrics['resolved_roi'] / max(1, total)
        roster_hit_rate = identity_metrics['roster_snapshot_hits'] / max(1, total)
        
        proof_data = {
            "run_ts": run_ts.isoformat(),
            "strict_mode": not args.allow_unrostered_resolution,
            "total_events_processed": effective_limit, # Proxy
            "total_rows_attempted": total,
            "resolved_rate": resolved_rate,
            "roster_hit_rate": roster_hit_rate,
            "ambiguous_collisions": identity_metrics['ambiguous_collisions'],
            "top_unresolved_names": unresolved_df['player_name_raw'].value_counts().head(5).to_dict() if not unresolved_df.empty else {}
        }
        
        with open(proof_path, "w") as f:
            json.dump(proof_data, f, indent=2)
            
        print("\n--- IDENTITY PROOF METRICS ---")
        print(f"Resolved Rate: {resolved_rate:.2%}")
        print(f"Roster Hit Rate: {roster_hit_rate:.2%}")
        print(f"Ambiguous Collisions: {identity_metrics['ambiguous_collisions']}")
        print(f"Proof written to: {proof_path}")

    print(f"Audit reports generated in {report_dir}")

if __name__ == "__main__":
    main()
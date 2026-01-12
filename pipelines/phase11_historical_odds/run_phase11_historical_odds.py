import os
import sys
import argparse
import json
import duckdb
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from collections import Counter

# Add src to path
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(os.path.dirname(current_dir))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

try:
    from src.nhl_bets.odds_historical.normalize_phase11 import normalize_batch
    from src.nhl_bets.odds_historical.match_phase11_to_games import match_phase11_rows
except ImportError as e:
    print(f"Import Error: {e}")
    sys.exit(1)

DB_PATH = os.path.join(repo_root, 'data', 'db', 'nhl_backtest.duckdb')
OUTPUT_DIR = os.path.join(repo_root, 'outputs', 'odds_archive_audit')
RUNS_DIR = os.path.join(repo_root, 'outputs', 'runs')
EVAL_DIR = os.path.join(repo_root, 'outputs', 'eval')

# DDL for Phase 11 Table (Updated with join keys)
DDL_PHASE11 = """
CREATE TABLE IF NOT EXISTS fact_odds_historical_phase11 (
    row_id VARCHAR PRIMARY KEY,
    source_vendor VARCHAR,
    capture_ts_utc TIMESTAMP,
    event_id_vendor VARCHAR,
    event_start_ts_utc TIMESTAMP,
    player_name_raw VARCHAR,
    market_type VARCHAR,
    line DOUBLE,
    side VARCHAR,
    book_id_vendor VARCHAR,
    odds_american INTEGER,
    odds_decimal DOUBLE,
    ingested_at_utc TIMESTAMP,
    game_date DATE,
    home_team_raw VARCHAR,
    away_team_raw VARCHAR,
    home_team_norm VARCHAR,
    away_team_norm VARCHAR,
    match_key VARCHAR,
    home_team_code VARCHAR,
    away_team_code VARCHAR,
    match_key_code VARCHAR
);
"""

# Column order matching DDL
DDL_COLUMNS = [
    'row_id',
    'source_vendor',
    'capture_ts_utc',
    'event_id_vendor',
    'event_start_ts_utc',
    'player_name_raw',
    'market_type',
    'line',
    'side',
    'book_id_vendor',
    'odds_american',
    'odds_decimal',
    'ingested_at_utc',
    'game_date',
    'home_team_raw',
    'away_team_raw',
    'home_team_norm',
    'away_team_norm',
    'match_key',
    'home_team_code',
    'away_team_code',
    'match_key_code'
]

def setup_db(con):
    con.execute(DDL_PHASE11)
    
    # Migration: Check if new columns exist, add if not
    existing_cols = [c[1] for c in con.execute("PRAGMA table_info('fact_odds_historical_phase11')").fetchall()]
    new_cols = {
        'game_date': 'DATE',
        'home_team_raw': 'VARCHAR',
        'away_team_raw': 'VARCHAR',
        'home_team_norm': 'VARCHAR',
        'away_team_norm': 'VARCHAR',
        'match_key': 'VARCHAR',
        'home_team_code': 'VARCHAR',
        'away_team_code': 'VARCHAR',
        'match_key_code': 'VARCHAR'
    }
    
    for col, dtype in new_cols.items():
        if col not in existing_cols:
            print(f"Migrating: Adding {col} to fact_odds_historical_phase11")
            try:
                con.execute(f"ALTER TABLE fact_odds_historical_phase11 ADD COLUMN {col} {dtype}")
            except Exception as e:
                print(f"Migration warning: {e}")

def main():
    parser = argparse.ArgumentParser(description="Phase 11: Historical Odds Ingestion")
    parser.add_argument("--fixture", help="Path to JSON fixture file")
    parser.add_argument("--date_from", help="Live fetch start date (NotImplemented)", default=None)
    parser.add_argument("--date_to", help="Live fetch end date (NotImplemented)", default=None)
    parser.add_argument("--match_to_games", action="store_true", help="Attempt to match odds to games in DB")
    args = parser.parse_args()

    start_time = datetime.now(timezone.utc)
    ts_str = start_time.strftime("%Y%m%d_%H%M%S")
    
    # Get Git SHA
    try:
        git_sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    except:
        git_sha = "unknown"

    # 1. Input Source
    data = []
    input_mode = "unknown"
    
    if args.fixture:
        input_mode = "fixture"
        p = Path(args.fixture)
        if not p.exists():
            print(f"Error: Fixture {p} not found.")
            sys.exit(1)
        with open(p, 'r') as f:
            data = json.load(f)
        print(f"Loaded fixture: {len(data)} items (if list)")
    elif args.date_from:
        print("Live fetch not implemented yet.")
        sys.exit(1)
    else:
        print("Error: Must provide --fixture or dates.")
        sys.exit(1)

    # 2. Normalize
    rows = normalize_batch(data, capture_ts=start_time.isoformat())
    print(f"Normalized {len(rows)} odds rows.")

    # 3. Write to DB
    con = duckdb.connect(DB_PATH)
    setup_db(con)
    
    inserted_count = 0
    rejected_count = 0 
    
    if rows:
        import pandas as pd
        df = pd.DataFrame(rows)
        
        for col in DDL_COLUMNS:
            if col not in df.columns:
                df[col] = None
        
        df = df[DDL_COLUMNS]
        
        con.register('df_stage', df)
        
        # Insert avoiding duplicates
        result = con.execute("""
            INSERT INTO fact_odds_historical_phase11
            SELECT * FROM df_stage
            WHERE row_id NOT IN (SELECT row_id FROM fact_odds_historical_phase11)
        ")
        
        try:
            inserted_count = result.fetchall()[0][0]
        except Exception:
            inserted_count = len(rows) 
            
        print(f"Inserted {inserted_count} new rows.")
    
    # 4. Optional Game Matching
    matching_metrics = {}
    if args.match_to_games:
        print("Running game matching...")
        matching_metrics = match_phase11_rows(con, "fact_odds_historical_phase11")
        print(f"Game Matching: {matching_metrics.get('status')} - Rate: {matching_metrics.get('match_rate', 0):.1%}")
    
    con.close()

    # 5. Reporting & Metrics
    resolved_both_count = sum(1 for r in rows if r.get('home_team_code') and r.get('away_team_code'))
    resolution_rate = resolved_both_count / len(rows) if rows else 0.0
    
    unresolved_teams = []
    for r in rows:
        if not r.get('home_team_code') and r.get('home_team_raw'):
            unresolved_teams.append(r['home_team_raw'])
        if not r.get('away_team_code') and r.get('away_team_raw'):
            unresolved_teams.append(r['away_team_raw'])
            
unresolved_counts = Counter(unresolved_teams)
    
    # Run Manifest
    manifest = {
        "timestamp": start_time.isoformat(),
        "git_sha": git_sha,
        "run_id": f"phase11_{ts_str}",
        "pipeline_name": "phase11_historical_odds",
        "input_mode": input_mode,
        "input_source": args.fixture,
        "counts": {
            "raw_items": len(data) if isinstance(data, list) else 1,
            "normalized_rows": len(rows),
            "inserted_rows": inserted_count,
            "rejected_rows": rejected_count
        },
        "metrics": {
            "team_code_resolution_rate": resolution_rate,
            "unresolved_team_count": len(unresolved_counts)
        },
        "output_table": "fact_odds_historical_phase11",
        "output_files": []
    }
    
    os.makedirs(RUNS_DIR, exist_ok=True)
    man_path = os.path.join(RUNS_DIR, f"run_manifest_{ts_str}.json")
    with open(man_path, 'w') as f:
        json.dump(manifest, f, indent=4)
        
    # Coverage Report
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    report_path = os.path.join(OUTPUT_DIR, f"phase11_coverage_{ts_str}.md")
    with open(report_path, 'w') as f:
        f.write(f"# Phase 11 Coverage Report\n\n")
        f.write(f"**Run ID:** {ts_str}\n")
        f.write(f"**Mode:** {input_mode}\n\n")
        f.write(f"## Statistics\n")
        f.write(f"- Input Items: {manifest['counts']['raw_items']}\n")
        f.write(f"- Normalized Rows: {len(rows)}\n")
        f.write(f"- DB Inserted: {inserted_count}\n")
        f.write(f"- Team Code Resolution Rate: {resolution_rate:.1%}\n\n")
        
        if matching_metrics:
            f.write(f"## Game Matching (Experimental)\n")
            f.write(f"- Status: {matching_metrics.get('status')}\n")
            f.write(f"- Table Used: {matching_metrics.get('game_table_selected')}\n")
            f.write(f"- Match Rate: {matching_metrics.get('match_rate', 0):.1%}\n")
            f.write(f"- Unmatched Reasons:\n")
            for r, c in matching_metrics.get('unmatched_reasons', {}).items():
                f.write(f"  - {r}: {c}\n")
            f.write(f"- Notes: {matching_metrics.get('notes')}\n\n")
        
        f.write(f"## Unresolved Teams (Top 10)\n")
        if unresolved_counts:
            for team, count in unresolved_counts.most_common(10):
                f.write(f"- {team}: {count}\n")
        else:
            f.write("None (All teams resolved)\n")
        
    # Eval Manifest
    eval_manifest = {
        "timestamp": start_time.isoformat(),
        "git_sha": git_sha,
        "pipeline_name": "phase11_historical_odds",
        "eval_type": "coverage",
        "coverage_summary": {
            "total_rows": len(rows),
            "inserted": inserted_count,
            "resolution_rate": resolution_rate,
            "game_matching": matching_metrics
        },
        "links": {
            "run_manifest": man_path,
            "coverage_report": report_path
        }
    }
    
    os.makedirs(EVAL_DIR, exist_ok=True)
    eval_man_path = os.path.join(EVAL_DIR, f"eval_manifest_{ts_str}.json")
    with open(eval_man_path, 'w') as f:
        json.dump(eval_manifest, f, indent=4)
        
    print(f"Run Manifest: {man_path}")
    print(f"Eval Manifest: {eval_man_path}")
    print(f"Report: {report_path}")

if __name__ == "__main__":
    main()

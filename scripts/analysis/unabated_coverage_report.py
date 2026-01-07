import duckdb
import os
import pandas as pd
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

def generate_unabated_coverage_report(db_path: str, output_dir: str):
    con = duckdb.connect(db_path)
    
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    report_path = os.path.join(output_dir, f"unabated_mapping_coverage_{timestamp}.md")
    
    # 1. Total Unabated Odds Rows
    total_unabated = con.execute("SELECT count(*) FROM fact_prop_odds WHERE source_vendor = 'UNABATED'").fetchone()[0]
    
    if total_unabated == 0:
        report_content = "# Unabated Mapping Coverage Report\n\nNo UNABATED records found in fact_prop_odds."
    else:
        # 2. Rows with vendor_event_id
        with_event_id = con.execute("SELECT count(*) FROM fact_prop_odds WHERE source_vendor = 'UNABATED' AND vendor_event_id IS NOT NULL").fetchone()[0]
        
        # 3. Rows with event_start_time_utc resolved
        with_start_time = con.execute("SELECT count(*) FROM fact_prop_odds WHERE source_vendor = 'UNABATED' AND event_start_time_utc IS NOT NULL").fetchone()[0]
        
        # 4. Rows with player mapping resolved (vendor_person_id)
        with_person_id = con.execute("SELECT count(*) FROM fact_prop_odds WHERE source_vendor = 'UNABATED' AND vendor_person_id IS NOT NULL").fetchone()[0]

        # 5. Rows with home/away teams resolved
        with_matchup = con.execute("""
            SELECT count(*) FROM fact_prop_odds
            WHERE source_vendor = 'UNABATED'
              AND home_team IS NOT NULL
              AND away_team IS NOT NULL
        """).fetchone()[0]

        # 6. Rows with player team resolved via metadata
        with_player_team = con.execute("""
            SELECT count(*) FROM fact_prop_odds o
            JOIN dim_players_unabated p
              ON o.vendor_person_id = p.vendor_person_id
            WHERE o.source_vendor = 'UNABATED'
              AND p.team_abbr IS NOT NULL
        """).fetchone()[0]
        
        # 5. Top 10 unresolved samples
        unresolved_samples = con.execute("""
            SELECT player_name_raw, event_name_raw, market_type, line, side
            FROM fact_prop_odds 
            WHERE source_vendor = 'UNABATED' 
              AND (vendor_event_id IS NULL OR vendor_person_id IS NULL)
            LIMIT 10
        """).df()
        
        coverage_event = (with_event_id / total_unabated) * 100
        coverage_time = (with_start_time / total_unabated) * 100
        coverage_person = (with_person_id / total_unabated) * 100
        coverage_matchup = (with_matchup / total_unabated) * 100
        coverage_player_team = (with_player_team / total_unabated) * 100
        
        report_content = f"""# Unabated Mapping Coverage Report
**Generated at:** {datetime.now(timezone.utc).isoformat()}
**Total Unabated Rows:** {total_unabated}

## Coverage Summary
- **Rows with vendor_event_id:** {with_event_id} ({coverage_event:.2f}%)
- **Rows with event_start_time_utc:** {with_start_time} ({coverage_time:.2f}%)
- **Rows with vendor_person_id:** {with_person_id} ({coverage_person:.2f}%)
- **Rows with home/away teams:** {with_matchup} ({coverage_matchup:.2f}%)
- **Rows with player_team via metadata:** {with_player_team} ({coverage_player_team:.2f}%)

## Top 10 Unresolved Samples
{unresolved_samples.to_markdown(index=False) if not unresolved_samples.empty else "None"}
"""

    os.makedirs(output_dir, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_content)
    
    # Also create a 'latest' symlink/copy if possible
    latest_path = os.path.join(output_dir, "unabated_mapping_coverage_latest.md")
    with open(latest_path, "w", encoding="utf-8") as f:
        f.write(report_content)
        
    print(f"Report generated: {report_path}")

if __name__ == "__main__":
    DB_PATH = "data/db/nhl_backtest.duckdb"
    OUTPUT_DIR = "outputs/monitoring"
    generate_unabated_coverage_report(DB_PATH, OUTPUT_DIR)

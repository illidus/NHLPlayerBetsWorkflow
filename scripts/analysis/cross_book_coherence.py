import duckdb
import os
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import logging
import sys

# Add src to sys.path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT_DIR, "src"))

from nhl_bets.analysis.normalize import update_player_mappings, update_event_mappings

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = "data/db/nhl_backtest.duckdb"
OUTPUT_DIR = "outputs/monitoring"

def run_cross_book_coherence(db_path: str, output_dir: str):
    con = duckdb.connect(db_path)
    
    # 1. Update mappings first to ensure we have latest canonical links
    update_player_mappings(con)
    update_event_mappings(con)
    
    # 2. Extract joined odds
    logger.info("Extracting mapped odds for coherence check...")
    df = con.execute("""
        SELECT 
            o.source_vendor,
            o.book_name_raw,
            o.player_name_raw,
            o.market_type,
            o.line,
            o.side,
            o.odds_american,
            o.odds_decimal,
            o.capture_ts_utc,
            pm.canonical_player_id,
            em.canonical_game_id,
            g.home_team as canonical_home,
            g.away_team as canonical_away
        FROM fact_prop_odds o
        LEFT JOIN dim_players_mapping pm ON o.player_name_raw = pm.vendor_player_name AND o.source_vendor = pm.source_vendor
        LEFT JOIN dim_events_mapping em ON o.event_id_vendor = em.vendor_event_id AND o.source_vendor = em.source_vendor
        LEFT JOIN dim_games g ON em.canonical_game_id = g.game_id
        WHERE em.canonical_game_id IS NOT NULL 
          AND pm.canonical_player_id IS NOT NULL
          AND o.capture_ts_utc > now() - interval '24 hours'
          AND o.market_type != 'GOALS'
    """).df()
    
    if df.empty:
        logger.warning("No recently mapped odds found for coherence check.")
        return

    # 3. Group by unique bet
    # A unique bet is defined by (game, player, market, line, side)
    group_cols = ['canonical_game_id', 'canonical_player_id', 'market_type', 'line', 'side']
    
    # Calculate median decimal odds per group
    medians = df.groupby(group_cols)['odds_decimal'].median().reset_index().rename(columns={'odds_decimal': 'median_odds'})
    
    # Join back to original data
    df = df.merge(medians, on=group_cols)
    
    # Calculate divergence (absolute and percentage)
    df['diff_decimal'] = df['odds_decimal'] - df['median_odds']
    df['diff_pct'] = (df['diff_decimal'] / df['median_odds']) * 100
    
    # 4. Identify Outliers
    # Thresholds: > 10% divergence OR > 0.3 decimal difference
    outliers = df[df['diff_pct'].abs() > 10].sort_values('diff_pct', ascending=False)
    
    # 5. Generate Report
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    report_path = os.path.join(output_dir, f"cross_book_coherence_{timestamp}.md")
    
    total_bets = df.groupby(group_cols).ngroups
    total_records = len(df)
    
    report_content = f"""# Cross-Book Price Coherence Report
**Generated at:** {datetime.now(timezone.utc).isoformat()}
**Data Scope:** Last 24 Hours
**Unique Bets Analyzed:** {total_bets}
**Total Odds Records:** {total_records}

## Divergence Summary by Vendor
"""
    
    vendor_summary = df.groupby('source_vendor')['diff_pct'].agg(['mean', 'std', 'min', 'max']).round(2)
    report_content += vendor_summary.to_markdown() + "\n\n"
    
    report_content += "## Top 20 Most Divergent Odds (Potential Outliers)\n"
    if not outliers.empty:
        cols_to_show = ['player_name_raw', 'market_type', 'line', 'side', 'source_vendor', 'book_name_raw', 'odds_american', 'median_odds', 'diff_pct']
        report_content += outliers.head(20)[cols_to_show].to_markdown(index=False)
    else:
        report_content += "No significant divergences found (>10%).\n"
        
    report_content += "\n## Outlier Count by Vendor (>10% diff)\n"
    outlier_counts = outliers.groupby('source_vendor').size().reset_index(name='count')
    report_content += outlier_counts.to_markdown(index=False)
    
    os.makedirs(output_dir, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_content)
        
    # Latest link
    latest_path = os.path.join(output_dir, "cross_book_coherence_latest.md")
    with open(latest_path, "w", encoding="utf-8") as f:
        f.write(report_content)
        
    logger.info(f"Coherence report generated: {report_path}")
    con.close()

if __name__ == "__main__":
    run_cross_book_coherence(DB_PATH, OUTPUT_DIR)

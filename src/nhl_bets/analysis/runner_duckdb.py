import pandas as pd
import numpy as np
import duckdb
import os
import sys
import logging
from datetime import datetime

# Ensure project root is in path
project_root = os.getcwd()
src_dir = os.path.join(project_root, "src")
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from nhl_bets.analysis.normalize import normalize_name, get_mapped_odds
from nhl_bets.projections.config import get_production_prob_column
from nhl_bets.analysis.side_integrity import (
    build_odds_side_lookup,
    normalize_side,
    resolve_odds_side,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = 'data/db/nhl_backtest.duckdb'
PROBS_PATH = 'outputs/projections/SingleGamePropProbabilities.csv'
OUTPUT_XLSX = 'outputs/ev_analysis/MultiBookBestBets.xlsx'
SIDE_INTEGRITY_GUARD = os.getenv("SIDE_INTEGRITY_GUARD", "0") == "1"

def main():
    logger.info("Starting Multi-Book EV Analysis...")
    
    # 1. Load Mapped Odds from DuckDB
    con = duckdb.connect(DB_PATH)
    try:
        df_odds = get_mapped_odds(con)
        logger.info(f"Loaded {len(df_odds)} mapped odds records.")
    finally:
        con.close()
        
    if df_odds.empty:
        logger.warning("No mapped odds found. Run ingestion and mapping first.")
        return

    # 2. Load Model Probabilities
    if not os.path.exists(PROBS_PATH):
        logger.error(f"Probs file not found: {PROBS_PATH}")
        return
        
    df_probs = pd.read_csv(PROBS_PATH)
    logger.info(f"Loaded {len(df_probs)} model probabilities.")
    
    # 3. Join Odds with Probs
    # Note: Use canonical_player_id if available, otherwise fallback to normalized name + team
    # Current Probs CSV doesn't have player_id, so we'll use Normalized Name + Team.
    
    df_probs['norm_name'] = df_probs['Player'].apply(normalize_name)
    df_odds['norm_name'] = df_odds['player_name_raw'].apply(normalize_name)
    
    # Merge
    merged = pd.merge(
        df_odds, 
        df_probs, 
        left_on=['norm_name'], 
        right_on=['norm_name'],
        how='inner',
        suffixes=('_raw', '_model')
    )
    
    logger.info(f"Joined {len(merged)} records.")
    
    if merged.empty:
        logger.warning("Join resulted in 0 records. Check player name normalization.")
        return

    # 3. Load base projections for stats
    base_proj_path = os.path.join('outputs', 'projections', 'BaseSingleGameProjections.csv')
    base_proj = pd.read_csv(base_proj_path) if os.path.exists(base_proj_path) else pd.DataFrame()
    base_proj['norm_name'] = base_proj['Player'].apply(normalize_name)

    # 4. Calculate EV for each record
    results = []

    side_lookup = {}
    if SIDE_INTEGRITY_GUARD:
        side_lookup = build_odds_side_lookup(df_odds)
    
    # Standard books only (exclude Pick'em/DFS with non-standard pricing)
    EXCLUDED_KEYWORDS = ['underdog', 'prizepicks', 'parlayplay', 'sleeper', 'chalkboard', 'boom']
    
    for idx, row in merged.iterrows():
        book_name_lower = row['book_name_raw'].lower()
        if any(kw in book_name_lower for kw in EXCLUDED_KEYWORDS):
            continue
            
        if row['market_type'] == 'GOALS':
            continue # Blacklisted
            
        stat_type = row['market_type'].lower()
        line = row['line']
        
        # Select correct model probability column based on policy
        prob_col = get_production_prob_column(stat_type, line, row.keys())
        
        if not prob_col or prob_col not in row:
            continue
            
        p_over_model = float(row[prob_col])
        
        # Adjust for side
        if row['side'].upper() == 'OVER':
            p_model = p_over_model
        else:
            p_model = 1.0 - p_over_model
            
        odds_decimal = row['odds_decimal']
        if not odds_decimal or odds_decimal <= 1.0:
            continue
            
        ev = (p_model * odds_decimal) - 1
        
        # Format results
        result_row = {
            'Player': row['Player'],
            'Team': row['Team'],
            'Market': row['market_type'],
            'Line': row['line'],
            'Side': row['side'],
            'Book': row['book_name_raw'],
            'Odds': row['odds_american'],
            'Model_Prob': f"{p_model:.1%}",
            'Implied_Prob': f"{1/odds_decimal:.1%}",
            'EV%': f"{ev:+.1%}",
            'ev_sort': ev,
            'Prob_Source': 'Calibrated' if 'calibrated' in prob_col else 'Raw',
            'Source_Col': prob_col,
            'event_id_vendor': row.get('event_id_vendor'),
            'canonical_game_id': row.get('canonical_game_id'),
            'event_start_time_utc': row.get('event_start_time_utc'),
            'capture_ts_utc': row.get('capture_ts_utc'),
            'source_vendor': row.get('source_vendor'),
            'raw_payload_hash': row.get('raw_payload_hash'),
        }

        if SIDE_INTEGRITY_GUARD:
            bet_side = normalize_side(row['side'])
            odds_side, odds_reason = resolve_odds_side(
                side_lookup,
                row.get('player_name_raw'),
                row.get('market_type'),
                row.get('line'),
                row.get('book_name_raw'),
                row.get('odds_american'),
            )
            side_ok = odds_side in {"OVER", "UNDER"} and bet_side == odds_side
            result_row["odds_side_interpreted"] = odds_side
            result_row["side_integrity_reason"] = odds_reason
            result_row["side_integrity_status"] = "OK" if side_ok else "SIDE_INVERSION_SUSPECT"

        results.append(result_row)
        
    df_results = pd.DataFrame(results)
    
    if df_results.empty:
        logger.warning("No bets found after filtering.")
        return

    # 5. Filter and Sort
    df_rank_source = df_results
    df_quarantine = pd.DataFrame()
    if SIDE_INTEGRITY_GUARD and "side_integrity_status" in df_results.columns:
        df_quarantine = df_results[df_results["side_integrity_status"] == "SIDE_INVERSION_SUSPECT"].copy()
        df_rank_source = df_results[df_results["side_integrity_status"] != "SIDE_INVERSION_SUSPECT"].copy()
        if not df_quarantine.empty:
            counts = df_quarantine.groupby(["Book", "Market"]).size().reset_index(name="n")
            logger.warning("Side integrity guard triggered; quarantining rows.")
            logger.warning("Side inversion counts by book/market:\n%s", counts.to_string(index=False))

    # EV >= 2%
    df_ev = df_rank_source[df_rank_source['ev_sort'] >= 0.02].sort_values('ev_sort', ascending=False)
    
    logger.info(f"Found {len(df_ev)} bets with EV% >= 2.0%")
    
    # 6. Export
    os.makedirs(os.path.dirname(OUTPUT_XLSX), exist_ok=True)
    if SIDE_INTEGRITY_GUARD:
        with pd.ExcelWriter(OUTPUT_XLSX, engine='openpyxl') as writer:
            df_ev.drop(columns=['ev_sort']).to_excel(writer, sheet_name='Ranked Bets', index=False)
            if not df_quarantine.empty:
                df_quarantine.drop(columns=['ev_sort']).to_excel(writer, sheet_name='Quarantine', index=False)
            df_results.drop(columns=['ev_sort']).to_excel(writer, sheet_name='All Bets', index=False)
        logger.info(f"Exported best bets to {OUTPUT_XLSX} (guarded)")
    else:
        df_ev.drop(columns=['ev_sort']).to_excel(OUTPUT_XLSX, index=False)
        logger.info(f"Exported best bets to {OUTPUT_XLSX}")
    
    # Print Top 10
    print("\n--- TOP 10 MULTI-BOOK BEST BETS ---")
    print(df_ev[['Player', 'Market', 'Line', 'Side', 'Book', 'Odds', 'EV%']].head(10).to_string(index=False))
    
    logger.info(f"Found {len(df_ev)} bets with EV% >= 2.0%")
    
    # 6. Export
    if not SIDE_INTEGRITY_GUARD:
        os.makedirs(os.path.dirname(OUTPUT_XLSX), exist_ok=True)
        df_ev.to_excel(OUTPUT_XLSX, index=False)
        logger.info(f"Exported best bets to {OUTPUT_XLSX}")
        
        # Print Top 10
        print("\n--- TOP 10 MULTI-BOOK BEST BETS ---")
        print(df_ev[['Player', 'Market', 'Line', 'Side', 'Book', 'Odds', 'EV%']].head(10).to_string(index=False))

if __name__ == "__main__":
    main()

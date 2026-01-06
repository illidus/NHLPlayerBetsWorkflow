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

from nhl_bets.analysis.normalize import normalize_name, get_mapped_odds, TEAM_NAME_TO_ABBR
from nhl_bets.projections.config import get_production_prob_column

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = 'data/db/nhl_backtest.duckdb'
PROBS_PATH = 'outputs/projections/SingleGamePropProbabilities.csv'
OUTPUT_XLSX = 'outputs/ev_analysis/MultiBookBestBets.xlsx'
CAPTURE_WINDOW_DAYS = 1

def normalize_team(value):
    if not isinstance(value, str):
        return None
    trimmed = value.strip()
    if trimmed in TEAM_NAME_TO_ABBR:
        return TEAM_NAME_TO_ABBR[trimmed]
    return trimmed.upper()

def main():
    logger.info("Starting Multi-Book EV Analysis...")
    
    # 1. Load Mapped Odds from DuckDB
    con = duckdb.connect(DB_PATH)
    try:
        df_odds = get_mapped_odds(con)
        df_players = con.execute("""
            SELECT
                player_id,
                player_name AS canonical_player_name,
                team AS canonical_team
            FROM dim_players
        """).df()
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
    
    df_odds = df_odds.merge(
        df_players,
        left_on='canonical_player_id',
        right_on='player_id',
        how='left'
    )
    df_odds['join_name'] = df_odds['canonical_player_name'].fillna(df_odds['player_name_raw'])
    df_odds['join_team'] = df_odds['canonical_team']
    df_odds.loc[df_odds['join_team'].isna(), 'join_team'] = None
    df_odds['join_team'] = df_odds['join_team'].astype(str).str.upper()
    df_odds.loc[df_odds['join_team'] == 'NONE', 'join_team'] = None
    df_odds['home_abbr'] = df_odds['home_team'].apply(normalize_team)
    df_odds['away_abbr'] = df_odds['away_team'].apply(normalize_team)
    df_odds['join_date'] = pd.to_datetime(df_odds['event_start_ts_utc'], errors='coerce')
    df_odds['join_date'] = df_odds['join_date'].fillna(
        pd.to_datetime(df_odds['capture_ts_utc'], errors='coerce')
    )
    df_odds['join_date'] = df_odds['join_date'].dt.normalize()
    player_key = df_odds['canonical_player_id'].astype(object).where(
        df_odds['canonical_player_id'].notna(),
        df_odds['join_name']
    )
    df_odds['player_key'] = player_key.astype(str)
    df_odds = df_odds.sort_values('capture_ts_utc').drop_duplicates(
        subset=[
            'source_vendor',
            'book_id_vendor',
            'market_type',
            'line',
            'side',
            'player_key',
            'home_abbr',
            'away_abbr'
        ],
        keep='last'
    )

    df_probs['norm_name'] = df_probs['Player'].apply(normalize_name)
    df_probs['team_abbr'] = df_probs['Team'].astype(str).str.upper()
    df_probs['prob_date'] = pd.to_datetime(df_probs['Date'], errors='coerce').dt.normalize()
    df_odds['norm_name'] = df_odds['join_name'].apply(normalize_name)
    
    # Merge
    merged = pd.merge(
        df_odds, 
        df_probs, 
        left_on=['norm_name'], 
        right_on=['norm_name'],
        how='inner',
        suffixes=('_raw', '_model')
    )

    # Team-aware join guard: prefer canonical team when available, else fallback to event teams.
    team_match = merged['team_abbr'] == merged['join_team']
    fallback_match = merged['join_team'].isna() & (
        (merged['team_abbr'] == merged['home_abbr']) | (merged['team_abbr'] == merged['away_abbr'])
    )
    merged = merged[team_match | fallback_match]

    # Capture window filter to prevent stale odds joining fresh projections.
    merged = merged[merged['join_date'].notna() & merged['prob_date'].notna()]
    merged = merged[(merged['join_date'] - merged['prob_date']).abs().dt.days <= CAPTURE_WINDOW_DAYS]
    
    logger.info(f"Joined {len(merged)} records.")
    
    if merged.empty:
        logger.warning("Join resulted in 0 records. Check player name normalization.")
        return

    # 4. Calculate EV for each record
    results = []
    
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
        results.append({
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
            'Source_Col': prob_col
        })
        
    df_results = pd.DataFrame(results)
    
    if df_results.empty:
        logger.warning("No bets found after filtering.")
        return

    # 5. Filter and Sort
    # EV >= 2%
    df_ev = df_results[df_results['ev_sort'] >= 0.02].sort_values('ev_sort', ascending=False)
    
    logger.info(f"Found {len(df_ev)} bets with EV% >= 2.0%")
    
    # 6. Export
    os.makedirs(os.path.dirname(OUTPUT_XLSX), exist_ok=True)
    df_ev.to_excel(OUTPUT_XLSX, index=False)
    logger.info(f"Exported best bets to {OUTPUT_XLSX}")
    
    # Print Top 10
    print("\n--- TOP 10 MULTI-BOOK BEST BETS ---")
    print(df_ev[['Player', 'Market', 'Line', 'Side', 'Book', 'Odds', 'EV%']].head(10).to_string(index=False))
    
if __name__ == "__main__":
    main()

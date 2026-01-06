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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = 'data/db/nhl_backtest.duckdb'
PROBS_PATH = 'outputs/projections/SingleGamePropProbabilities.csv'
OUTPUT_XLSX = 'outputs/ev_analysis/MultiBookBestBets.xlsx'

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
    df_ev.drop(columns=['ev_sort']).to_excel(OUTPUT_XLSX, index=False)
    logger.info(f"Exported best bets to {OUTPUT_XLSX}")
    
    # Print Top 10
    print("\n--- TOP 10 MULTI-BOOK BEST BETS ---")
    print(df_ev[['Player', 'Market', 'Line', 'Side', 'Book', 'Odds', 'EV%']].head(10).to_string(index=False))
    
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

import pandas as pd
import numpy as np
import duckdb
import os
import sys
import logging
from datetime import datetime, timezone, timedelta

# Ensure project root is in path
project_root = os.getcwd()
src_dir = os.path.join(project_root, "src")
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from nhl_bets.analysis.normalize import normalize_name, get_mapped_odds
from nhl_bets.projections.config import get_production_prob_column, ALPHAS, MARKET_POLICY

from nhl_bets.common.db_init import get_db_connection, DEFAULT_DB_PATH

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = DEFAULT_DB_PATH
PROBS_PATH = 'outputs/projections/SingleGamePropProbabilities.csv'
OUTPUT_XLSX = 'outputs/ev_analysis/MultiBookBestBets.xlsx'

def get_mu_column(market):
    m = market.upper()
    if m == 'GOALS': return 'mu_adj_G'
    if m == 'ASSISTS': return 'mu_adj_A'
    if m == 'POINTS': return 'mu_adj_PTS'
    if m == 'SOG': return 'mu_adj_SOG'
    if m == 'BLOCKS': return 'mu_adj_BLK'
    return None

def get_distribution_info(market):
    m = market.upper()
    if m in ['SOG', 'BLOCKS']:
        return 'Negative Binomial', ALPHAS.get(m)
    return 'Poisson', None

def filter_by_freshness(df, snapshot_ts_dt, window_minutes):
    """
    Filters odds dataframe by freshness relative to snapshot timestamp.
    Returns (df_fresh, df_excluded)
    """
    if df.empty or 'capture_ts_utc' not in df.columns:
        return df, pd.DataFrame()

    # Ensure capture_ts_utc is datetime aware
    df['capture_ts_dt'] = pd.to_datetime(df['capture_ts_utc'], utc=True, errors='coerce')
    
    # Calculate diff in minutes
    # Note: snapshot_ts_dt must be timezone-aware (UTC)
    df['freshness_minutes'] = (df['capture_ts_dt'] - snapshot_ts_dt).abs().dt.total_seconds() / 60.0
    
    # Keep rows where freshness is <= window OR freshness is None (if we wanted to be lenient, but we don't)
    # Exclude None (missing timestamps)
    mask_fresh = (df['freshness_minutes'].notna()) & (df['freshness_minutes'] <= window_minutes)
    
    df_fresh = df[mask_fresh].copy()
    df_excluded = df[~mask_fresh].copy()
    
    return df_fresh, df_excluded

def main():
    logger.info("Starting Multi-Book EV Analysis...")
    
    # 1. Load Mapped Odds from DuckDB
    con = get_db_connection(DB_PATH)
    try:
        df_odds = get_mapped_odds(con)
        logger.info(f"Loaded {len(df_odds)} mapped odds records.")
    finally:
        con.close()
        
    if df_odds.empty:
        logger.warning("No mapped odds found. Run ingestion and mapping first.")
        # Continue to produce empty output
        df_odds = pd.DataFrame()

    # 2. Load Model Probabilities
    if not os.path.exists(PROBS_PATH):
        logger.error(f"Probs file not found: {PROBS_PATH}")
        return
        
    df_probs = pd.read_csv(PROBS_PATH)
    
    # Determine Snapshot Timestamp (Freshness Baseline)
    if 'prob_snapshot_ts' in df_probs.columns and not df_probs['prob_snapshot_ts'].isnull().all():
        prob_snapshot_ts_str = str(df_probs['prob_snapshot_ts'].iloc[0])
        try:
            prob_snapshot_ts_dt = pd.to_datetime(prob_snapshot_ts_str, utc=True).to_pydatetime()
            logger.info(f"Using Canonical Snapshot TS from Data: {prob_snapshot_ts_str}")
        except Exception as e:
            logger.warning(f"Could not parse prob_snapshot_ts from data: {e}. Fallback to file mtime.")
            try:
                mtime = os.path.getmtime(PROBS_PATH)
                prob_snapshot_ts_dt = datetime.fromtimestamp(mtime, timezone.utc)
                prob_snapshot_ts_str = prob_snapshot_ts_dt.isoformat()
            except:
                prob_snapshot_ts_dt = datetime.now(timezone.utc)
                prob_snapshot_ts_str = prob_snapshot_ts_dt.isoformat()
    else:
        logger.warning("prob_snapshot_ts column not found in probs file (or empty). Fallback to file mtime.")
        try:
            mtime = os.path.getmtime(PROBS_PATH)
            prob_snapshot_ts_dt = datetime.fromtimestamp(mtime, timezone.utc)
            prob_snapshot_ts_str = prob_snapshot_ts_dt.isoformat()
        except Exception as e:
            logger.warning(f"Could not determine probs file mtime: {e}")
            prob_snapshot_ts_dt = datetime.now(timezone.utc)
            prob_snapshot_ts_str = prob_snapshot_ts_dt.isoformat()

    logger.info(f"Loaded {len(df_probs)} model probabilities. Snapshot TS: {prob_snapshot_ts_str}")
    
    # 3. Join Odds with Probs
    # Note: Use canonical_player_id if available, otherwise fallback to normalized name + team
    # Current Probs CSV doesn't have player_id, so we'll use Normalized Name + Team.
    
    if not df_odds.empty:
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
    else:
        merged = pd.DataFrame()
    
    logger.info(f"Joined {len(merged)} records.")
    
    # 4. Calculate EV for each record
    results = []
    
    # Standard books only (exclude Pick'em/DFS with non-standard pricing)
    EXCLUDED_KEYWORDS = ['underdog', 'prizepicks', 'parlayplay', 'sleeper', 'chalkboard', 'boom']
    
    if not merged.empty:
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
            
            # Provenance Metadata
            mu_col = get_mu_column(stat_type)
            mu_val = row.get(mu_col, 0.0) if mu_col else 0.0
            dist_name, alpha_val = get_distribution_info(stat_type)
            
            is_calibrated = 'calibrated' in prob_col
            
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
                'Prob_Source': 'Calibrated' if is_calibrated else 'Raw',
                'Source_Col': prob_col,
                # New Provenance Columns
                'source_vendor': row.get('source_vendor', 'UNKNOWN'),
                'capture_ts_utc': row.get('capture_ts_utc'),
                'raw_payload_hash': row.get('raw_payload_hash', ''),
                'mu': mu_val,
                'distribution': dist_name,
                'alpha': alpha_val if alpha_val is not None else '',
                'prob_snapshot_ts': prob_snapshot_ts_str
            })
        
    df_results = pd.DataFrame(results)
    
    if df_results.empty:
        logger.warning("No bets found after filtering.")
        df_ev = pd.DataFrame(columns=['Player', 'Team', 'Market', 'Line', 'Side', 'Book', 'Odds', 'Model_Prob', 'Implied_Prob', 'EV%', 'ev_sort', 'Prob_Source', 'Source_Col', 'source_vendor', 'capture_ts_utc', 'raw_payload_hash', 'mu', 'distribution', 'alpha', 'prob_snapshot_ts', 'freshness_minutes'])
    else:
        # --- FRESHNESS GATING (Phase 12.2) ---
        
        # 2. Apply Filter Window
        try:
            freshness_window = float(os.environ.get('EV_ODDS_FRESHNESS_MINUTES', 90))
        except ValueError:
            freshness_window = 90.0
            
        total_candidates = len(df_results)
        
        df_fresh, df_excluded = filter_by_freshness(df_results, prob_snapshot_ts_dt, freshness_window)
        
        logger.info(f"Freshness Filter (Window={freshness_window}m): kept {len(df_fresh)}/{total_candidates} rows.")
        
        # 3. Generate Diagnostics Report
        report_date = datetime.now().strftime('%Y-%m-%d')
        report_path = f"outputs/monitoring/ev_freshness_coverage_{report_date}.md"
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        
        with open(report_path, 'w') as f:
            f.write(f"# EV Freshness Coverage Report - {report_date}\n\n")
            f.write(f"- **Total Candidates:** {total_candidates}\n")
            f.write(f"- **Retained (Fresh):** {len(df_fresh)}\n")
            f.write(f"- **Excluded (Stale/Missing):** {len(df_excluded)}\n")
            f.write(f"- **Window:** {freshness_window} minutes\n")
            f.write(f"- **Snapshot TS:** {prob_snapshot_ts_str}\n\n")

            if not df_fresh.empty:
                min_cap = df_fresh['capture_ts_utc'].min()
                max_cap = df_fresh['capture_ts_utc'].max()
                min_fresh = df_fresh['freshness_minutes'].min()
                max_fresh = df_fresh['freshness_minutes'].max()
                f.write(f"- **Capture TS Range:** {min_cap} to {max_cap}\n")
                f.write(f"- **Freshness (min) Range:** {min_fresh:.2f} to {max_fresh:.2f}\n\n")
            
            if not df_excluded.empty:
                f.write("## Excluded Breakdown by Vendor/Book\n")
                if 'Book' in df_excluded.columns and 'source_vendor' in df_excluded.columns:
                    breakdown = df_excluded.groupby(['source_vendor', 'Book']).size().reset_index(name='count')
                    f.write(breakdown.to_markdown(index=False))
                else:
                    f.write("(Book/Vendor columns missing)")
                f.write("\n\n")
                
        # Proceed with fresh data
        df_filtered = df_fresh[df_fresh['ev_sort'] >= 0.02].copy()
        
        # 5. Filter and Sort
        # Deduplication (Deterministic)
        # Sort by capture_ts_utc descending so we keep the latest
        if 'capture_ts_utc' in df_filtered.columns:
            df_filtered = df_filtered.sort_values('capture_ts_utc', ascending=False)
            
        # Drop duplicates based on stable key + source_vendor
        dedup_cols = ['Player', 'Market', 'Line', 'Side', 'Book', 'source_vendor']
        before_count = len(df_filtered)
        df_filtered = df_filtered.drop_duplicates(subset=dedup_cols, keep='first')
        after_count = len(df_filtered)
        
        if before_count > after_count:
            logger.info(f"Deduplicated bets: {before_count} -> {after_count}")
        
        df_ev = df_filtered.sort_values('ev_sort', ascending=False)
    
    logger.info(f"Found {len(df_ev)} bets with EV% >= 2.0%")
    
    # 6. Export
    os.makedirs(os.path.dirname(OUTPUT_XLSX), exist_ok=True)
    
    # Drop intermediate columns if desired, but keep freshness for transparency
    cols_to_drop = ['ev_sort', 'capture_ts_dt']
    df_export = df_ev.drop(columns=[c for c in cols_to_drop if c in df_ev.columns])
        
    df_export.to_excel(OUTPUT_XLSX, index=False)
    logger.info(f"Exported best bets to {OUTPUT_XLSX}")
    
    # Print Top 10
    print("\n--- TOP 10 MULTI-BOOK BEST BETS ---")
    if not df_ev.empty:
        print(df_ev[['Player', 'Market', 'Line', 'Side', 'Book', 'Odds', 'EV%']].head(10).to_string(index=False))
    else:
        print("(No bets found)")

if __name__ == "__main__":
    main()
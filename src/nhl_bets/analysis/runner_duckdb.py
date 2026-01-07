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

from nhl_bets.common.db_init import get_db_connection, DEFAULT_DB_PATH, initialize_phase11_tables

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = DEFAULT_DB_PATH
PROBS_PATH = 'outputs/projections/SingleGamePropProbabilities.csv'
OUTPUT_XLSX = 'outputs/ev_analysis/MultiBookBestBets.xlsx'

def parse_exclusion_list(env_key, default_list=None):
    raw = os.environ.get(env_key, "").strip()
    if not raw:
        return []
    if raw.lower() in ("1", "true", "yes"):
        return default_list or []
    return [item.strip().lower() for item in raw.split(",") if item.strip()]

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

def filter_by_event_eligibility(df, now_utc, grace_minutes=0):
    """
    Filters bets to only include those that are bettable now (not started or within grace).
    Returns (df_eligible, df_started, df_missing_time)
    """
    if df.empty:
        return df, pd.DataFrame(), pd.DataFrame()

    # Ensure event_start_time_utc is datetime aware
    df['event_start_time_dt'] = pd.to_datetime(df['event_start_time_utc'], utc=True, errors='coerce')
    
    # event_time_delta_minutes = (event_start_time_utc - now_utc) in minutes
    df['event_time_delta_minutes'] = (df['event_start_time_dt'] - now_utc).dt.total_seconds() / 60.0
    
    # is_bettable_now: game hasn't started yet OR within grace_minutes
    # AND is_live == False (unless we specifically allow live, but task says default false)
    
    mask_missing = df['event_start_time_dt'].isna()
    mask_started = (df['event_start_time_dt'].notna()) & (df['event_start_time_dt'] < now_utc - timedelta(minutes=grace_minutes))
    
    # Optionally also filter by is_live if it's explicitly True
    # If is_live is True, we might exclude it depending on policy.
    # Task says: "only include if is_live == false by default"
    mask_live = df['is_live'] == True
    
    mask_eligible = (~mask_missing) & (~mask_started) & (~mask_live)
    
    df_eligible = df[mask_eligible].copy()
    df_started = df[mask_started].copy()
    df_missing = df[mask_missing].copy()
    df_live = df[mask_live].copy() # We can treat live as started/ineligible for now
    
    df_eligible['is_bettable_now'] = True
    
    # Combine started and live for the started return if desired, or keep separate
    return df_eligible, df_started, df_missing, df_live

def main():
    run_start_ts = datetime.now(timezone.utc)
    logger.info(f"Starting Multi-Book EV Analysis at {run_start_ts.isoformat()}...")
    
    # 1. Load Mapped Odds from DuckDB
    con = get_db_connection(DB_PATH)
    try:
        initialize_phase11_tables(con)
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
    default_excluded_keywords = ['underdog', 'prizepicks', 'parlayplay', 'sleeper', 'chalkboard', 'boom']
    excluded_keywords = parse_exclusion_list("EV_EXCLUDE_BOOK_TYPES", default_excluded_keywords)
    excluded_markets = parse_exclusion_list("EV_EXCLUDE_MARKETS", [])
    excluded_markets_upper = {m.upper() for m in excluded_markets}
    
    if not merged.empty:
        for idx, row in merged.iterrows():
            book_name_raw = row.get('book_name_raw') or ''
            book_name_lower = book_name_raw.lower()
            if excluded_keywords and any(kw in book_name_lower for kw in excluded_keywords):
                continue

            market_type = row['market_type']
            if excluded_markets_upper and market_type.upper() in excluded_markets_upper:
                continue
                
            stat_type = market_type.lower()
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
                'Model_Prob': p_model,
                'Implied_Prob': 1/odds_decimal,
                'EV%': ev,
                'Model_Prob_display': f"{p_model:.1%}",
                'Implied_Prob_display': f"{1/odds_decimal:.1%}",
                'EV_display': f"{ev:+.1%}",
                'ev_sort': ev,
                'Prob_Source': 'Calibrated' if is_calibrated else 'Raw',
                'Source_Col': prob_col,
                # New Provenance Columns
                'source_vendor': row.get('source_vendor', 'UNKNOWN'),
                'capture_ts_utc': row.get('capture_ts_utc'),
                'event_start_time_utc': row.get('event_start_time_utc'),
                'home_team': row.get('home_team'),
                'away_team': row.get('away_team'),
                'is_live': row.get('is_live', False),
                'raw_payload_hash': row.get('raw_payload_hash', ''),
                'mu': mu_val,
                'distribution': dist_name,
                'alpha': alpha_val if alpha_val is not None else '',
                'prob_snapshot_ts': prob_snapshot_ts_str
            })
        
    df_results = pd.DataFrame(results)
    
    # --- FRESHNESS GATING & REPORTING (Phase 12.2 / 12.3) ---
    # Always generate report, even if empty
    
    try:
        freshness_window = float(os.environ.get('EV_ODDS_FRESHNESS_MINUTES', 90))
    except ValueError:
        freshness_window = 90.0

    try:
        grace_minutes = float(os.environ.get('EV_EVENT_START_GRACE_MINUTES', 0))
    except ValueError:
        grace_minutes = 0.0

    total_candidates = len(df_results)
    
    if not df_results.empty:
        # 1. Freshness Filter
        df_fresh, df_excluded_stale = filter_by_freshness(df_results, prob_snapshot_ts_dt, freshness_window)
        
        # 2. Event Eligibility Filter (Phase 12.7)
        df_eligible, df_started, df_missing_time, df_live = filter_by_event_eligibility(df_fresh, run_start_ts, grace_minutes)
        
        logger.info(f"Freshness Filter: kept {len(df_fresh)}/{total_candidates} rows.")
        logger.info(f"Eligibility Filter: kept {len(df_eligible)}/{len(df_fresh)} rows (Started={len(df_started)}, MissingTime={len(df_missing_time)}, Live={len(df_live)}).")
    else:
        df_eligible = pd.DataFrame()
        df_excluded_stale = pd.DataFrame()
        df_started = pd.DataFrame()
        df_missing_time = pd.DataFrame()
        df_live = pd.DataFrame()
        logger.warning("No bets found to filter for freshness or eligibility.")

    # Generate Diagnostics Report
    run_end_ts = datetime.now(timezone.utc)
    report_ts_str = run_end_ts.strftime('%H%M%SZ')
    report_date_str = run_end_ts.strftime('%Y-%m-%d')
    
    # Format: ev_freshness_coverage_YYYY-MM-DD_HHMMSSZ.md
    report_filename = f"ev_freshness_coverage_{report_date_str}_{report_ts_str}.md"
    report_path = os.path.join("outputs", "monitoring", report_filename)
    latest_path = os.path.join("outputs", "monitoring", "ev_freshness_coverage_latest.md")
    
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    
    report_content = []
    report_content.append(f"# EV Freshness Coverage Report - {report_date_str} {report_ts_str}\n\n")
    report_content.append(f"- **Run Start (UTC):** {run_start_ts.isoformat()}\n")
    report_content.append(f"- **Run End (UTC):** {run_end_ts.isoformat()}\n")
    report_content.append(f"- **Total Candidates:** {total_candidates}\n")
    report_content.append(f"- **Total Raw Candidates:** {total_candidates}\n")
    report_content.append(f"- **Retained (Eligible & Fresh):** {len(df_eligible)}\n")
    report_content.append(f"- **Excluded (Stale):** {len(df_excluded_stale)}\n")
    report_content.append(f"- **Excluded (Started/Live):** {len(df_started) + len(df_live)}\n")
    report_content.append(f"- **Excluded (Missing Start Time):** {len(df_missing_time)}\n")
    report_content.append(f"- **Freshness Window:** {freshness_window} minutes\n")
    report_content.append(f"- **Grace Period:** {grace_minutes} minutes\n")
    report_content.append(f"- **Snapshot TS:** {prob_snapshot_ts_str}\n\n")
    
    report_content.append("## Diagnostics\n")
    report_content.append("_Note: Ensure 'Production Projections' runs immediately before 'Odds Ingestion' and 'EV Analysis' for optimal alignment._\n\n")

    if not df_eligible.empty:
        # Use capture_ts_dt which is aware UTC
        min_cap = df_eligible['capture_ts_dt'].min()
        max_cap = df_eligible['capture_ts_dt'].max()
        min_cap_str = min_cap.isoformat() if pd.notnull(min_cap) else "N/A"
        max_cap_str = max_cap.isoformat() if pd.notnull(max_cap) else "N/A"
        
        min_fresh = df_eligible['freshness_minutes'].min()
        med_fresh = df_eligible['freshness_minutes'].median()
        max_fresh = df_eligible['freshness_minutes'].max()
        
        report_content.append(f"### Eligible Data Stats\n")
        report_content.append(f"- **Capture TS Range (UTC):** {min_cap_str} to {max_cap_str}\n")
        report_content.append(f"- **Freshness (min):** Min={min_fresh:.2f}, Med={med_fresh:.2f}, Max={max_fresh:.2f}\n\n")
    
    if not df_excluded_stale.empty:
        report_content.append("## Excluded: Stale Breakdown\n")
        if 'Book' in df_excluded_stale.columns and 'source_vendor' in df_excluded_stale.columns:
            breakdown = df_excluded_stale.groupby(['source_vendor', 'Book']).size().reset_index(name='count')
            report_content.append(breakdown.to_markdown(index=False))
        report_content.append("\n\n")

    if not df_missing_time.empty:
        report_content.append("## Excluded: Missing Start Time Breakdown\n")
        breakdown = df_missing_time.groupby(['source_vendor', 'Book']).size().reset_index(name='count')
        report_content.append(breakdown.to_markdown(index=False))
        report_content.append("\n\n")
    
    if not df_started.empty or not df_live.empty:
        report_content.append("## Excluded: Already Started or Live\n")
        df_too_late = pd.concat([df_started, df_live])
        breakdown = df_too_late.groupby(['source_vendor', 'Book']).size().reset_index(name='count')
        report_content.append(breakdown.to_markdown(index=False))
        report_content.append("\n\n")
    
    full_report = "".join(report_content)
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(full_report)
        
    # Update Latest Pointer
    with open(latest_path, 'w', encoding='utf-8') as f:
        f.write(full_report)
        
    # Proceed with eligible data
    df_filtered = df_eligible[df_eligible['ev_sort'] >= 0.02].copy() if not df_eligible.empty else pd.DataFrame()
    
    # 5. Filter and Sort
    # Deduplication (Deterministic)
    # Sort by capture_ts_utc descending so we keep the latest
    if not df_filtered.empty and 'capture_ts_utc' in df_filtered.columns:
        df_filtered = df_filtered.sort_values('capture_ts_utc', ascending=False)
        
    # Drop duplicates based on stable key + source_vendor
    if not df_filtered.empty:
        dedup_cols = ['Player', 'Market', 'Line', 'Side', 'Book', 'source_vendor']
        before_count = len(df_filtered)
        df_filtered = df_filtered.drop_duplicates(subset=dedup_cols, keep='first')
        after_count = len(df_filtered)
        
        if before_count > after_count:
            logger.info(f"Deduplicated bets: {before_count} -> {after_count}")
        
        df_ev = df_filtered.sort_values('ev_sort', ascending=False)
    else:
        df_ev = pd.DataFrame()
    
    logger.info(f"Found {len(df_ev)} bets with EV% >= 2.0%")
    
    # 6. Export
    os.makedirs(os.path.dirname(OUTPUT_XLSX), exist_ok=True)
    
    # Drop intermediate columns if desired, but keep freshness and event time for transparency
    cols_to_drop = ['ev_sort', 'capture_ts_dt', 'event_start_time_dt']
    df_export = df_ev.drop(columns=[c for c in cols_to_drop if c in df_ev.columns])
        
    if df_export.empty:
        df_export.to_excel(OUTPUT_XLSX, index=False, sheet_name='BestBets')
    else:
        # Use ExcelWriter for formatting
        with pd.ExcelWriter(OUTPUT_XLSX, engine='openpyxl') as writer:
            df_export.to_excel(writer, index=False, sheet_name='BestBets')
            
            # Apply number formats
            workbook = writer.book
            if 'BestBets' not in workbook.sheetnames:
                logger.warning("BestBets sheet not found for formatting.")
                worksheet = None
            else:
                worksheet = workbook['BestBets']
            
            # Find column indices for probability and EV
            col_names = list(df_export.columns)
            try:
                m_idx = col_names.index('Model_Prob') + 1 # openpyxl is 1-indexed
                i_idx = col_names.index('Implied_Prob') + 1
                e_idx = col_names.index('EV%') + 1
                
                # Formatting as percentage with 1 decimal
                if worksheet is not None:
                    for row in range(2, len(df_export) + 2):
                        worksheet.cell(row=row, column=m_idx).number_format = '0.0%'
                        worksheet.cell(row=row, column=i_idx).number_format = '0.0%'
                        worksheet.cell(row=row, column=e_idx).number_format = '0.0%'
            except ValueError:
                pass # Columns might be missing if empty df
            
    logger.info(f"Exported best bets to {OUTPUT_XLSX}")
    
    # Print Top 10
    print("\n--- TOP 10 MULTI-BOOK BEST BETS ---")
    if not df_ev.empty:
        print(df_ev[['Player', 'Market', 'Line', 'Side', 'Book', 'Odds', 'EV_display']].head(10).to_string(index=False))
    else:
        print("(No bets found)")

if __name__ == "__main__":
    main()

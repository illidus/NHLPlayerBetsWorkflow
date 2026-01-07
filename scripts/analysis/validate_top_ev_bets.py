import pandas as pd
import numpy as np
import scipy.stats as stats
from datetime import datetime, timezone
import os

# --- Constants ---
INPUT_FILE = "outputs/ev_analysis/MultiBookBestBets.xlsx"
OUTPUT_REPORT = f"outputs/monitoring/top_ev_validation_report_{datetime.now().strftime('%Y-%m-%d')}.md"
N_TOP = 25
ODDS_FRESHNESS_WINDOW_MINUTES = 60

# --- Helper Functions ---
def parse_ts(ts_str):
    if pd.isna(ts_str) or ts_str == '':
        return None
    try:
        # Try ISO format
        dt = datetime.fromisoformat(str(ts_str).replace('Z', '+00:00'))
        if dt.tzinfo is None:
            # Assume UTC if naive, as per field name
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None

def compute_poisson_p_over(mu, line):
    # P(X >= k_over) where k_over = ceil(line)
    # Standard prop convention: Over X.5 -> X >= ceil(X.5).
    k = np.floor(line) + 1
    # sf is survival function = 1 - cdf = P(X > k-1) = P(X >= k)
    return stats.poisson.sf(k-1, mu)

def compute_nbinom_p_over(mu, alpha, line):
    if alpha <= 0: return np.nan
    # Scipy parameterization: n (successes), p (probability of success)
    # Mean = n(1-p)/p
    # Variance = Mean + alpha * Mean^2
    # p = 1 / (1 + alpha * Mean)
    # n = 1 / alpha
    
    r = 1.0 / alpha
    p = 1.0 / (1.0 + alpha * mu)
    
    k = np.floor(line) + 1
    return stats.nbinom.sf(k-1, r, p)

def american_to_decimal(odds):
    try:
        o = float(odds)
        if o > 0:
            return 1 + (o / 100.0)
        else:
            return 1 + (100.0 / abs(o))
    except:
        return np.nan

# --- Main Analysis ---
def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    print(f"Loading {INPUT_FILE}...")
    try:
        df = pd.read_excel(INPUT_FILE)
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return
    
    # 1. Filter N_TOP by EV% descending
    # Ensure EV% column is float if it's string formatted
    if 'EV%' in df.columns:
        if df['EV%'].dtype == 'object':
            df['ev_float'] = df['EV%'].astype(str).str.rstrip('%').astype(float) / 100.0
        else:
            df['ev_float'] = df['EV%']
    else:
        print("Column 'EV%' not found.")
        return
        
    df = df.sort_values('ev_float', ascending=False).head(N_TOP).copy()
    
    report_lines = []
    report_lines.append(f"# Top EV Bets Validation Report - {datetime.now().strftime('%Y-%m-%d')}")
    report_lines.append(f"\n**Source File:** `{INPUT_FILE}`")
    report_lines.append(f"**Analysis Time:** {datetime.now().strftime('%H:%M UTC')}")
    report_lines.append(f"**Freshness Window:** {ODDS_FRESHNESS_WINDOW_MINUTES} mins")
    
    # Section A
    total_rows = len(df)
    excluded_rows = 0
    retained_rows = []
    
    for idx, row in df.iterrows():
        cap_ts = parse_ts(row.get('capture_ts_utc'))
        snap_ts = parse_ts(row.get('prob_snapshot_ts'))
        
        status = "OK"
        if cap_ts and snap_ts:
            diff_mins = abs((cap_ts - snap_ts).total_seconds()) / 60.0
            if diff_mins > ODDS_FRESHNESS_WINDOW_MINUTES:
                status = "STALE"
                excluded_rows += 1
        elif not cap_ts or not snap_ts:
            # Missing timestamps treated as excluded/stale for safety
            status = "MISSING_TS"
            excluded_rows += 1 
        
        row['freshness_status'] = status
        if status == "OK":
            retained_rows.append(row)
            
    report_lines.append("\n## Section A: Data Selection & Freshness")
    report_lines.append(f"- Total rows examined (Top {N_TOP}): {total_rows}")
    report_lines.append(f"- Rows excluded (Stale/Missing TS): {excluded_rows}")
    report_lines.append(f"- Rows retained for audit: {len(retained_rows)}")
    
    if not retained_rows:
        report_lines.append("\nNo bets retained. Aborting detailed analysis.")
        with open(OUTPUT_REPORT, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        print(f"Report generated (empty): {OUTPUT_REPORT}")
        return

    # Section B, C, D, E processing
    retained_df = pd.DataFrame(retained_rows)
    
    results_table = []
    
    for idx, row in retained_df.iterrows():
        # -- B. Probability Recalculation --
        market = row.get('Market', '')
        line = float(row.get('Line', 0))
        side = row.get('Side', '').upper()
        dist = row.get('distribution', '')
        mu = float(row.get('mu', 0))
        alpha = row.get('alpha')
        
        p_recalc = 0.0
        
        # Determine P_over based on distribution
        if 'Poisson' in dist:
            p_over = compute_poisson_p_over(mu, line)
        elif 'Negative Binomial' in dist:
            try:
                a_val = float(alpha)
                p_over = compute_nbinom_p_over(mu, a_val, line)
            except:
                p_over = np.nan
        else:
            # Fallback if distribution unknown (should not happen for valid rows)
            p_over = np.nan
            
        if side == 'UNDER':
            p_model_recalc = 1.0 - p_over
        else:
            p_model_recalc = p_over
            
        # Compare with sheet
        # Sheet 'Model_Prob' can be numeric 0.449 or string "44.9%"
        try:
            val = row.get('Model_Prob', 0)
            if isinstance(val, str):
                p_sheet = float(val.strip('%')) / 100.0
            else:
                p_sheet = float(val)
        except:
            p_sheet = 0.0
            
        delta_p = abs(p_model_recalc - p_sheet)
        p_flag = "MISMATCH" if delta_p > 0.005 else "OK"
        
        # -- C. EV Recalculation --
        odds_amer = row.get('Odds')
        odds_dec = american_to_decimal(odds_amer)
        
        ev_recalc = (p_model_recalc * odds_dec) - 1.0
        
        # Sheet EV
        try:
            val = row.get('EV%', 0)
            if isinstance(val, str):
                ev_sheet = float(val.strip('%')) / 100.0
            else:
                ev_sheet = float(val)
        except:
            ev_sheet = 0.0
            
        delta_ev = abs(ev_recalc - ev_sheet)
        ev_flag = "MISMATCH" if delta_ev > 0.005 else "OK" 
        
        # -- D. Edge Robustness --
        ev_minus = ((p_model_recalc - 0.02) * odds_dec) - 1.0
        ev_base = ev_recalc
        ev_plus = ((p_model_recalc + 0.02) * odds_dec) - 1.0
        
        if ev_minus > 0:
            robustness = "ROBUST"
        elif ev_base > 0:
            robustness = "FRAGILE"
        else:
            # Recalc shows negative EV
            robustness = "NEGATIVE_RECALC"
            
        if (ev_minus < 0 and ev_plus > 0): 
             # If it spans zero, it's knife-edge, unless it was already negative
             if ev_minus < 0 and ev_base > 0:
                 robustness = "KNIFE-EDGE"
        
        # -- Recommendation --
        rec = "BETTABLE"
        if robustness in ["FRAGILE", "KNIFE-EDGE"]:
            rec = "CAUTION"
        if robustness == "NEGATIVE_RECALC":
             rec = "DO NOT BET (Neg EV)"
        if p_flag == "MISMATCH" or ev_flag == "MISMATCH":
            rec = "DO NOT BET (Error)"
            
        # -- Provenance Check (Section E) --
        prov_cols = ['source_vendor', 'Book', 'capture_ts_utc', 'raw_payload_hash', 'prob_snapshot_ts']
        missing_prov = [c for c in prov_cols if pd.isna(row.get(c)) or str(row.get(c)) == '']
        if missing_prov:
            rec = "DO NOT BET (No Prov)"
            
        # Store for table
        results_table.append({
            'Player': row.get('Player'),
            'Market': f"{market} ({line})",
            'Side': side,
            'Book': f"{row.get('Book')} ({row.get('source_vendor')})",
            'EV%': f"{ev_sheet:.1%}",
            'Freshness': "OK",
            'Delta_P': f"{delta_p:.4f}",
            'Robustness': robustness,
            'Rec': rec
        })

    # Section F Table
    report_lines.append("\n## Section F: Final Decision Table")
    report_lines.append("| Player | Market | Side | Book | EV% | ΔP | Robustness | Recommendation |")
    report_lines.append("|---|---|---|---|---|---|---|---|")
    
    for r in results_table:
        line_str = f"| {r['Player']} | {r['Market']} | {r['Side']} | {r['Book']} | {r['EV%']} | {r['Delta_P']} | {r['Robustness']} | {r['Rec']} |"
        report_lines.append(line_str)

    # Write Report
    with open(OUTPUT_REPORT, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
        
    print(f"Report generated: {OUTPUT_REPORT}")

if __name__ == "__main__":
    main()

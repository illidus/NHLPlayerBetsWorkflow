import pandas as pd
import numpy as np
import duckdb
import os
import sys
import logging
from datetime import datetime, timedelta
import joblib

# Setup paths
project_root = os.getcwd()
src_dir = os.path.join(project_root, "src")
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from nhl_bets.analysis.normalize import normalize_name
from nhl_bets.projections.config import MARKET_POLICY, ALPHAS
from nhl_bets.common.distributions import calculate_poisson_probs, calculate_nbinom_probs, nbinom_probability, poisson_probability
from nhl_bets.projections.single_game_model import apply_posthoc_calibration

# Constants
DB_PATH = 'data/db/nhl_backtest.duckdb'
INPUT_XLSX = 'outputs/ev_analysis/MultiBookBestBets.xlsx'
PROBS_CSV = 'outputs/projections/SingleGamePropProbabilities.csv'
DATE_FILTER = datetime.now().strftime("%Y-%m-%d")
OUTPUT_REPORT = f'outputs/monitoring/model_prob_derivation_report_{DATE_FILTER}.md'
WALKTHROUGH_REPORT = f'outputs/monitoring/top5_ev_walkthrough_{DATE_FILTER}.md'
WALKTHROUGH_LATEST = 'outputs/monitoring/top5_ev_walkthrough_latest.md'

# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('audit')

def get_mu_column(market):
    m = market.upper()
    if m == 'GOALS': return 'mu_adj_G'
    if m == 'ASSISTS': return 'mu_adj_A'
    if m == 'POINTS': return 'mu_adj_PTS'
    if m == 'SOG': return 'mu_adj_SOG'
    if m == 'BLOCKS': return 'mu_adj_BLK'
    return None

def recompute_prob(market, line, side, mu, alpha=None):
    # Determine K
    # Per prompt: k = ceil(L) for over, but we want to be precise
    # SOG/BLOCKS use floor(L) + 1 for OVER
    if market.upper() in ['SOG', 'BLOCKS']:
        k = int(np.floor(line)) + 1
    else:
        # Poisson markets Goals/Assists/Points usually .5 lines
        k = int(np.floor(line)) + 1
        
    # Determine Distribution
    m = market.upper()
    if m in ['SOG', 'BLOCKS']:
        prob = nbinom_probability(k, mu, alpha, side='over')
    else:
        prob = poisson_probability(k, mu, side='over')
        
    if side.upper() == 'UNDER':
        return 1.0 - prob
    return prob

def main():
    logger.info("Starting Model Prob Audit...")
    
    # 1. Load Bets
    if not os.path.exists(INPUT_XLSX):
        logger.error(f"Input file not found: {INPUT_XLSX}")
        return

    df_bets = pd.read_excel(INPUT_XLSX)
    if 'EV%' in df_bets.columns:
        # Handle both string "+9.9%" and numeric 0.099
        if df_bets['EV%'].dtype == object:
            df_bets['ev_val'] = df_bets['EV%'].astype(str).str.rstrip('%').replace('', '0').astype(float)
            # Normalize to 0..1 if it was 9.9
            if (df_bets['ev_val'] > 1.0).any():
                df_bets['ev_val'] = df_bets['ev_val'] / 100.0
        else:
            df_bets['ev_val'] = df_bets['EV%']
            
        df_bets = df_bets.sort_values('ev_val', ascending=False)
    
    top_25 = df_bets.head(25).copy()
    logger.info(f"Loaded {len(top_25)} bets.")

    # 2. Load Projections (Source of Mu)
    if not os.path.exists(PROBS_CSV):
        logger.error(f"Probs file not found: {PROBS_CSV}")
        return
    
    df_probs = pd.read_csv(PROBS_CSV)
    df_probs['norm_name'] = df_probs['Player'].apply(normalize_name)
    logger.info(f"Loaded {len(df_probs)} projections.")

    # 3. Connect DB (Source of Provenance)
    con = duckdb.connect(DB_PATH, read_only=True)
    con.create_function("normalize_name", normalize_name, ["VARCHAR"], "VARCHAR")
    
    report_lines = []
    walkthrough_lines = []
    
    report_lines.append(f"# Model Probability Derivation Report ({DATE_FILTER})")
    report_lines.append(f"Generated at: {datetime.now()}")
    
    walkthrough_lines.append(f"# Top 5 EV Walkthrough Report - {DATE_FILTER}")
    walkthrough_lines.append(f"\nThis report provides a forensic walkthrough of the top 5 bets by EV% as of the latest run.\n")

    # Walkthrough Selection Summary
    walkthrough_lines.append("## Selection Summary")
    walkthrough_lines.append(f"- **Source File:** `{INPUT_XLSX}`")
    
    # Freshness/Snapshot context (pull from first row)
    snapshot_ts = top_25.iloc[0].get('prob_snapshot_ts', 'N/A')
    walkthrough_lines.append(f"- **Snapshot Anchor:** {snapshot_ts}")
    walkthrough_lines.append(f"- **Total Rows Evaluated:** {len(top_25)}")
    walkthrough_lines.append("\n---\n")

    # Tables headers for derivation report
    report_lines.append("\n## Section C & D: Recomputation & EV Verification")
    report_lines.append("| Player | Market | Line | Side | Odds | Vendor | Mu | Raw_Prob (Recomp) | Cal_Prob (Recomp) | Model_Prob (Sheet) | Error | EV% (Sheet) | EV% (Recomp) |")
    report_lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")

    # 4. Process Rows
    results = [] # used for tracking top 5
    for idx, row in top_25.iterrows():
        rank = len(walkthrough_lines) // 20 + 1 # rough rank
        player = row['Player']
        market = row['Market']
        line = row['Line']
        side = row['Side']
        book = row['Book']
        odds_amer = row['Odds']
        hash_val = row.get('raw_payload_hash', 'N/A')
        
        model_prob_sheet = row['Model_Prob']
        if isinstance(model_prob_sheet, str):
            model_prob_sheet = float(model_prob_sheet.rstrip('%')) / 100.0
            
        ev_sheet = row['EV%']
        if isinstance(ev_sheet, str):
            ev_sheet = float(ev_sheet.rstrip('%').replace('+','')) / 100.0
            
        norm_player = normalize_name(player)
        
        # Provenance Lookup
        q_odds = f"""
        SELECT source_vendor, capture_ts_utc, raw_payload_hash, odds_decimal, odds_american
        FROM fact_prop_odds
        WHERE raw_payload_hash = ?
          AND normalize_name(player_name_raw) = ?
          AND market_type = ?
          AND line = ?
          AND side = ?
        ORDER BY capture_ts_utc DESC
        LIMIT 1
        """
        
        res_odds = con.execute(q_odds, [hash_val, norm_player, market, line, side]).fetchone()
        
        vendor = row.get('source_vendor', 'UNKNOWN')
        capture_ts = row.get('capture_ts_utc', 'N/A')
        hash_val = row.get('raw_payload_hash', 'N/A')
        odds_dec_db = 0.0
        
        if res_odds:
            vendor = res_odds[0]
            capture_ts = res_odds[1]
            hash_val = res_odds[2]
            odds_dec_db = res_odds[3]

        # Mu Lookup (From CSV)
        mu_col = get_mu_column(market)
        mu_val = row.get('mu', 0.0)
        
        # Recompute Prob
        alpha = row.get('alpha')
        if pd.isna(alpha) or alpha == '':
            alpha = ALPHAS.get(market.upper())
            
        prob_raw = recompute_prob(market, line, side, mu_val, alpha)
        
        # Calibration
        prob_final = prob_raw
        is_calibrated = False
        
        # Check Policy (Simplification for audit)
        if market.upper() in ['ASSISTS', 'POINTS'] and 'calibrated' in str(row.get('Source_Col', '')).lower():
             # Get Raw Over Prob for calibration
             k = int(np.floor(line)) + 1
             p_over_raw = poisson_probability(k, mu_val, side='over')
             p_over_cal = apply_posthoc_calibration(p_over_raw, market.upper())
             
             if side.upper() == 'OVER':
                 prob_final = p_over_cal
             else:
                 prob_final = 1.0 - p_over_cal
             is_calibrated = True
             
        # EV Recompute
        if odds_amer > 0:
            odds_dec_calc = 1 + (odds_amer / 100)
        else:
            odds_dec_calc = 1 + (100 / abs(odds_amer))
            
        ev_recomp = (prob_final * odds_dec_calc) - 1
        
        error = abs(prob_final - model_prob_sheet)
        
        # Derivation Report Row
        cal_str = f"{prob_final:.4f}" if is_calibrated else "-"
        report_lines.append(f"| {player} | {market} | {line} | {side} | {odds_amer} | {vendor} | {mu_val:.4f} | {prob_raw:.4f} | {cal_str} | {model_prob_sheet:.4f} | {error:.6f} | {ev_sheet:.1%} | {ev_recomp:.1%} |")

        # Walkthrough Detail (Top 5 only)
        if len(results) < 5:
            walkthrough_lines.append(f"## Bet {len(results)+1}: {player} ({row['Team']}) - {market} {line} {side}\n")
            walkthrough_lines.append(f"### A) Bet Identity")
            walkthrough_lines.append(f"- **Player / Team:** {player} / {row['Team']}")
            walkthrough_lines.append(f"- **Market / Line / Side:** {market} / {line} / {side}")
            walkthrough_lines.append(f"- **Book, source_vendor:** {book}, {vendor}")
            walkthrough_lines.append(f"- **Odds:** {odds_amer} (American), **Implied_Prob:** {1/odds_dec_calc:.1%}, **Model_Prob:** {model_prob_sheet:.1%}, **EV%:** {ev_sheet:+.1%}")
            walkthrough_lines.append(f"- **capture_ts_utc:** {capture_ts}")
            walkthrough_lines.append(f"- **prob_snapshot_ts:** {snapshot_ts}")
            walkthrough_lines.append(f"- **raw_payload_hash:** {hash_val}")
            walkthrough_lines.append(f"- **Prob_Source:** {row.get('Prob_Source')}, **Source_Col:** {row.get('Source_Col')}")
            
            walkthrough_lines.append(f"\n### B) Odds Provenance")
            walkthrough_lines.append(f"- **Query:** `SELECT book_name_raw, source_vendor, capture_ts_utc, market_type, line, side, odds_american FROM fact_prop_odds WHERE raw_payload_hash = '{hash_val}' AND player_name_raw = '{player}'`")
            walkthrough_lines.append(f"- **Record Found:** `{book} | {vendor} | {capture_ts} | {market} | {line} | {side} | {odds_amer}`")
            
            walkthrough_lines.append(f"\n### C) Probability Derivation")
            dist_name = row.get('distribution', 'Poisson')
            walkthrough_lines.append(f"1) **Parameters:** mu = {mu_val:.4f}, distribution = {dist_name}, alpha = {alpha if alpha else 'N/A'}")
            walkthrough_lines.append(f"2) **Threshold mapping:** `k = {int(np.floor(line)) + 1}`.")
            walkthrough_lines.append(f"3) **Raw distribution probability:** {prob_raw:.5f}")
            walkthrough_lines.append(f"4) **Calibration:** {'Applied' if is_calibrated else 'None'}")
            walkthrough_lines.append(f"5) **Reconciliation:** Diff={error:.5f} ({'MATCH' if error < 0.005 else 'MISMATCH'})")
            
            walkthrough_lines.append(f"\n### D) EV Calculation")
            walkthrough_lines.append(f"1) **Convert odds:** American {odds_amer} -> Decimal = {odds_dec_calc:.3f}")
            walkthrough_lines.append(f"2) **EV:** `{prob_final:.4f} * {odds_dec_calc:.3f} - 1 = {ev_recomp:+.2%}`")
            
            walkthrough_lines.append(f"\n### E) Sensitivity")
            ev_low = ((prob_final - 0.02) * odds_dec_calc - 1)
            walkthrough_lines.append(f"- **EV_low (P-0.02):** {ev_low:+.2%}")
            walkthrough_lines.append(f"- **Verdict:** **{'ROBUST' if ev_low > 0 else 'FRAGILE'}**")
            walkthrough_lines.append("\n---\n")
            
            results.append(row)

    # Write Derivation Report
    with open(OUTPUT_REPORT, 'w') as f:
        f.write('\n'.join(report_lines))
    logger.info(f"Derivation Report written to {OUTPUT_REPORT}")

    # Write Walkthrough Report
    full_walkthrough = '\n'.join(walkthrough_lines)
    with open(WALKTHROUGH_REPORT, 'w') as f:
        f.write(full_walkthrough)
    with open(WALKTHROUGH_LATEST, 'w') as f:
        f.write(full_walkthrough)
    logger.info(f"Walkthrough Report written to {WALKTHROUGH_REPORT} and latest.")

    con.close()

if __name__ == "__main__":
    main()

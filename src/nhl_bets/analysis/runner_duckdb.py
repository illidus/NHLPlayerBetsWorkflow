import pandas as pd
import numpy as np
import duckdb
import os
import sys
import logging
import math
from datetime import datetime, timezone, timedelta

# Ensure project root is in path
project_root = os.getcwd()
src_dir = os.path.join(project_root, "src")
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from nhl_bets.analysis.normalize import normalize_name, get_mapped_odds
from nhl_bets.projections.config import get_production_prob_column, ALPHAS, MARKET_POLICY, BETAS, LG_SA60, LG_XGA60, ITT_BASE

from nhl_bets.common.db_init import get_db_connection, DEFAULT_DB_PATH, initialize_phase11_tables

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = DEFAULT_DB_PATH
PROBS_PATH = 'outputs/projections/SingleGamePropProbabilities.csv'
OUTPUT_XLSX = 'outputs/ev_analysis/MultiBookBestBets.xlsx'

def _poisson_p_over(mu, line_int):
    if mu is None or line_int is None:
        return None
    try:
        mu = float(mu)
        line_int = int(line_int)
    except Exception:
        return None
    if line_int <= 0:
        return 1.0
    exp_term = math.exp(-mu)
    cumulative = 0.0
    for k in range(0, line_int):
        cumulative += (mu ** k) / math.factorial(k)
    return 1.0 - (exp_term * cumulative)

def _line_to_int(line):
    try:
        line = float(line)
    except Exception:
        return None
    if abs(line - 0.5) < 1e-6:
        return 1
    if abs(line - 1.5) < 1e-6:
        return 2
    if abs(line - 2.5) < 1e-6:
        return 3
    return None

def _nbinom_p_over(mu, alpha, line_int):
    if mu is None or alpha is None or line_int is None:
        return None
    try:
        mu = float(mu)
        alpha = float(alpha)
        line_int = int(line_int)
    except Exception:
        return None
    if mu <= 0 or alpha <= 0:
        return None
    if line_int <= 0:
        return 1.0
    # NB parameterization: variance = mu + alpha * mu^2
    r = 1.0 / alpha
    p = r / (r + mu)
    # cumulative sum of pmf up to line_int - 1
    cumulative = 0.0
    pmf = p ** r
    cumulative += pmf
    for k in range(1, line_int):
        pmf = pmf * (1 - p) * (r + k - 1) / k
        cumulative += pmf
    return 1.0 - cumulative

def write_ev_forensics_report(df_results, df_eligible, df_filtered, run_start_ts, prob_snapshot_ts_str,
                              freshness_window, grace_minutes, excluded_keywords, report_dir="outputs/monitoring"):
    """
    Generates an EV forensics report aligned to the current BestBets run.
    Diagnostic only; does not change production outputs.
    """
    os.makedirs(report_dir, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H%M%SZ')
    report_path = os.path.join(report_dir, f"ev_forensics_top50_aligned_asof_{ts}.md")

    # Use filtered if available, else eligible
    top_source = df_filtered if not df_filtered.empty else df_eligible
    top50 = top_source.sort_values('EV%', ascending=False).head(50).copy() if not top_source.empty else pd.DataFrame()

    # Step 2 line alignment (supported lines only)
    if not top50.empty:
        top50['line_status'] = top50.apply(
            lambda r: 'LINE_MATCH' if _line_to_int(r['Line']) in (1, 2, 3)
            else 'LINE_MISMATCH', axis=1
        )
        line_status_counts = top50['line_status'].value_counts().reset_index()
        line_status_counts.columns = ['line_status', 'count']
    else:
        line_status_counts = pd.DataFrame()

    # Step 3 tail sanity: recompute p_over from mu for Poisson/NB markets
    if not top50.empty and 'mu' in top50.columns and 'distribution' in top50.columns:
        top50['line_int'] = top50['Line'].apply(_line_to_int)
        top50['p_over_selected'] = top50.apply(
            lambda r: r['Model_Prob'] if str(r['Side']).upper() == 'OVER' else (1.0 - r['Model_Prob']), axis=1
        )
        def recompute_p_over(row):
            dist = str(row.get('distribution') or '').lower()
            if dist == 'poisson':
                return _poisson_p_over(row.get('mu'), row.get('line_int'))
            if dist == 'negative binomial':
                return _nbinom_p_over(row.get('mu'), row.get('alpha'), row.get('line_int'))
            return None
        top50['p_over_recalc'] = top50.apply(recompute_p_over, axis=1)
        top50['abs_dev'] = (top50['p_over_selected'] - top50['p_over_recalc']).abs()
        max_dev = top50['abs_dev'].max()
        max_dev_rows = top50.sort_values('abs_dev', ascending=False).head(5)[
            ['Player', 'Market', 'Line', 'Side', 'p_over_selected', 'p_over_recalc', 'abs_dev', 'mu', 'line_int', 'distribution', 'alpha']
        ]
    else:
        max_dev = None
        max_dev_rows = pd.DataFrame()

    # Step 4 calibration plateau: bucket p_over_selected
    if not top50.empty:
        top50['p_over_bucket'] = top50.apply(
            lambda r: round(r['Model_Prob'], 6) if str(r['Side']).upper() == 'OVER' else round(1.0 - r['Model_Prob'], 6),
            axis=1
        )
        plateau_counts = top50.groupby('p_over_bucket', dropna=True).agg(
            count=('p_over_bucket', 'size'),
            avg_ev=('EV%', 'mean')
        ).reset_index().sort_values('count', ascending=False)
    else:
        plateau_counts = pd.DataFrame()

    # Step 5 book dispersion (eligible data)
    if not df_eligible.empty:
        df_eligible['prop_key'] = df_eligible.apply(
            lambda r: f"{r['Player']}|{r['Market']}|{r['Line']}|{r['Side']}|{r.get('event_start_time_utc')}", axis=1
        )
        disp = df_eligible.groupby('prop_key').agg(
            book_count=('Book', 'nunique'),
            implied_min=('Implied_Prob', 'min'),
            implied_max=('Implied_Prob', 'max'),
            implied_median=('Implied_Prob', 'median')
        ).reset_index()
        top50['prop_key'] = top50.apply(
            lambda r: f"{r['Player']}|{r['Market']}|{r['Line']}|{r['Side']}|{r.get('event_start_time_utc')}", axis=1
        ) if not top50.empty else None
        top50_disp = top50.merge(disp, on='prop_key', how='left') if not top50.empty else pd.DataFrame()
        if not top50_disp.empty:
            top50_disp['outlier_flag'] = (top50_disp['book_count'] >= 3) & (
                (top50_disp['Implied_Prob'] - top50_disp['implied_median']).abs() >= 0.08
            )
            outlier_examples = top50_disp[top50_disp['outlier_flag']].head(10)[
                ['Player', 'Market', 'Line', 'Side', 'Book', 'Implied_Prob', 'implied_median', 'book_count']
            ]
        else:
            outlier_examples = pd.DataFrame()
    else:
        outlier_examples = pd.DataFrame()

    # Format top50 for output
    if not top50.empty:
        format_df = top50.copy()
        format_df['Model_Prob'] = format_df['Model_Prob'].apply(lambda x: f"{x:.4f}" if pd.notna(x) else '')
        format_df['Implied_Prob'] = format_df['Implied_Prob'].apply(lambda x: f"{x:.4f}" if pd.notna(x) else '')
        format_df['EV%'] = format_df['EV%'].apply(lambda x: f"{x:+.4f}" if pd.notna(x) else '')
        format_df['Odds'] = format_df['Odds'].apply(lambda x: f"{int(x)}" if pd.notna(x) else '')
    else:
        format_df = pd.DataFrame()

    # Trace section: Top 10 bets with full data lineage
    if not top50.empty:
        trace_df = top50.copy().head(10)
        trace_df['p_over_raw'] = trace_df['p_over_raw'].apply(lambda x: f"{x:.6f}" if pd.notna(x) else '')
        trace_df['p_over_calibrated'] = trace_df['p_over_calibrated'].apply(lambda x: f"{x:.6f}" if pd.notna(x) else '')
        trace_df['p_over_selected'] = trace_df['p_over_selected'].apply(lambda x: f"{x:.6f}" if pd.notna(x) else '')
        trace_df['Model_Prob'] = trace_df['Model_Prob'].apply(lambda x: f"{x:.6f}" if pd.notna(x) else '')
        trace_df['Implied_Prob'] = trace_df['Implied_Prob'].apply(lambda x: f"{x:.6f}" if pd.notna(x) else '')
        trace_df['EV%'] = trace_df['EV%'].apply(lambda x: f"{x:+.6f}" if pd.notna(x) else '')
        if 'freshness_minutes' in trace_df.columns:
            trace_df['freshness_minutes'] = trace_df['freshness_minutes'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else '')
        else:
            trace_df['freshness_minutes'] = ''
        if 'event_time_delta_minutes' in trace_df.columns:
            trace_df['event_time_delta_minutes'] = trace_df['event_time_delta_minutes'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else '')
        else:
            trace_df['event_time_delta_minutes'] = ''
    else:
        trace_df = pd.DataFrame()

    # Projection inputs (BaseSingleGameProjections + GameContext)
    base_proj_path = os.path.join('outputs', 'projections', 'BaseSingleGameProjections.csv')
    context_path = os.path.join('outputs', 'projections', 'GameContext.csv')
    base_df = pd.DataFrame()
    context_df = pd.DataFrame()
    if os.path.exists(base_proj_path):
        try:
            base_df = pd.read_csv(base_proj_path)
        except Exception:
            base_df = pd.DataFrame()
    if os.path.exists(context_path):
        try:
            context_df = pd.read_csv(context_path)
        except Exception:
            context_df = pd.DataFrame()

    proj_trace_df = pd.DataFrame()
    if not trace_df.empty and not base_df.empty and 'Player' in base_df.columns:
        proj_trace_df = trace_df.merge(base_df, on='Player', how='left', suffixes=('', '_base'))
        if not context_df.empty and 'Player' in context_df.columns:
            proj_trace_df = proj_trace_df.merge(context_df, on='Player', how='left', suffixes=('', '_ctx'))
    # Math breakdown based on projection inputs (top 10)
    breakdown_df = pd.DataFrame()
    if not proj_trace_df.empty:
        def is_valid(val):
            return val is not None and not (isinstance(val, float) and np.isnan(val)) and not pd.isna(val)

        def clamp(val, lo, hi):
            return max(lo, min(hi, val))

        def compute_row(r):
            base_toi = r.get('TOI', 15.0)
            if not is_valid(base_toi) or base_toi == 0:
                base_toi = 15.0
            proj_toi = r.get('proj_toi', None)
            if not is_valid(proj_toi):
                proj_toi = base_toi
            toi_factor = (proj_toi / base_toi) if base_toi > 0 else 1.0

            mult_opp_sog = 1.0
            mult_opp_g = 1.0
            mult_goalie = 1.0
            mult_itt = 1.0
            mult_b2b = 1.0

            opp_sa60 = r.get('opp_sa60', None)
            if is_valid(opp_sa60) and LG_SA60 > 0:
                mult_opp_sog = (opp_sa60 / LG_SA60) ** BETAS['opp_sog']

            opp_xga60 = r.get('opp_xga60', None)
            if is_valid(opp_xga60) and LG_XGA60 > 0:
                mult_opp_g = (opp_xga60 / LG_XGA60) ** BETAS['opp_g']

            goalie_gsax60 = r.get('goalie_gsax60', None)
            goalie_xga60 = r.get('goalie_xga60', None)
            if is_valid(goalie_gsax60) and is_valid(goalie_xga60) and goalie_xga60 > 0:
                raw_m = max(0.1, 1 - (goalie_gsax60 / goalie_xga60))
                mult_goalie = clamp(raw_m ** BETAS['goalie'], 0.5, 1.5)

            itt = r.get('implied_team_total', None)
            if is_valid(itt) and ITT_BASE > 0:
                mult_itt = (itt / ITT_BASE) ** BETAS['itt']

            is_b2b = r.get('is_b2b', None)
            if is_valid(is_b2b) and is_b2b in [1, '1', True]:
                mult_b2b = np.exp(BETAS['b2b'])

            scoring_mult = mult_opp_g * mult_goalie * mult_itt * mult_b2b
            opp_sog_mult = mult_opp_sog * mult_b2b

            market = str(r.get('Market', '')).upper()
            base_stat = None
            base_stat_source = ''
            mu_adj_calc = None

            if market == 'GOALS':
                base_stat = r.get('G', None)
                base_stat_source = 'G'
                if is_valid(base_stat):
                    mu_adj_calc = base_stat * scoring_mult * toi_factor
            elif market == 'ASSISTS':
                base_stat = r.get('A', None)
                base_stat_source = 'A'
                if is_valid(base_stat):
                    mu_adj_calc = base_stat * scoring_mult * toi_factor
            elif market == 'POINTS':
                base_stat = r.get('PTS', None)
                base_stat_source = 'PTS'
                if is_valid(base_stat):
                    mu_adj_calc = base_stat * scoring_mult * toi_factor
            elif market == 'SOG':
                corsi_60 = r.get('corsi_per_60_L20', None)
                thru_pct = r.get('thru_pct_L40', None)
                if is_valid(corsi_60) and is_valid(thru_pct):
                    base_stat = (corsi_60 * thru_pct) * (proj_toi / 60.0)
                    base_stat_source = 'corsi_per_60_L20*thru_pct_L40'
                else:
                    base_stat = r.get('SOG', None)
                    base_stat_source = 'SOG'
                if is_valid(base_stat):
                    mu_adj_calc = base_stat * opp_sog_mult * toi_factor
            elif market == 'BLOCKS':
                base_stat = r.get('BLK', None)
                base_stat_source = 'BLK'
                if is_valid(base_stat):
                    mu_adj_calc = base_stat * opp_sog_mult * toi_factor

            p_over_recalc = None
            line_int = _line_to_int(r.get('Line', None))
            if is_valid(mu_adj_calc) and is_valid(line_int):
                dist = str(r.get('distribution') or '').lower()
                if dist == 'poisson':
                    p_over_recalc = _poisson_p_over(mu_adj_calc, line_int)
                elif dist == 'negative binomial':
                    p_over_recalc = _nbinom_p_over(mu_adj_calc, r.get('alpha', None), line_int)

            return pd.Series({
                'base_stat_source': base_stat_source,
                'base_stat_value': base_stat,
                'proj_toi_used': proj_toi,
                'base_toi_used': base_toi,
                'toi_factor': toi_factor,
                'mult_opp_sog': mult_opp_sog,
                'mult_opp_g': mult_opp_g,
                'mult_goalie': mult_goalie,
                'mult_itt': mult_itt,
                'mult_b2b': mult_b2b,
                'scoring_mult': scoring_mult,
                'opp_sog_mult': opp_sog_mult,
                'mu_adj_calc': mu_adj_calc,
                'p_over_recalc': p_over_recalc
            })

        breakdown_df = proj_trace_df.apply(compute_row, axis=1)
        breakdown_df = pd.concat([proj_trace_df, breakdown_df], axis=1)

    cols = [
        'Player', 'Market', 'Line', 'Side', 'Model_Prob', 'Prob_Source',
        'Book', 'Odds', 'Implied_Prob', 'EV%', 'capture_ts_utc', 'event_start_time_utc'
    ]

    with open(report_path, 'w', encoding='ascii') as f:
        f.write('# EV Forensics Top 50 (All Markets) - Production Filters (As-Of BestBets Run)\n\n')
        f.write(f'Report timestamp (UTC): {ts}\n\n')
        f.write(f'- As-of run_start_ts: {run_start_ts.isoformat()}\n')
        f.write(f"- DFS/Pick'em excluded keywords: {', '.join(excluded_keywords)}\n")
        f.write(f'- Freshness window: {freshness_window} minutes\n')
        f.write(f'- Event grace minutes: {grace_minutes}\n')
        f.write(f'- Prob snapshot ts: {prob_snapshot_ts_str}\n\n')

        f.write('## Pipeline Trace Overview\n\n')
        f.write('Projection pipeline (raw model values):\n')
        f.write('1) Base projections from DuckDB features -> outputs/projections/BaseSingleGameProjections.csv\n')
        f.write('2) Context inputs (opponent/goalie/usage) -> outputs/projections/GameContext.csv\n')
        f.write('3) Mu + raw probabilities computed in src/nhl_bets/projections/single_game_probs.py\n')
        f.write('4) Raw + calibrated outputs -> outputs/projections/SingleGamePropProbabilities.csv\n\n')
        f.write('EV pipeline (odds + model):\n')
        f.write('1) Odds source (fact_prop_odds) -> normalized join on Player name\n')
        f.write('2) Probability selection via MARKET_POLICY and line-specific column choice\n')
        f.write('3) Side adjustment (OVER uses p_over; UNDER uses 1 - p_over)\n')
        f.write('4) Implied probability from decimal odds\n')
        f.write('5) EV% = (Model_Prob * odds_decimal) - 1\n')
        f.write('6) Freshness gating vs prob snapshot\n')
        f.write('7) Event eligibility (not started, not live, has start time)\n')
        f.write('8) EV% threshold and dedup (latest capture_ts)\n\n')

        f.write('## Step 1 - High-EV Triage (Top 50)\n\n')
        if format_df.empty:
            f.write('No eligible bets after production filters.\n\n')
        else:
            f.write(format_df[cols].to_markdown(index=False))
            f.write('\n\n')

        f.write('## Full Trace (Top 10 Bets)\n\n')
        if trace_df.empty:
            f.write('No rows available for full trace.\n\n')
        else:
            trace_cols = [
                'Player', 'Market', 'Line', 'Side', 'Book', 'source_vendor', 'capture_ts_utc',
                'event_start_time_utc', 'Odds', 'Implied_Prob', 'Source_Col', 'p_over_raw',
                'p_over_calibrated', 'p_over_selected', 'Model_Prob', 'EV%', 'mu_adj_col', 'mu_adj_value',
                'distribution', 'alpha', 'freshness_minutes', 'event_time_delta_minutes', 'is_live',
                'prob_snapshot_ts'
            ]
            for col in trace_cols:
                if col not in trace_df.columns:
                    trace_df[col] = ''
            f.write(trace_df[trace_cols].to_markdown(index=False))
            f.write('\n\n')

        f.write('## SingleGamePropProbabilities Values (Top 10 Bets)\n\n')
        if trace_df.empty:
            f.write('No rows available for SingleGamePropProbabilities examples.\n\n')
        else:
            sg_cols = [
                'Player', 'Market', 'Line', 'Source_Col', 'p_over_raw', 'p_over_calibrated',
                'p_over_selected', 'mu_adj_col', 'mu_adj_value', 'prob_snapshot_ts'
            ]
            for col in sg_cols:
                if col not in trace_df.columns:
                    trace_df[col] = ''
            f.write(trace_df[sg_cols].to_markdown(index=False))
            f.write('\n\n')

        f.write('## Math Breakdown (Top 10 Bets)\n\n')
        if breakdown_df.empty:
            f.write('Math breakdown not available (projection inputs missing).\n\n')
        else:
            breakdown_cols = [
                'Player', 'Market', 'Line', 'base_stat_source', 'base_stat_value', 'base_toi_used',
                'proj_toi_used', 'toi_factor', 'mult_opp_sog', 'mult_opp_g', 'mult_goalie', 'mult_itt',
                'mult_b2b', 'scoring_mult', 'opp_sog_mult', 'mu_adj_calc', 'mu_adj_value', 'p_over_raw',
                'p_over_recalc'
            ]
            for col in breakdown_cols:
                if col not in breakdown_df.columns:
                    breakdown_df[col] = ''
            f.write(breakdown_df[breakdown_cols].to_markdown(index=False))
            f.write('\n\n')

        f.write('## Projection Inputs (Top 10 Bets)\n\n')
        if proj_trace_df.empty:
            f.write('Projection inputs not available (BaseSingleGameProjections.csv or GameContext.csv missing).\n\n')
        else:
            proj_cols = [
                'Player', 'Team', 'OppTeam', 'GP', 'TOI', 'PPTOI',
                'G', 'A', 'PTS', 'SOG', 'BLK', 'notes',
                'opp_sa60', 'opp_xga60', 'goalie_gsax60', 'goalie_xga60',
                'implied_team_total', 'is_b2b', 'proj_toi', 'proj_pp_toi'
            ]
            for col in proj_cols:
                if col not in proj_trace_df.columns:
                    proj_trace_df[col] = ''
            f.write(proj_trace_df[proj_cols].to_markdown(index=False))
            f.write('\n\n')

        f.write('## Step 2 - Line & Side Alignment Check\n\n')
        if line_status_counts.empty:
            f.write('No rows to evaluate for line alignment.\n\n')
        else:
            f.write(line_status_counts.to_markdown(index=False))
            f.write('\n\n')

        f.write('## Step 3 - Tail Probability Sanity (Poisson Recalc)\n\n')
        if max_dev is not None and not pd.isna(max_dev):
            f.write(f"Max abs deviation vs recomputed Poisson tail: {max_dev:.6f}\n\n")
            if not max_dev_rows.empty:
                f.write(max_dev_rows.to_markdown(index=False))
                f.write('\n\n')
        else:
            f.write('No rows to evaluate for tail sanity.\n\n')

        f.write('## Step 4 - Calibration Plateau Audit\n\n')
        if not plateau_counts.empty:
            f.write(plateau_counts.head(10).to_markdown(index=False))
            f.write('\n\n')
        else:
            f.write('No calibrated probabilities found in top50 sample.\n\n')

        f.write('## Step 5 - Book Dispersion Lens\n\n')
        f.write('Outlier heuristic: book_count >= 3 and |implied_prob - median| >= 0.08\n\n')
        if not outlier_examples.empty:
            f.write(outlier_examples.to_markdown(index=False))
            f.write('\n\n')
        else:
            f.write('No outlier books flagged in top50 sample under heuristic.\n\n')

    return report_path

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
            raw_col = prob_col.replace('_calibrated', '')
            p_over_raw = row.get(raw_col) if raw_col in row else None
            if 'calibrated' in prob_col:
                p_over_calibrated = row.get(prob_col)
            else:
                alt_cal_col = f"{prob_col}_calibrated"
                p_over_calibrated = row.get(alt_cal_col) if alt_cal_col in row else None
            p_over_selected = p_over_model
            
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
                'p_over_raw': p_over_raw,
                'p_over_calibrated': p_over_calibrated,
                'p_over_selected': p_over_selected,
                # New Provenance Columns
                'source_vendor': row.get('source_vendor', 'UNKNOWN'),
                'capture_ts_utc': row.get('capture_ts_utc'),
                'event_start_time_utc': row.get('event_start_time_utc'),
                'home_team': row.get('home_team'),
                'away_team': row.get('away_team'),
                'is_live': row.get('is_live', False),
                'raw_payload_hash': row.get('raw_payload_hash', ''),
                'mu_adj_col': mu_col,
                'mu_adj_value': mu_val,
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

    # Forensics report aligned to this run (ASSISTS/POINTS only)
    try:
        report_path = write_ev_forensics_report(
            df_results=df_results,
            df_eligible=df_eligible,
            df_filtered=df_filtered,
            run_start_ts=run_start_ts,
            prob_snapshot_ts_str=prob_snapshot_ts_str,
            freshness_window=freshness_window,
            grace_minutes=grace_minutes,
            excluded_keywords=excluded_keywords
        )
        logger.info(f"Exported EV forensics report to {report_path}")
    except Exception as e:
        logger.warning(f"Could not generate EV forensics report: {e}")
    
    # Print Top 10
    print("\n--- TOP 10 MULTI-BOOK BEST BETS ---")
    if not df_ev.empty:
        print(df_ev[['Player', 'Market', 'Line', 'Side', 'Book', 'Odds', 'EV_display']].head(10).to_string(index=False))
    else:
        print("(No bets found)")

if __name__ == "__main__":
    main()

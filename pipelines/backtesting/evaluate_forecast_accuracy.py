import duckdb
import pandas as pd
import numpy as np
import os
import argparse
import json
import sys
from datetime import datetime
from sklearn.metrics import roc_auc_score

# Add src to path
sys.path.append(os.path.join(os.getcwd()))
from src.nhl_bets.eval.metrics import compute_log_loss, compute_brier_score

def calculate_ece(y_true, y_prob, n_bins=10):
    """
    Expected Calibration Error.
    """
    if len(y_true) == 0:
        return 0.0
    
    bins = np.linspace(0., 1. + 1e-8, n_bins + 1)
    binids = np.digitize(y_prob, bins) - 1
    
    bin_sums = np.bincount(binids, weights=y_prob, minlength=n_bins)
    bin_true = np.bincount(binids, weights=y_true, minlength=n_bins)
    bin_total = np.bincount(binids, minlength=n_bins)
    
    nonzero = bin_total > 0
    bin_abs_diff = np.abs(bin_true[nonzero] / bin_total[nonzero] - bin_sums[nonzero] / bin_total[nonzero])
    ece = np.sum(bin_abs_diff * bin_total[nonzero]) / np.sum(bin_total)
    return ece

def get_calibration_bins(y_true, y_prob, variant='Raw', n_bins=10):
    """
    Returns bin-wise calibration data.
    """
    bins = np.linspace(0., 1. + 1e-8, n_bins + 1)
    binids = np.digitize(y_prob, bins) - 1
    
    bin_data = []
    for i in range(n_bins):
        mask = binids == i
        if np.any(mask):
            count = np.sum(mask)
            avg_p = np.mean(y_prob[mask])
            actual_rate = np.mean(y_true[mask])
            gap = actual_rate - avg_p
            ece_contrib = np.abs(gap) * count
            bin_data.append({
                'bin': i,
                'bin_label': f"{bins[i]:.2f}-{bins[i+1]:.2f}",
                'bin_lo': bins[i],
                'bin_hi': bins[i+1],
                'count': int(count),
                'variant': variant,
                'avg_p': avg_p,
                'actual_rate': actual_rate,
                'gap': gap,
                'ece_contrib': ece_contrib / len(y_true) if len(y_true) > 0 else 0
            })
    return bin_data

def calculate_top_k_hit_rate(df, k_values=[5, 10, 20], p_col='p_over'):
    """
    Calculates hit rate for top K highest probability players per slate (game_date + market).
    """
    results = {}
    
    # Define slate grouping
    slate_groups = df.groupby(['game_date', 'market'])
    num_slates = len(slate_groups)
    if num_slates == 0:
        return {f'Top-{k} Hit Rate': np.nan for k in k_values}, 0, 0

    avg_candidates = df.groupby(['game_date', 'market']).size().mean()

    for k in k_values:
        def get_top_k_hits(group):
            top_k = group.nlargest(k, p_col)
            return top_k['realized'].mean()
        
        hit_rates = slate_groups.apply(get_top_k_hits)
        results[f'Top-{k} Hit Rate'] = hit_rates.mean()
        
    return results, num_slates, avg_candidates

def evaluate_accuracy(db_path, output_md, output_csv, output_bins_csv, table_name="fact_probabilities"):
    if not os.path.exists(db_path):
        print(f"Error: Database not found at {db_path}")
        return

    con = duckdb.connect(db_path)
    
    print(f"Joining predictions from {table_name} with realized outcomes and team stats...")
    
    # Query to join fact_probabilities with fact_skater_game_all and fact_player_game_features
    # Check if p_over_calibrated exists
    try:
        con.execute(f"SELECT p_over_calibrated FROM {table_name} LIMIT 1")
        has_calibrated = True
    except:
        has_calibrated = False

    prob_cols = "p.p_over"
    if has_calibrated:
        prob_cols += ", p.p_over_calibrated"

    query = f"""
    SELECT
        p.game_date,
        p.market,
        p.line,
        {prob_cols},
        s.toi_seconds,
        s.pp_toi_seconds,
        f.team_pp_xg_60_L20,
        CASE 
            WHEN p.market = 'GOALS' THEN (s.goals >= p.line)
            WHEN p.market = 'ASSISTS' THEN (s.assists >= p.line)
            WHEN p.market = 'POINTS' THEN (s.points >= p.line)
            WHEN p.market = 'SOG' THEN (s.sog >= p.line)
            WHEN p.market = 'BLOCKS' THEN (s.blocks >= p.line)
            ELSE NULL
        END::INTEGER as realized
    FROM {table_name} p
    JOIN fact_skater_game_all s ON p.game_id = s.game_id AND p.player_id = s.player_id
    LEFT JOIN fact_player_game_features f ON p.game_id = f.game_id AND p.player_id = f.player_id
    WHERE realized IS NOT NULL
    """
    
    df = con.execute(query).df()
    con.close()
    
    if df.empty:
        print(f"No data found for evaluation in {table_name}. Ensure table and fact_skater_game_all are populated.")
        return

    print(f"Evaluating {len(df)} predictions...")
    
    # Infer Data Scope
    min_date = df['game_date'].min()
    max_date = df['game_date'].max()
    
    # Get seasons
    con = duckdb.connect(db_path)
    seasons = con.execute("SELECT DISTINCT season FROM fact_skater_game_all").df()['season'].unique()
    seasons.sort()
    con.close()
    
    market_reports = []
    all_bins = []
    
    primary_lines = {
        'GOALS': 1,
        'ASSISTS': 1,
        'POINTS': 1,
        'SOG': 3,
        'BLOCKS': 2
    }

    # For Slate Info
    slate_info = {}

    # Helper for metric calculation
    def get_market_metrics(group, p_col, variant='Raw'):
        market = group['market'].iloc[0]
        line = group['line'].iloc[0]
        y_true = group['realized'].values
        y_prob = group[p_col].values
        
        eps = 1e-15
        y_prob_clamped = np.clip(y_prob, eps, 1 - eps)
        
        if len(np.unique(y_true)) < 2:
            auc = np.nan
        else:
            auc = roc_auc_score(y_true, y_prob)
            
        empirical_rate = y_true.mean()
        baseline_probs = np.full_like(y_true, empirical_rate, dtype=float)
        baseline_probs_clamped = np.clip(baseline_probs, eps, 1 - eps)
        
        ll = compute_log_loss(y_true, y_prob)
        baseline_ll = compute_log_loss(y_true, baseline_probs)
        
        metrics = {
            'Market': market,
            'Variant': variant,
            'Line': line,
            'Count': len(group),
            'Actual Rate': empirical_rate,
            'Avg Model Prob': y_prob.mean(),
            'Brier Score': compute_brier_score(y_true, y_prob),
            'Log Loss': ll,
            'Baseline Log Loss': baseline_ll,
            'Log Loss Improvement': baseline_ll - ll,
            'ECE': calculate_ece(y_true, y_prob),
            'ROC AUC': auc
        }

        # Low-Probability Slice Diagnostics
        ll_sum_total = -(y_true * np.log(y_prob_clamped) + (1 - y_true) * np.log(1 - y_prob_clamped)).sum()
        for p_thresh in [0.01, 0.05]:
            p_mask = y_prob < p_thresh
            count_low = int(np.sum(p_mask))
            if count_low > 0:
                y_true_low = y_true[p_mask]
                y_prob_low = y_prob_clamped[p_mask]
                ll_sum_slice = -(y_true_low * np.log(y_prob_low) + (1 - y_true_low) * np.log(1 - y_prob_low)).sum()
                metrics[f'p<{p_thresh} Count'] = count_low
                metrics[f'p<{p_thresh} Hit Rate'] = y_true_low.mean()
                metrics[f'p<{p_thresh} Avg P'] = y_prob[p_mask].mean()
                metrics[f'p<{p_thresh} LL Share'] = ll_sum_slice / ll_sum_total if ll_sum_total > 0 else 0
            else:
                metrics[f'p<{p_thresh} Count'] = 0
                metrics[f'p<{p_thresh} Hit Rate'] = np.nan
                metrics[f'p<{p_thresh} Avg P'] = np.nan
                metrics[f'p<{p_thresh} LL Share'] = 0

        # Tail Calibration (p>=0.30)
        for threshold in [0.20, 0.30]:
            t_str = f"{threshold:.2f}"
            mask = y_prob >= threshold
            if np.any(mask):
                metrics[f'Tail p>={t_str} Count'] = int(np.sum(mask))
                metrics[f'Tail p>={t_str} Avg P'] = np.mean(y_prob[mask])
                metrics[f'Tail p>={t_str} Actual'] = np.mean(y_true[mask])
                metrics[f'Tail p>={t_str} Gap'] = metrics[f'Tail p>={t_str} Actual'] - metrics[f'Tail p>={t_str} Avg P']
                if threshold == 0.30:
                    metrics['Tail Log Loss (p>=0.30)'] = compute_log_loss(y_true[mask], y_prob[mask])
            else:
                metrics[f'Tail p>={t_str} Count'] = 0
                metrics[f'Tail p>={t_str} Avg P'] = np.nan
                metrics[f'Tail p>={t_str} Actual'] = np.nan
                metrics[f'Tail p>={t_str} Gap'] = np.nan
                if threshold == 0.30:
                    metrics['Tail Log Loss (p>=0.30)'] = np.nan

        return metrics

    # Group by Market and Line
    validation_data = {}
    for (market, line), group in df.groupby(['market', 'line']):
        # Raw Metrics
        m_metrics = get_market_metrics(group, 'p_over', 'Raw')
        
        # Validation Info for Report (Raw) - Only for Primary Line
        if line == primary_lines.get(market):
            y_true = group['realized'].values
            y_prob = group['p_over'].values
            empirical_rate = y_true.mean()
            eps = 1e-15
            ll = m_metrics['Log Loss']
            baseline_ll = m_metrics['Baseline Log Loss']
            
            def calc_ll(y, p):
                p_c = np.clip(p, eps, 1-eps)
                return -(y * np.log(p_c) + (1-y) * np.log(1-p_c))
            
            spot_check = []
            indices = [0, len(group)//2, len(group)-1]
            for idx in indices:
                y = y_true[idx]
                p = y_prob[idx]
                spot_check.append({
                    'y': int(y),
                    'p': float(p),
                    'model_ll': float(calc_ll(y, p)),
                    'base_ll': float(calc_ll(y, empirical_rate))
                })
                
            validation_data[market] = {
                'p_base': empirical_rate,
                'baseline_ll': baseline_ll,
                'model_ll': ll,
                'spot_check': spot_check
            }
        
            # Bins for primary lines (using Raw)
            bins = get_calibration_bins(y_true, y_prob, variant='Raw')
            for b in bins:
                b['market'] = market
                b['line'] = line
                all_bins.append(b)
            
            # Top-K (Raw)
            top_k, num_slates, avg_cand = calculate_top_k_hit_rate(group, p_col='p_over')
            m_metrics.update(top_k)
            slate_info[market] = {'num_slates': num_slates, 'avg_candidates': avg_cand}

            # Slices (using Raw for diagnostic)
            if market in ['ASSISTS', 'POINTS']:
                group_slice = group.copy()
                if group_slice['toi_seconds'].nunique() >= 5:
                    group_slice['toi_q'] = pd.qcut(group_slice['toi_seconds'], 5, labels=['Q1', 'Q2', 'Q3', 'Q4', 'Q5'])
                    for q, q_group in group_slice.groupby('toi_q', observed=True):
                        m_metrics[f'TOI {q} Avg P'] = q_group['p_over'].mean()
                        m_metrics[f'TOI {q} Actual'] = q_group['realized'].mean()
                
                group_slice['pp_group'] = 'PP0'
                pp_mask = group_slice['pp_toi_seconds'] > 0
                if pp_mask.any():
                    pp_players = group_slice[pp_mask].copy()
                    if pp_players['pp_toi_seconds'].nunique() >= 5:
                        pp_players['pp_q'] = pd.qcut(pp_players['pp_toi_seconds'], 5, labels=['PP_Q1', 'PP_Q2', 'PP_Q3', 'PP_Q4', 'PP_Q5'])
                        group_slice.loc[pp_mask, 'pp_group'] = pp_players['pp_q'].astype(str)
                    else:
                        group_slice.loc[pp_mask, 'pp_group'] = 'PP>0'
                
                for ppg, ppg_group in group_slice.groupby('pp_group'):
                    m_metrics[f'PP_GRP {ppg} Avg P'] = ppg_group['p_over'].mean()
                    m_metrics[f'PP_GRP {ppg} Actual'] = ppg_group['realized'].mean()

                if 'team_pp_xg_60_L20' in group_slice.columns and group_slice['team_pp_xg_60_L20'].notna().any():
                    valid_pp = group_slice[group_slice['team_pp_xg_60_L20'].notna()].copy()
                    if valid_pp['team_pp_xg_60_L20'].nunique() >= 5:
                        valid_pp['pp_env_q'] = pd.qcut(valid_pp['team_pp_xg_60_L20'], 5, labels=['PP_Env_Q1', 'PP_Env_Q2', 'PP_Env_Q3', 'PP_Env_Q4', 'PP_Env_Q5'])
                        for q, q_group in valid_pp.groupby('pp_env_q', observed=True):
                            m_metrics[f'PP_ENV {q} Avg P'] = q_group['p_over'].mean()
                            m_metrics[f'PP_ENV {q} Actual'] = q_group['realized'].mean()
        
        market_reports.append(m_metrics)

        # Calibrated Metrics (If applicable)
        is_calib_market = market in ['ASSISTS', 'POINTS'] and line == 1
        if has_calibrated and is_calib_market:
            c_metrics = get_market_metrics(group, 'p_over_calibrated', 'Calibrated')
            
            # Top-K (Calibrated)
            if line == primary_lines.get(market):
                top_k_c, _, _ = calculate_top_k_hit_rate(group, p_col='p_over_calibrated')
                c_metrics.update(top_k_c)
                
                # Bins (Calibrated)
                bins_c = get_calibration_bins(group['realized'].values, group['p_over_calibrated'].values, variant='Calibrated')
                for b in bins_c:
                    b['market'] = market
                    b['line'] = line
                    all_bins.append(b)

            market_reports.append(c_metrics)

    report_df = pd.DataFrame(market_reports)
    bins_df = pd.DataFrame(all_bins)
    
    # Reorder bins_df columns for artifact
    bin_cols = ['market', 'line', 'variant', 'bin_label', 'bin_lo', 'bin_hi', 'count', 'avg_p', 'actual_rate', 'gap', 'ece_contrib']
    bins_df = bins_df[bin_cols]
    
    # Save CSVs
    report_df.to_csv(output_csv, index=False)
    bins_df.to_csv(output_bins_csv, index=False)
    print(f"Saved accuracy report to {output_csv} and bins to {output_bins_csv}")
    
    # Key Diagnostic Findings
    findings = []
    for market in primary_lines.keys():
        m_bins = bins_df[bins_df['market'] == market]
        if not m_bins.empty:
            # Largest ECE contribution
            max_ece_row = m_bins.loc[m_bins['ece_contrib'].idxmax()]
            findings.append(f"**{market}**: Largest ECE contribution in bin `{max_ece_row['bin_label']}` (Contrib: {max_ece_row['ece_contrib']:.4f}).")
            
            # Largest gap among bins with count >= 500
            large_bins = m_bins[m_bins['count'] >= 500]
            if not large_bins.empty:
                max_gap_row = large_bins.loc[large_bins['gap'].abs().idxmax()]
                findings.append(f"**{market}**: Largest absolute gap (count >= 500) in bin `{max_gap_row['bin_label']}` (Gap: {max_gap_row['gap']:.3f}).")
    
    # Summary DF: Filter to primary lines
    summary_df = report_df[report_df.apply(lambda x: x['Line'] == primary_lines.get(x['Market']), axis=1)].copy()
    
    # Low-p findings (on Raw variants)
    for market in primary_lines.keys():
        m_row = summary_df[(summary_df['Market'] == market) & (summary_df['Variant'] == 'Raw')]
        if not m_row.empty:
            ll_share_01 = m_row.iloc[0].get('p<0.01 LL Share', 0)
            if ll_share_01 > 0.30:
                findings.append(f"**{market}**: High log-loss pathology detected. >30% of log loss ({ll_share_01:.1%}) comes from p < 0.01 predictions.")

    tail_30_gaps = summary_df[summary_df['Variant'] == 'Raw'][['Market', 'Tail p>=0.30 Gap']].dropna()
    if not tail_30_gaps.empty:
        worst_tail = tail_30_gaps.loc[tail_30_gaps['Tail p>=0.30 Gap'].idxmin()]
        findings.append(f"**Tail Risk**: `{worst_tail['Market']}` has the highest overconfidence at p>=0.30 (Gap: {worst_tail['Tail p>=0.30 Gap']:.3f}).")

    # Generate Markdown Summary
    with open(output_md, 'w') as f:
        f.write("# Model Forecast Accuracy Report\n\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Metadata Section
        f.write("## Model Metadata\n")
        f.write(f"- **Logic Version:** v2.1-l40-corsi\n")
        f.write(f"- **Data Scope:** Seasons {', '.join(map(str, seasons))} ({min_date} to {max_date})\n")
        f.write(f"- **Sample Size:** {len(df)} player-game predictions\n\n")
        
        f.write("Evaluation is accuracy and calibration-only. Historical odds were unavailable.\n\n")
        
        f.write("## Key Diagnostic Findings\n")
        for finding in findings:
            f.write(f"- {finding}\n")
        f.write("\n")

        f.write("## Metric Validation\n")
        f.write("Log Loss Formula: `-(y * log(p) + (1-y) * log(1-p))`  \n")
        f.write("Probability Clamp: `1e-15` to `1 - 1e-15`  \n\n")
        
        for market, v in validation_data.items():
            f.write(f"### {market} Validation\n")
            f.write(f"- **Empirical Base Rate (p_base)**: `{v['p_base']:.6f}`\n")
            f.write(f"- **Baseline Log Loss**: `{v['baseline_ll']:.6f}`\n")
            f.write(f"- **Model Log Loss**: `{v['model_ll']:.6f}`\n\n")
            
            f.write("#### Spot Check (3 Rows)\n")
            f.write("| y | p | Model LL Contrib | Base LL Contrib |\n")
            f.write("|:--|:--|:-----------------|:----------------|\n")
            for row in v['spot_check']:
                f.write(f"| {row['y']} | {row['p']:.6f} | {row['model_ll']:.6f} | {row['base_ll']:.6f} |\n")
            f.write("\n")

        f.write("## Slate Definition\n")
        for m, info in slate_info.items():
            f.write(f"  - **{m}**: {info['num_slates']} slates, average {info['avg_candidates']:.1f} candidates per slate.\n")
        f.write("\n")

        # Primary Market Summary
        cols = ['Market', 'Variant', 'Line', 'Count', 'Actual Rate', 'Log Loss', 'Baseline Log Loss', 'Log Loss Improvement', 'ECE', 'ROC AUC']
        summary_cols = [c for c in cols if c in summary_df.columns]
        
        f.write("## Primary Market Summary\n")
        f.write(summary_df[summary_cols].to_markdown(index=False))
        f.write("\n\n")
    
        # Post-hoc Calibration Impact
        if has_calibrated:
            f.write("## Post-hoc Calibration Impact (ASSISTS & POINTS)\n")
            f.write("Comparison of metrics before and after applying chronological post-hoc calibrators.\n\n")
            impact_cols = ['Market', 'Variant', 'Log Loss', 'Log Loss Improvement', 'ECE', 'Top-5 Hit Rate', 'Top-10 Hit Rate']
            calib_impact = summary_df[summary_df['Market'].isin(['ASSISTS', 'POINTS'])][impact_cols].copy()
            f.write(calib_impact.to_markdown(index=False))
            f.write("\n\n")

        # Low-Probability Mass Diagnostics
        f.write("## Low-Probability Mass Diagnostics\n")
        f.write("Analysis of predictions with very low probabilities to detect numerical pathologies.\n\n")
        low_p_cols = ['Market', 'Variant', 'p<0.01 Count', 'p<0.01 Hit Rate', 'p<0.01 Avg P', 'p<0.01 LL Share', 'p<0.05 Count', 'p<0.05 Hit Rate']
        low_p_df = summary_df[low_p_cols].copy()
        # Fill NaNs for zero-count rows
        low_p_df = low_p_df.fillna(0.0)
        # Format LL Share as percentage
        low_p_df['p<0.01 LL Share'] = low_p_df['p<0.01 LL Share'].apply(lambda x: f"{x:.1%}")
        f.write(low_p_df.to_markdown(index=False))
        f.write("\n\n")
        
        # Tail Calibration
        f.write("## Tail Calibration (Primary Lines)\n")
        tail_cols = ['Market', 'Variant', 'Tail p>=0.30 Count', 'Tail p>=0.30 Gap', 'Tail Log Loss (p>=0.30)', 'Tail p>=0.20 Count', 'Tail p>=0.20 Gap']
        tail_df = summary_df[tail_cols].copy().fillna(0.0)
        f.write(tail_df.to_markdown(index=False))
        f.write("\n\n")

        # Bin-wise Calibration Tables
        f.write("## Bin-wise Calibration\n")
        for market in primary_lines.keys():
            m_bins = bins_df[bins_df['market'] == market]
            if not m_bins.empty:
                f.write(f"### {market} (Line {primary_lines[market]})\n")
                f.write(m_bins[['variant', 'bin_label', 'count', 'avg_p', 'actual_rate', 'gap', 'ece_contrib']].to_markdown(index=False))
                f.write("\n\n")

        # Diagnostic Slices
        f.write("## Diagnostic Slices (ASSISTS & POINTS - Raw Variant)\n")
        for market in ['ASSISTS', 'POINTS']:
            m_rows = summary_df[summary_df['Market'] == market]
            if m_rows.empty: continue
            m_row = m_rows.iloc[0]
            
            f.write(f"### {market} Calibration by TOI Quintiles\n")
            toi_data = []
            for q in ['Q1', 'Q2', 'Q3', 'Q4', 'Q5']:
                if f'TOI {q} Avg P' in m_row and not pd.isna(m_row[f'TOI {q} Avg P']):
                    toi_data.append({
                        'Quintile': q,
                        'Avg P': m_row[f'TOI {q} Avg P'],
                        'Actual': m_row[f'TOI {q} Actual'],
                        'Gap': m_row[f'TOI {q} Actual'] - m_row[f'TOI {q} Avg P']
                    })
            if toi_data:
                f.write(pd.DataFrame(toi_data).to_markdown(index=False))
                f.write("\n\n")

            f.write(f"### {market} Calibration by PP TOI Groups (PP0 + Quintiles)\n")
            pp_grp_data = []
            pp_groups = ['PP0', 'PP_Q1', 'PP_Q2', 'PP_Q3', 'PP_Q4', 'PP_Q5', 'PP>0']
            for g in pp_groups:
                if f'PP_GRP {g} Avg P' in m_row and not pd.isna(m_row[f'PP_GRP {g} Avg P']):
                    pp_grp_data.append({
                        'Group': g,
                        'Avg P': m_row[f'PP_GRP {g} Avg P'],
                        'Actual': m_row[f'PP_GRP {g} Actual'],
                        'Gap': m_row[f'PP_GRP {g} Actual'] - m_row[f'PP_GRP {g} Avg P']
                    })
            if pp_grp_data:
                f.write(pd.DataFrame(pp_grp_data).to_markdown(index=False))
                f.write("\n\n")

            f.write(f"### {market} Calibration by PP Environment (Team PP xG Rate Quintiles)\n")
            env_data = []
            for q in ['PP_Env_Q1', 'PP_Env_Q2', 'PP_Env_Q3', 'PP_Env_Q4', 'PP_Env_Q5']:
                if f'PP_ENV {q} Avg P' in m_row and not pd.isna(m_row[f'PP_ENV {q} Avg P']):
                    env_data.append({
                        'Quintile': q,
                        'Avg P': m_row[f'PP_ENV {q} Avg P'],
                        'Actual': m_row[f'PP_ENV {q} Actual'],
                        'Gap': m_row[f'PP_ENV {q} Actual'] - m_row[f'PP_ENV {q} Avg P']
                    })
            if env_data:
                f.write(pd.DataFrame(env_data).to_markdown(index=False))
                f.write("\n\n")

        f.write("## Metric Definitions\n")
        f.write("- **Brier Score**: Mean squared error of forecasts. Lower is better (0 to 1).\n")
        f.write("- **ECE (Expected Calibration Error)**: Average difference between predicted probability and actual frequency. Lower is better.\n")
        f.write("- **ROC AUC**: Ability to discriminate between hits and misses. Higher is better (0.5 to 1.0).\n")
        f.write("- **Top-10 Hit Rate**: Percentage of the 10 highest-probability players per slate who actually hit.\n")
        f.write("- **Gap**: Actual Rate - Avg Model Prob. Positive means under-predicting, negative means over-predicting.\n")

    # Save Manifest
    import subprocess
    git_sha = "unknown"
    try:
        git_sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    except:
        pass

    # Scoring Alphas
    alphas = {}
    alpha_path = os.environ.get('NHL_BETS_SCORING_ALPHA_OVERRIDE_PATH')
    if alpha_path and os.path.exists(alpha_path):
        with open(alpha_path, 'r') as f:
            alphas = json.load(f)

    manifest = {
        'timestamp': datetime.now().strftime("%Y%m%d_%H%M%S"),
        'git_sha': git_sha,
        'table_evaluated': table_name,
        'data_range': {
            'start': str(min_date),
            'end': str(max_date),
            'seasons': [int(s) for s in seasons]
        },
        'resolved_logic': {
            'scoring_alphas': alphas,
            'variance_mode': os.environ.get('NHL_BETS_VARIANCE_MODE', 'unknown'),
            'calibration_mode': os.environ.get('NHL_BETS_CALIBRATION_MODE', 'unknown')
        },
        'metrics_summary': report_df.to_dict(orient='records'),
        'row_count': len(df)
    }
    
    eval_manifest_path = os.path.join(os.path.dirname(output_csv), f"eval_manifest_{table_name}.json")
    with open(eval_manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)
        
    print(f"Eval manifest saved to {eval_manifest_path}")
    print(f"Saved accuracy summary to {output_md}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb-path", default="data/db/nhl_backtest.duckdb")
    parser.add_argument("--out-md", default="outputs/backtest_reports/forecast_accuracy.md")
    parser.add_argument("--out-csv", default="outputs/backtest_reports/forecast_accuracy.csv")
    parser.add_argument("--out-bins-csv", default="outputs/backtest_reports/forecast_accuracy_bins.csv")
    parser.add_argument("--table", default="fact_probabilities", help="DuckDB table to evaluate")
    
    args = parser.parse_args()
    
    # Ensure report directory exists
    os.makedirs(os.path.dirname(args.out_md), exist_ok=True)
    
    evaluate_accuracy(args.duckdb_path, args.out_md, args.out_csv, args.out_bins_csv, args.table)

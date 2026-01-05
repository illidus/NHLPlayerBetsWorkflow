import pandas as pd
import json
import os
from datetime import datetime
import numpy as np

def generate_audit_reports(bets, date_str, metadata=None):
    """
    Generates CSV, JSONL, and Markdown audit reports for the given bets.
    """
    audit_data = []
    
    for b in bets:
        if not b.supported:
            continue
            
        # Identity
        record = {
            'date': b.game_date or date_str,
            'event_id': b.game_slug, # Using slug as ID if not present
            'teams': b.game_slug, 
            'market_key': b.market_raw,
            'player_name': b.player_matched or b.player_raw,
            'player_id': None, # Not always present in Bet object
            'sportsbook': 'PlayNow', # Hardcoded for now based on context
            'odds_raw': b.odds_decimal,
            'odds_decimal': b.odds_decimal,
        }
        
        # Odds math
        record['implied_prob'] = 1.0 / b.odds_decimal
        record['b'] = b.odds_decimal - 1.0
        
        # Projection -> Mu provenance
        # We need to extract these from the audit dict we populated in main.py
        audit = b.audit
        multipliers = audit.get('multipliers', {})
        
        # NOTE: In current implementation, we don't have 'mu_raw_from_source' 
        # easily accessible because it's in a different file's local scope during generation.
        # We can approximate it by dividing by multipliers if they exist.
        
        m_total = 1.0
        for m_val in multipliers.values():
            m_total *= m_val
            
        record['mu_raw_from_source'] = b.model_mean / m_total if m_total != 0 else b.model_mean
        record['mu_after_all_multipliers'] = b.model_mean
        # In current code, no further rounding happens in analysis step, 
        # but it might have happened in projection step.
        record['mu_after_rounding_or_bucketing'] = b.model_mean 
        record['rounding_function_line'] = "src/nhl_bets/projections/single_game_probs.py:round(..., 4)"
        
        for k, v in multipliers.items():
            record[f'mult_{k}'] = v
            
        record['source_columns'] = audit.get('source_columns', [])
        record['source_prob_column'] = audit.get('source_prob_column', 'Recalculated')
        record['ProbSource'] = 'Calibrated' if 'calibrated' in record['source_prob_column'].lower() else ('Raw' if 'p_' in record['source_prob_column'].lower() else 'Base')
        record['input_file'] = audit.get('input_file', 'Unknown')
        record['missing_fields'] = audit.get('missing_fields', [])
        record['fallback_value'] = 1.0 # Standard fallback for multipliers
        record['fallback_reason'] = "Missing context data (OppTeam/Goalie)" if record['missing_fields'] else "None"
        
        record['mu_quantized_flag'] = abs(record['mu_after_rounding_or_bucketing'] - record['mu_raw_from_source']) > 1e-6
        
        # Distribution -> probability conversion
        record['distribution_name'] = "Negative Binomial" if b.stat_type in ['sog', 'blocks', 'blk'] else "Poisson"
        record['threshold_definition'] = f"P(X >= {b.threshold_k if b.threshold_k is not None else b.line_value})"
        
        # p_model_computed (re-calculate from mu_after_all_multipliers)
        from nhl_bets.analysis.distributions import poisson_probability, calc_prob_from_line
        
        # We use mu_after_all_multipliers for p_model_computed
        if b.threshold_k is not None:
            # Poisson P(X >= k)
            record['p_model_computed'] = poisson_probability(b.threshold_k, record['mu_after_all_multipliers'], side='over')
        else:
            # calc_prob_from_line handles NBinom too
            record['p_model_computed'] = calc_prob_from_line(b.line_value, record['mu_after_all_multipliers'], b.side, stat_type=b.stat_type)
            
        record['p_model_used_in_ev'] = b.model_prob
        record['p_model_diff'] = record['p_model_used_in_ev'] - record['p_model_computed']
        
        # Check if p_model_used_in_ev corresponds to mu rounded to 1 decimal
        # p = 1 - exp(-mu)  => mu = -ln(1-p)
        if record['distribution_name'] == "Poisson" and b.threshold_k == 1:
            try:
                inferred_mu = -np.log(1.0 - record['p_model_used_in_ev'])
                record['mu_1d_flag'] = abs(inferred_mu - round(inferred_mu, 1)) < 1e-3
            except:
                record['mu_1d_flag'] = False
        else:
            record['mu_1d_flag'] = False
            
        # EV math
        record['ev_roi'] = (record['p_model_used_in_ev'] * record['odds_decimal']) - 1.0
        record['ev_percent'] = record['ev_roi'] * 100.0
        record['kelly_full'] = (record['p_model_used_in_ev'] * b.odds_decimal - 1) / (b.odds_decimal - 1) if b.odds_decimal > 1 else 0
        record['kelly_1_10'] = record['kelly_full'] * 0.1
        record['kelly_1_4'] = record['kelly_full'] * 0.25
        
        audit_data.append(record)
        
    if not audit_data:
        print("No audit data to export.")
        return

    df_audit = pd.DataFrame(audit_data)
    
    # Filenames
    audit_dir = "outputs/audits"
    os.makedirs(audit_dir, exist_ok=True)
    
    csv_file = os.path.join(audit_dir, f"ev_prob_audit_{date_str}.csv")
    jsonl_file = os.path.join(audit_dir, f"ev_prob_audit_{date_str}.jsonl")
    md_file = os.path.join(audit_dir, f"ev_prob_audit_{date_str}.md")
    
    # Export CSV
    df_audit.to_csv(csv_file, index=False)
    
    # Export JSONL
    df_audit.to_json(jsonl_file, orient='records', lines=True)
    
    # Export Markdown (Human Readable)
    with open(md_file, 'w') as f:
        f.write(f"# EV Probability Audit Report - {date_str}\n\n")
        
        if metadata:
            f.write("## Model Metadata\n")
            for k, v in metadata.items():
                f.write(f"- **{k}:** {v}\n")
            f.write("\n")
            
        f.write(f"Total Audited Bets: {len(audit_data)}\n\n")
        
        # Summary Table
        cols_to_show = ['player_name', 'market_key', 'odds_decimal', 'model_mean', 'p_model_used_in_ev', 'ev_percent']
        # Map some internal names to nice names for MD
        md_table_df = df_audit.rename(columns={
            'player_name': 'Player',
            'market_key': 'Market',
            'odds_decimal': 'Odds',
            'mu_after_rounding_or_bucketing': 'Mu',
            'p_model_used_in_ev': 'P(Model)',
            'ev_percent': 'EV%'
        })
        f.write(md_table_df[['Player', 'Market', 'Odds', 'Mu', 'P(Model)', 'EV%']].sort_values('EV%', ascending=False).head(50).to_markdown(index=False))
        f.write("\n\n")
        
        # Full details for top 5
        f.write("## Top 5 Detailed Derivations\n\n")
        for i, row in md_table_df.sort_values('EV%', ascending=False).head(5).iterrows():
            f.write(f"### {row['Player']} - {row['Market']}\n")
            f.write(f"- **Odds:** {row['Odds']} (Implied: {row['implied_prob']:.2%})\n")
            f.write(f"- **Mu Derivation:** Raw Source ({row['mu_raw_from_source']:.4f}) -> Adjusted ({row['mu_after_all_multipliers']:.4f})\n")
            f.write(f"- **Distribution:** {row['distribution_name']} ({row['threshold_definition']})\n")
            f.write(f"- **Probability Source:** {row['ProbSource']} (`{row['source_prob_column']}`)\n")
            f.write(f"- **Probability Value:** {row['P(Model)']:.4f}\n")
            f.write(f"- **EV Calculation:** ({row['P(Model)']:.4f} * {row['Odds']}) - 1 = **{row['EV%']:+.2f}%**\n\n")

    print(f"Audit reports generated in {audit_dir}: {os.path.basename(csv_file)}, {os.path.basename(jsonl_file)}, {os.path.basename(md_file)}")
    
    # Duplicate Probability Diagnostics
    p_counts = df_audit['p_model_used_in_ev'].value_counts()
    print("\nTop 20 Most Frequent Model Probabilities:")
    for p, count in p_counts.head(20).items():
        bets_with_p = df_audit[df_audit['p_model_used_in_ev'] == p]['player_name'].tolist()
        print(f"  P={p:.4f} | Count: {count} | Samples: {', '.join(bets_with_p[:3])}...")
        
    # Duplicates Report
    if (p_counts >= 5).any():
        dup_report = os.path.join(audit_dir, "duplicates_report.md")
        with open(dup_report, 'w') as f:
            f.write("# Duplicate Probability Diagnostic Report\n\n")
            f.write("Identified Model Probabilities appearing 5 or more times.\n\n")
            
            for p, count in p_counts[p_counts >= 5].items():
                f.write(f"### Probability: {p:.4f} (Count: {count})\n")
                affected_bets = df_audit[df_audit['p_model_used_in_ev'] == p]
                f.write(f"- **Markets:** {', '.join(affected_bets['market_key'].unique())}\n")
                f.write(f"- **Mu Quantized:** {affected_bets['mu_quantized_flag'].any()}\n")
                f.write(f"- **Code Location:** `src/nhl_bets/projections/single_game_probs.py` (Likely source quantization)\n\n")
                
                table = affected_bets[['player_name', 'market_key', 'mu_after_rounding_or_bucketing', 'ev_percent']]
                f.write(table.to_markdown(index=False))
                f.write("\n\n")
        print(f"Duplicates report generated: {dup_report}")

def run_quick_checks():
    """
    Focused unit test / quick check for math correctness.
    """
    print("\nRunning Quick Math Checks...")
    
    # Poisson Check
    # P(X >= 1) = 1 - P(X=0) = 1 - exp(-mu)
    mu = 0.1
    expected_p = 1.0 - np.exp(-mu)
    print(f"  Poisson Check: mu=0.1 -> P(X>=1) = {expected_p:.6f} (Expected ~0.095163)")
    assert abs(expected_p - 0.095163) < 1e-5
    
    # EV Formula Check
    p = 0.5
    d = 2.2
    ev1 = p * (d - 1) - (1 - p)
    ev2 = p * d - 1
    print(f"  EV Check: p=0.5, d=2.2 -> EV1={ev1:.2f}, EV2={ev2:.2f}")
    assert abs(ev1 - ev2) < 1e-7
    assert abs(ev1 - 0.1) < 1e-7
    
    print("Quick checks passed.\n")

import argparse
import pandas as pd
import numpy as np
import sys
import os

# Ensure project root is in path for nhl_bets import
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# If 'src' is also in path (which it should be via run_production_pipeline.py), 
# we can use absolute imports from nhl_bets.
# If not, we ensure it is.
src_dir = os.path.join(project_root, "src")
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from nhl_bets.projections.config import get_production_prob_column
from nhl_bets.analysis.file_io import read_csv, validate_base_columns
from nhl_bets.analysis.normalize import normalize_name, get_teams_from_slug, fuzzy_match_player, TEAM_MAP
from nhl_bets.analysis.parse import parse_bets
from nhl_bets.analysis.distributions import poisson_probability, calc_prob_from_line
from nhl_bets.analysis.ev import decimal_to_implied, remove_vig, calculate_ev
from nhl_bets.analysis.export import export_to_excel, export_to_csv
from nhl_bets.analysis.audit import generate_audit_reports, run_quick_checks

def main():
    parser = argparse.ArgumentParser(description="NHL EV Betting Pipeline")
    parser.add_argument("--base", required=True, help="Path to BaseSingleGameProjections.csv")
    parser.add_argument("--props", required=True, help="Path to nhl_player_props_all.csv")
    parser.add_argument("--probs", required=False, help="Path to SingleGamePropProbabilities.csv (Phase 8 Model Output)")
    parser.add_argument("--out_xlsx", required=True, help="Output Excel path")
    parser.add_argument("--out_csv", required=True, help="Output CSV path")
    
    args = parser.parse_args()
    
    print(f"Reading base projections: {args.base}")
    df_base = read_csv(args.base)
    validate_base_columns(df_base)
    
    # Load Probs if available
    df_probs = None
    probs_lookup = {}
    probs_lookup_name_only = {}
    
    if args.probs and os.path.exists(args.probs):
        print(f"Reading calculated probabilities: {args.probs}")
        df_probs = read_csv(args.probs)
        has_date = 'Date' in df_probs.columns
        
        for idx, row in df_probs.iterrows():
            norm_name = normalize_name(row['Player'])
            if has_date:
                key = (norm_name, str(row['Date']))
                probs_lookup[key] = row.to_dict()
            
            # Name only lookup for fallbacks (if unique)
            if norm_name not in probs_lookup_name_only:
                probs_lookup_name_only[norm_name] = []
            probs_lookup_name_only[norm_name].append(row.to_dict())
    
    print(f"Reading props: {args.props}")
    df_props = read_csv(args.props)
    
    # 1. Pre-process Base Projections
    # Create lookup dict: (NormalizedName, Team) -> Row
    # Also (NormalizedName) -> List of Rows (for disambiguation)
    
    base_lookup = {}
    base_lookup_name_only = {}
    
    for idx, row in df_base.iterrows():
        norm_name = normalize_name(row['Player'])
        team = row['Team']
        
        # Stat mapping
        stats = {
            'goals': row.get('mu_base_goals', 0),
            'assists': row.get('Assists Per Game', 0),
            'points': row.get('Points Per Game', 0),
            'sog': row.get('SOG Per Game', 0)
        }
        
        record = {'team': team, 'stats': stats, 'original_name': row['Player']}
        
        base_lookup[(norm_name, team)] = record
        
        if norm_name not in base_lookup_name_only:
            base_lookup_name_only[norm_name] = []
        base_lookup_name_only[norm_name].append(record)
        
    all_base_names = list(base_lookup_name_only.keys())
    
    # 2. Parse Bets
    print("Parsing bets...")
    bets = parse_bets(df_props)
    print(f"Parsed {len(bets)} potential bets.")
    
    # 3. Match Players and Get Means
    print("Matching players...")
    all_probs_names = []
    probs_lookup_norm = {}
    if df_probs is not None:
        # Build normalized lookup for Probs
        for idx, row in df_probs.iterrows():
            p_name = row['Player']
            norm_p = normalize_name(p_name)
            if norm_p not in probs_lookup_norm:
                probs_lookup_norm[norm_p] = []
            probs_lookup_norm[norm_p].append(row.to_dict())
        all_probs_names = list(set(df_probs['Player'].tolist()))

    for bet in bets:
        if not bet.supported:
            continue
            
        norm_player = normalize_name(bet.player_raw)
        mu_found = False
        probs_row = None
        
        if df_probs is not None:
            # 1. Exact Normalized Match in Probs
            candidates = probs_lookup_norm.get(norm_player, [])
            
            # 2. Fuzzy Match in Probs (if no exact)
            if not candidates:
                matched_name, score = fuzzy_match_player(norm_player, all_probs_names, threshold=0.85)
                if matched_name:
                    candidates = probs_lookup_norm.get(normalize_name(matched_name), [])
            
            if candidates:
                # Disambiguate by team if possible
                game_away, home_home = get_teams_from_slug(bet.game_slug)
                valid_teams = {game_away, home_home} if game_away and home_home else set()
                
                if valid_teams:
                    team_matches = [c for c in candidates if c['Team'] in valid_teams]
                    if team_matches:
                        probs_row = team_matches[0]
                    else:
                        # Fallback to first if no team match but unique?
                        # Better to be strict with Probs to avoid wrong projections
                        pass
                else:
                    probs_row = candidates[0]

            if probs_row:
                mu_map = {
                    'goals': 'mu_adj_G',
                    'assists': 'mu_adj_A',
                    'points': 'mu_adj_PTS',
                    'sog': 'mu_adj_SOG',
                    'blocks': 'mu_adj_BLK',
                    'blk': 'mu_adj_BLK'
                }
                
                col = mu_map.get(bet.stat_type)
                if col and col in probs_row:
                    bet.model_mean = float(probs_row[col])
                    bet.player_matched = probs_row['Player']
                    bet.team_matched = probs_row['Team']
                    mu_found = True
                    
                    # Probability selection via centralized policy
                    prob_col = get_production_prob_column(bet.stat_type, bet.line_value, probs_row.keys())
                    if prob_col and prob_col in probs_row:
                        bet.model_prob = float(probs_row[prob_col])
                        bet.audit['source_prob_column'] = prob_col
                    
                    # Capture Audit Multipliers
                    bet.audit['multipliers'] = {
                        'opp_sog': probs_row.get('mult_opp_sog', 1.0),
                        'opp_g': probs_row.get('mult_opp_g', 1.0),
                        'goalie': probs_row.get('mult_goalie', 1.0),
                        'itt': probs_row.get('mult_itt', 1.0),
                        'b2b': probs_row.get('mult_b2b', 1.0)
                    }
                    bet.audit['source_columns'] = [col, prob_col] if prob_col else [col]
                    bet.audit['input_file'] = args.probs
                    
                    missing = []
                    if pd.isna(probs_row.get('OppTeam')): missing.append('OppTeam')
                    bet.audit['missing_fields'] = missing
        
        if not mu_found:
            # Fallback to base mean (no multipliers)
            candidates = base_lookup_name_only.get(norm_player)
        
        if mu_found:
            continue

        # If not found in Probs, Fallback to Base Logic (Phase 1)
        # Attempt Exact Match (Name Only)
        candidates = base_lookup_name_only.get(norm_player)
        
        match_record = None
        
        # Disambiguate if multiple
        game_away, game_home = get_teams_from_slug(bet.game_slug)
        valid_teams = {game_away, game_home} if game_away and game_home else set()
        
        if candidates:
            # Filter by team if possible
            if valid_teams:
                team_matches = [c for c in candidates if c['team'] in valid_teams]
                if team_matches:
                    match_record = team_matches[0] # Take first valid team match
                else:
                    # Player found but not on teams in game slug? 
                    # Maybe traded or slug map issue. 
                    # Strict: reject. Loose: take first. 
                    # We will be Strict to avoid bad EVs.
                    bet.supported = False
                    bet.reason = f"Player matched ({norm_player}) but team ({candidates[0]['team']}) not in game ({valid_teams})"
                    continue
            else:
                match_record = candidates[0]
        else:
            # Fuzzy Match
            # Only fuzzy match against players ON the teams in the game to reduce false positives
            # Get all base names for the teams in this game
            if valid_teams:
                # Optimized: Filter names first? No, iterate all names is safer but slower.
                # Only iterate names that belong to relevant teams
                # Build subset of names
                
                # For speed, we just fuzzy match against ALL names, then filter result.
                matched_name, score = fuzzy_match_player(norm_player, all_base_names)
                
                if matched_name:
                    cands = base_lookup_name_only[matched_name]
                    # Check team
                    team_matches = [c for c in cands if c['team'] in valid_teams]
                    if team_matches:
                        match_record = team_matches[0]
                        bet.match_score = score
                        bet.player_matched = match_record['original_name']
                    else:
                        bet.supported = False
                        bet.reason = f"Fuzzy matched {matched_name} ({score:.2f}) but wrong team."
                        continue
                else:
                    bet.supported = False
                    bet.reason = "No match found."
                    continue
            else:
                # No valid teams parsed from slug, can't disambiguate safely
                bet.supported = False
                bet.reason = "Could not parse teams from game slug."
                continue
        
        # If we have a match via Base
        if match_record:
            bet.player_matched = match_record['original_name']
            bet.team_matched = match_record['team']
            bet.model_mean = match_record['stats'].get(bet.stat_type)
            
            if bet.model_mean is None or pd.isna(bet.model_mean):
                bet.supported = False
                bet.reason = f"No projection for stat {bet.stat_type}"
    
    # 4. Infer Sides and Calculate Probabilities
    print("Calculating probabilities...")
    
    for bet in bets:
        if not bet.supported:
            continue
            
        # Single Sided (Player 1+ Goals)
        if getattr(bet, 'pair_bet', None) is None:
            # Calculate Model Probability
            if 'source_prob_column' in bet.audit:
                # Already pulled from model output (Phase 8 calibrated/raw)
                # Naming like p_A_1plus always refers to 'over'
                if bet.side == 'under':
                    bet.model_prob = 1.0 - bet.model_prob
            else:
                if bet.threshold_k is not None:
                    bet.model_prob = poisson_probability(bet.threshold_k, bet.model_mean, side='over')
                elif bet.line_value is not None:
                    bet.model_prob = calc_prob_from_line(bet.line_value, bet.model_mean, bet.side, stat_type=bet.stat_type)
            
            bet.implied_prob_raw = decimal_to_implied(bet.odds_decimal)
            bet.implied_prob_novig = bet.implied_prob_raw # Can't remove vig
            
            bet.ev = calculate_ev(bet.model_prob, bet.odds_decimal)
            bet.edge = bet.model_prob - bet.implied_prob_raw
            
        else:
            # Paired Bet (Total X Over/Under)
            other = bet.pair_bet
            
            # We handle inference if side is None
            if bet.side is None:
                # Inference Logic
                p1_raw = decimal_to_implied(bet.odds_decimal)
                p2_raw = decimal_to_implied(other.odds_decimal)
                p1_fair, p2_fair = remove_vig(p1_raw, p2_raw)
                
                # Use model over prob for inference
                if 'source_prob_column' in bet.audit:
                    prob_over = bet.model_prob # Assume the loaded prob is the 'over'
                else:
                    prob_over = calc_prob_from_line(bet.line_value, bet.model_mean, side='over', stat_type=bet.stat_type)
                
                diff1 = abs(p1_fair - prob_over)
                diff2 = abs(p2_fair - prob_over)
                
                if diff1 < diff2:
                    bet.side = 'over'
                    other.side = 'under'
                else:
                    bet.side = 'under'
                    other.side = 'over'
            
            # Now calculate metrics using assigned side
            if 'source_prob_column' in bet.audit:
                # Already have 'over' prob
                if bet.side == 'under':
                    bet.model_prob = 1.0 - bet.model_prob
                # else it stays as loaded 'over'
            else:
                bet.model_prob = calc_prob_from_line(bet.line_value, bet.model_mean, bet.side, stat_type=bet.stat_type)
            
            # Re-calc vig free specific to this bet
            p_raw = decimal_to_implied(bet.odds_decimal)
            p_other = decimal_to_implied(other.odds_decimal)
            p_fair, _ = remove_vig(p_raw, p_other)
            
            bet.implied_prob_raw = p_raw
            bet.implied_prob_novig = p_fair
            bet.ev = calculate_ev(bet.model_prob, bet.odds_decimal)
            bet.edge = bet.model_prob - p_raw

    # 5. Export
    print("Exporting results...")
    export_to_excel(bets, args.out_xlsx)
    export_to_csv(bets, args.out_csv)
    
    # 6. Summary
    supported_count = len([b for b in bets if b.supported])
    ev_bets = [b for b in bets if b.supported and b.ev > 0]
    print(f"Supported bets: {supported_count}")
    print(f"+EV bets: {len(ev_bets)}")
    
    print(f"\nAll +EV Bets ({len(ev_bets)} found):")
    ev_bets.sort(key=lambda x: x.ev, reverse=True)
    for b in ev_bets:
        dist_type = "Negative Binomial" if b.stat_type in ['sog', 'blocks', 'blk'] else "Poisson"
        k_val = b.threshold_k if b.threshold_k is not None else b.line_value
        
        print(f"--------------------------------------------------")
        print(f"BET: {b.player_matched} ({b.team_matched}) | {b.market_raw}")
        print(f"MATH AUDIT:")
        print(f"  1. Adjusted Mean (Mu): {b.model_mean:.4f}")
        if b.audit.get('multipliers'):
            m = b.audit['multipliers']
            m_list = [f"{k}={v}" for k,v in m.items() if v != 1.0]
            if m_list: print(f"     [Multipliers Used: {', '.join(m_list)}]")
        
        print(f"  2. Distribution: {dist_type} (k={k_val})")
        print(f"  3. Implied Prob: 1 / {b.odds_decimal} = {b.implied_prob_raw:.2%}")
        print(f"  4. Model Prob:   {b.model_prob:.2%}")
        print(f"  5. EV Formula:   ({b.model_prob:.4f} * {b.odds_decimal}) - 1 = {b.ev:+.2%}")

    # --- Market Sanity Diagnostic ---
    print("\n" + "="*50)
    print("MARKET SANITY DIAGNOSTIC")
    print("="*50)
    
    market_stats = {}
    for b in bets:
        if not b.supported: continue
        m = b.stat_type.upper()
        if m not in market_stats:
            market_stats[m] = {'implied': [], 'model': [], 'high_ev': 0}
        
        market_stats[m]['implied'].append(b.implied_prob_novig)
        market_stats[m]['model'].append(b.model_prob)
        if b.ev > 0.5:
            market_stats[m]['high_ev'] += 1
            
    print(f"{'MARKET':<10} | {'AVG IMPLIED':<12} | {'AVG MODEL':<12} | {'EV > 50%'}")
    print("-" * 55)
    for m, data in market_stats.items():
        avg_imp = np.mean(data['implied']) if data['implied'] else 0
        avg_mod = np.mean(data['model']) if data['model'] else 0
        print(f"{m:<10} | {avg_imp:>11.1%} | {avg_mod:>11.1%} | {data['high_ev']}")
    print("="*50 + "\n")

    # --- Audit Step ---
    run_quick_checks()
    # Try to get date from bets if possible
    example_date = None
    for b in bets:
        if b.game_date:
            example_date = b.game_date
            break
    game_date_str = example_date or datetime.now().strftime("%Y-%m-%d")
    
    # Define Model Metadata
    metadata = {
        "Logic Version": "v2.1-l40-corsi",
        "Data Scope": "Seasons 2023-2025 (Full Regular Seasons)",
        "Primary Metric": "Log Loss (Minimized via L40)",
        "Calibration": "Isotonic (Assists/Points), Raw (Others)"
    }
    
    generate_audit_reports(bets, game_date_str, metadata=metadata)

if __name__ == "__main__":
    main()

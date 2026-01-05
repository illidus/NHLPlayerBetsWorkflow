import pandas as pd

target_players = ["Artemi Panarin", "Clayton Keller", "Mikhail Sergachev", "Moritz Seider", "Adam Fox"]

# Check Base Projections
base_df = pd.read_csv('outputs/projections/BaseSingleGameProjections.csv')
base_subset = base_df[base_df['Player'].isin(target_players)]

cols = [
    'Player', 'Team', 'GP', 'TOI', 'Assists Per Game', 'Points Per Game',
    'ev_ast_60_L20', 'pp_ast_60_L20', 'ev_pts_60_L20', 'pp_pts_60_L20'
]
print("--- BASE PROJECTION INPUTS (L10/L20) ---")
print(base_subset[cols].to_string(index=False))

# Check Audit for Calibration Policy
audit_df = pd.read_csv('ev_prob_audit_2026-01-06.csv')
audit_subset = audit_df[audit_df['player_name'].isin(target_players)]

audit_cols = [
    'player_name', 'market_key', 'odds_decimal', 'implied_prob', 
    'p_model_used_in_ev', 'ev_percent', 'ProbSource', 'source_prob_column'
]
print("\n--- AUDIT TRACE (CALIBRATION CHECK) ---")
print(audit_subset[audit_subset['market_key'].str.contains('Assists|Points')][audit_cols].head(10).to_string(index=False))

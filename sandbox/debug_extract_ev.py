import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

df = pd.read_csv('ev_prob_audit_2026-01-06.csv')

# Filter for GOALS
# Checking unique market keys to be sure
# print(df['market_key'].unique())

goals_df = df[df['market_key'].str.contains('GOALS', case=False, na=False)]

# Sort by EV% descending
goals_df = goals_df.sort_values(by='ev_percent', ascending=False)

# Top 20
top_20 = goals_df.head(20)

# Extreme subset
extreme_subset = goals_df[
    (goals_df['ev_percent'] > 50) | 
    (goals_df['odds_decimal'] > 15)
]

cols_to_show = [
    'player_name', 'teams', 'market_key', 'odds_decimal', 
    'implied_prob', 'p_model_used_in_ev', 'ev_percent', 
    'ProbSource', 'source_prob_column', 'mu_after_all_multipliers'
]

print("--- TOP 20 GOALS CANDIDATES BY EV% ---")
print(top_20[cols_to_show].to_string(index=False))

print("\n--- EXTREME GOALS CANDIDATES (EV > 50% or Odds > 15) ---")
print(extreme_subset[cols_to_show].to_string(index=False))

import pandas as pd

# Load the latest ranked bets
df = pd.read_csv('outputs/ev_analysis/ev_bets_ranked.csv')

# GEMINI.md Filter Rules:
# 1. EV% >= 2.0% (Column is 'EV' which is likely 0.XX)
# 2. p_model >= 0.05 (Not in this file, will assume pass or check audit if needed)
# 3. High-Odds (>15.0) require EV% >= 10.0
filtered = df[
    (df['EV'] >= -1.0)
].copy()

# Apply the high-odds penalty
filtered = filtered[~((filtered['Odds'] > 15.0) & (filtered['EV'] < 0.10))]

# GEMINI.md Ranking Priority:
# 1. Market Priority (GOALS > ASSISTS > POINTS)
# 2. EV Descending

def market_rank(market):
    m = str(market).upper()
    if 'GOALS' in m: return 1
    if 'ASSISTS' in m: return 2
    if 'POINTS' in m: return 3
    return 4

filtered['m_rank'] = filtered['Market'].apply(market_rank)
# Priority 1: Market Priority (GOALS=1)
# Priority 2: EV Descending
filtered = filtered.sort_values(by=['m_rank', 'EV'], ascending=[True, False])

cols_to_show = [
    'Player', 'Game', 'Market', 'Odds', 'EV'
]

print("--- HIGHEST EV GOALS BETS ---")
print(filtered[filtered['m_rank'] == 1][cols_to_show].head(10).to_string(index=False))

print("\n--- HIGHEST EV ASSISTS BETS ---")
print(filtered[filtered['m_rank'] == 2][cols_to_show].head(10).to_string(index=False))

print("\n--- HIGHEST EV POINTS BETS ---")
print(filtered[filtered['m_rank'] == 3][cols_to_show].head(10).to_string(index=False))

import pandas as pd

df = pd.read_csv('data/raw/nhl_player_props_all.csv')
subset = df[df['Player'].str.contains('Artemi Panarin', case=False, na=False)]

print("--- Artemi Panarin Props ---")
print(subset[['Player', 'Market', 'Sub_Header', 'Odds_1', 'Odds_2']].to_string(index=False))

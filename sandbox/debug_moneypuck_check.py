import pandas as pd
import os

base_path = 'data/raw/moneypuck/teamPlayerGameByGame/2025/regular/skaters'
teams = ['MIN', 'SJS', 'LAK', 'DET', 'OTT', 'SEA']
target_players = ["Nico Sturm", "Tyler Kleven", "Emmitt Finnie", "Ben Meyers"]

for team in teams:
    file_path = os.path.join(base_path, f"{team}.csv")
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path)
            if 'name' in df.columns:
                 for p in target_players:
                     matches = df[df['name'].str.contains(p, case=False, na=False)]
                     if not matches.empty:
                         print(f"Found {p} in {team}.csv")
                         cols_to_show = ['name', 'gameId', 'gameDate', 'situation']
                         available_cols = [c for c in cols_to_show if c in df.columns]
                         print(matches[available_cols].head(5).to_string())
                         print(f"Total Games in {team}: {len(matches)}")
        except Exception as e:
            print(f"Error reading {team}.csv: {e}")

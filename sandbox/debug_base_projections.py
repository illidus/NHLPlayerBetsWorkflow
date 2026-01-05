import pandas as pd

file_path = 'outputs/projections/BaseSingleGameProjections.csv'
try:
    df = pd.read_csv(file_path)
    target_players = ["Nico Sturm", "Tyler Kleven", "Ryan Winterton", "Emmitt Finnie", "Ben Meyers"]
    
    # Filter for target players
    subset = df[df['Player'].isin(target_players)]
    
    # Check available columns before selecting
    cols = ['Player', 'Team', 'mu_base_goals', 'GP', 'TOI']
    existing_cols = [c for c in cols if c in df.columns]
    
    print(subset[existing_cols].to_string())
except Exception as e:
    print(f"Error reading file: {e}")

import pandas as pd
import os

def read_csv(file_path):
    """Reads a CSV file into a pandas DataFrame."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    return pd.read_csv(file_path)

def validate_base_columns(df):
    """Validates that the base projections file has required columns."""
    required = ['Player', 'Team', 'mu_base_goals', 'Assists Per Game', 
                'Points Per Game', 'SOG Per Game']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Base projections missing columns: {missing}")
    return True

def validate_props_columns(df):
    """Validates that the props file has required columns."""
    required = ['Game', 'Market', 'Player', 'Odds_1', 'Raw_Line']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Props file missing columns: {missing}")
    return True

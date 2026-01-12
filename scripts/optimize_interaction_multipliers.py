import duckdb
import pandas as pd
import numpy as np
import os
import json
import argparse
from scipy.optimize import minimize
import sys

# Add project root and src to path for imports
# current_dir = os.path.dirname(os.path.abspath(__file__))
# project_root = os.path.dirname(os.path.dirname(current_dir))
project_root = os.getcwd()
src_dir = os.path.join(project_root, 'src')
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Config
DB_PATH = 'data/db/nhl_backtest.duckdb'
OUTPUT_DIR = 'outputs/beta_optimization'
CONFIG_FILE = 'outputs/beta_optimization/optimized_interactions.json'

def load_data(table="fact_probabilities_interaction_v1"):
    con = duckdb.connect(DB_PATH)
    query = f"""
    SELECT 
        matchup_type,
        market,
        p_over as p_raw, -- This includes current interaction multiplier
        mu_used,
        -- Need Outcome
        f.goals, f.assists, f.points, f.sog, f.blocks,
        p.line
    FROM {table} p
    JOIN fact_player_game_features f ON p.player_id = f.player_id AND p.game_id = f.game_id
    WHERE market IN ('GOALS', 'ASSISTS', 'POINTS')
      AND matchup_type != 'none'
    """
    df = con.execute(query).df()
    
    # Add y_true
    df['y_true'] = np.nan
    for m in ['GOALS', 'ASSISTS', 'POINTS']:
        mask = df['market'] == m
        col = m.lower()
        df.loc[mask, 'y_true'] = (df.loc[mask, col] > df.loc[mask, 'line']).astype(int)
    
    return df.dropna(subset=['y_true'])

def objective(x, df_slice, current_mult):
    """
    x: new multiplier
    """
    new_mult = x[0]
    
    # Adjust mu: mu_new = mu_old / current_mult * new_mult
    # Approximate p_new from mu_new
    # Since we don't have Poisson distribution readily available vectorized here without importing heavy logic,
    # we can use a linear approximation for small changes in p around mu, or just re-calculate Poisson.
    # Poisson P(X > line) = 1 - CDF(line, mu)
    
    from scipy.stats import poisson
    
    mu_new = df_slice['mu_used'] / current_mult * new_mult
    line = df_slice['line']
    
    # Calculate P(X > line) = 1 - P(X <= line)
    p_new = 1.0 - poisson.cdf(line, mu_new)
    
    # Clip
    p_new = np.clip(p_new, 1e-15, 1 - 1e-15)
    
    # Log Loss
    y = df_slice['y_true'].values
    ll = -np.mean(y * np.log(p_new) + (1 - y) * np.log(1 - p_new))
    return ll

def optimize_multipliers(df):
    results = {}
    
    # Group by Matchup
    matchups = df['matchup_type'].unique()
    
    from nhl_bets.projections.config import INTERACTION_MULTIPLIERS
    
    print(f"Optimizing {len(matchups)} matchups...")
    
    for m in matchups:
        if m not in INTERACTION_MULTIPLIERS:
            current_mult = 1.0
        else:
            current_mult = INTERACTION_MULTIPLIERS[m]
            
        sub = df[df['matchup_type'] == m]
        if len(sub) < 100:
            results[m] = current_mult
            continue
            
        # Optimize
        res = minimize(objective, [current_mult], args=(sub, current_mult), 
                       bounds=[(0.8, 1.2)], method='L-BFGS-B')
        
        opt_mult = float(res.x[0])
        results[m] = round(opt_mult, 3)
        print(f"{m}: {current_mult} -> {results[m]} (n={len(sub)})")
        
    return results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default="fact_probabilities_interaction_v1")
    args = parser.parse_args()
    
    df = load_data(args.table)
    optimized = optimize_multipliers(df)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(CONFIG_FILE, 'w') as f:
        json.dump(optimized, f, indent=4)
        
    print(f"Saved optimized multipliers to {CONFIG_FILE}")

if __name__ == "__main__":
    main()

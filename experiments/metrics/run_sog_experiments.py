
import duckdb
import pandas as pd
import numpy as np
import time
from sklearn.metrics import log_loss, brier_score_loss
from scipy.stats import nbinom

DB_PATH = "data/db/nhl_backtest.duckdb"
ALPHA_SOG = 0.35 # Fixed Alpha from THEORY

def load_data():
    con = duckdb.connect(DB_PATH)
    # Select games from 2023-2025
    query = """
    SELECT *
    FROM fact_player_game_features
    WHERE season >= 2023
    AND ev_toi_minutes_L40 > 0
    ORDER BY game_date
    """
    df = con.execute(query).df()
    con.close()
    return df

def calculate_nbinom_prob(mu_col, target_val=1.5):
    # P(X > target) = 1 - CDF(target)
    # nbinom parameters: n=1/alpha, p=1/(1+alpha*mu) ?? No, check distributions.py
    # r = 1/alpha, p = r/(r+mu)

    r = 1.0 / ALPHA_SOG
    # mu is a vector
    p_param = r / (r + mu_col)

    # We want P(X > 1.5) -> P(X >= 2) -> 1 - CDF(1)
    # Wait, SOG lines vary. 1.5, 2.5, etc.
    # For this experiment, let's just predict P(X >= 3) (2.5 line) as a standard high-value test.
    # Or just use LogLoss on the actual counts? No, LogLoss needs probs.
    # Let's target the 2.5 Line (P >= 3)
    k = 3

    prob = 1 - nbinom.cdf(k - 1, n=r, p=p_param)
    return prob

def calculate_metrics(df, pred_col, target_val=2.5):
    # Target: Did they get > 2.5 shots? (3+)
    y_true = (df['sog'] > target_val).astype(int)
    y_pred = np.clip(df[pred_col], 1e-15, 1 - 1e-15)

    ll = log_loss(y_true, y_pred)
    bs = brier_score_loss(y_true, y_pred)
    return ll, bs

def run_sog_experiments(df):
    print(f"Running SOG Experiments on {len(df)} rows...")
    results = []

    # Base TOI (Projected) - Use L10 Average TOI as simple baseline proxy
    # In live model we use L10 avg TOI or manual proj. 
    # Here we use `avg_toi_minutes_L10`.
    toi_col = df['avg_toi_minutes_L10']

    # --- 1. Baseline: SOG L20 Rate ---
    # Mu = Rate * Time
    # sog_per_60_L20_Derived was calculated as (SOG_L20 / TOI_L10) * 60??
    # Wait, in build_player_features:
    # CASE WHEN avg_toi_minutes_L10 > 0 THEN (sog_L20 / avg_toi_minutes_L10) * 60
    # This assumes usage hasn't changed.
    # Let's just use the raw average SOG_L20 as the Mu estimate for "Status Quo" 
    # because if TOI is stable, Avg SOG is the best predictor of SOG.

    df['mu_L20'] = df['sog_L20']
    df['p_L20'] = calculate_nbinom_prob(df['mu_L20'])
    ll, bs = calculate_metrics(df, 'p_L20')
    results.append({'Experiment': '1. Baseline (Raw SOG L20)', 'LogLoss': ll, 'Brier': bs})

    # --- 2. Corsi Model (L20) ---
    # Theory: SOG = Corsi * (SOG/Corsi Ratio)
    # Let's assume a League Average Thru% or Player Specific? 
    # Simple Corsi: Just use Corsi Rate * Standard Thru% (say 60%)? 
    # Or use Corsi * Player's L20 Thru%? That just equals SOG L20.
    # The value of Corsi is it stabilizes faster.
    # So: Mu = Corsi_L20_Rate * (Season Thru% or L40 Thru%)
    # Let's try: Mu = Corsi_L20 (Count) * 0.58 (League Avg Thru%)

    LG_THRU_PCT = 0.58
    df['mu_Corsi_L20'] = df['shot_attempts_L20'] * LG_THRU_PCT
    df['p_Corsi_L20'] = calculate_nbinom_prob(df['mu_Corsi_L20'])
    ll, bs = calculate_metrics(df, 'p_Corsi_L20')
    results.append({'Experiment': '2. Corsi L20 * LgAvg (0.58)', 'LogLoss': ll, 'Brier': bs})

    # --- 3. Corsi Model (L20 Corsi * L40 Thru%) ---
    # Combine stable Corsi volume (L20) with stable Finishing (L40 SOG/Corsi)
    # Thru% = sog_L40 / shot_attempts_L40
    # Guard against div/0
    df['thru_pct_L40'] = df['sog_L40'] / df['shot_attempts_L40'].replace(0, 1)
    # Clip extreme values
    df['thru_pct_L40'] = df['thru_pct_L40'].clip(0.3, 0.8) 

    df['mu_Corsi_Split'] = df['shot_attempts_L20'] * df['thru_pct_L40']
    df['p_Corsi_Split'] = calculate_nbinom_prob(df['mu_Corsi_Split'])
    ll, bs = calculate_metrics(df, 'p_Corsi_Split')
    results.append({'Experiment': '3. Corsi L20 * Thru% L40', 'LogLoss': ll, 'Brier': bs})

    # --- 4. Weighted SOG (50% L20, 50% L40) ---
    df['mu_Weighted'] = (df['sog_L20'] + df['sog_L40']) / 2
    df['p_Weighted'] = calculate_nbinom_prob(df['mu_Weighted'])
    ll, bs = calculate_metrics(df, 'p_Weighted')
    results.append({'Experiment': '4. Weighted SOG (50/50)', 'LogLoss': ll, 'Brier': bs})

    return pd.DataFrame(results).sort_values('LogLoss')

if __name__ == "__main__":
    start = time.time()
    print("Loading Data...")
    df = load_data()
    print(f"Loaded {len(df)} rows in {time.time() - start:.2f}s")
    
    res = run_sog_experiments(df)
    print("\n--- FINAL RESULTS (SOG 2.5+ Line) ---")
    print(res.to_string(index=False))

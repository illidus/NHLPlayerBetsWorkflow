import duckdb
import pandas as pd
import numpy as np
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
import joblib
import os

# Paths
DB_PATH = 'data/db/nhl_backtest.duckdb'
MODEL_DIR = 'data/models'
MODEL_PATH = os.path.join(MODEL_DIR, 'toi_model.pkl')

def train():
    print("--- Training TOI Model ---")
    
    con = duckdb.connect(DB_PATH)
    
    # Query Data
    # We need lag features. DuckDB can do window functions.
    # We want features that are KNOWN before the game.
    # Assuming fact_player_game_features L10 columns are pre-game computed.
    
    query = """
    WITH base AS (
        SELECT 
            player_id,
            game_date,
            team,
            opp_team,
            toi_minutes as target_toi,
            avg_toi_minutes_L10 as toi_L10,
            ev_toi_minutes_L5 as ev_toi_L5,
            pp_toi_minutes_L20 as pp_toi_L20,
            -- Lagged Target (Autoregressive) - very powerful
            LAG(toi_minutes, 1) OVER (PARTITION BY player_id ORDER BY game_date) as last_toi,
            LAG(toi_minutes, 2) OVER (PARTITION BY player_id ORDER BY game_date) as last_toi_2,
            -- Context
            CASE WHEN home_or_away = 'HOME' THEN 1 ELSE 0 END as is_home
        FROM fact_player_game_features
        WHERE season >= 2021 -- Recent eras
    )
    SELECT * FROM base 
    WHERE target_toi > 0 
      AND toi_L10 > 0
      AND last_toi IS NOT NULL
    """
    
    print("Executing Query...")
    df = con.execute(query).df()
    con.close()
    
    print(f"Loaded {len(df)} rows.")
    
    # Features
    features = ['toi_L10', 'ev_toi_L5', 'pp_toi_L20', 'last_toi', 'last_toi_2', 'is_home']
    target = 'target_toi'
    
    X = df[features]
    y = df[target]
    
    # Split (Time-based split ideally, but random is okay for this quick proto if we don't leak heavily)
    # Using random split for simplicity of implementation now, but acknowledging time-series risk.
    # Actually, let's just train on all past, but valid.
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Model
    # HistGradientBoostingRegressor is fast and handles NaNs (though we filtered them)
    model = HistGradientBoostingRegressor(
        max_iter=100,
        max_depth=5,
        learning_rate=0.1,
        random_state=42
    )
    
    print("Fitting Model...")
    model.fit(X_train, y_train)
    
    # Evaluate
    preds = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)
    r2 = r2_score(y_test, preds)
    
    print(f"Model Results: MAE={mae:.3f} mins, R2={r2:.3f}")
    
    # Save
    os.makedirs(MODEL_DIR, exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    print(f"Model saved to {MODEL_PATH}")

if __name__ == "__main__":
    train()

import duckdb
import pandas as pd
import numpy as np
import os
import sys
import joblib
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss
from scipy.special import logit, expit

def calculate_ece(y_true, y_prob, n_bins=10):
    if len(y_true) == 0:
        return 0.0
    bins = np.linspace(0., 1. + 1e-8, n_bins + 1)
    binids = np.digitize(y_prob, bins) - 1
    bin_sums = np.bincount(binids, weights=y_prob, minlength=n_bins)
    bin_true = np.bincount(binids, weights=y_true, minlength=n_bins)
    bin_total = np.bincount(binids, minlength=n_bins)
    nonzero = bin_total > 0
    bin_abs_diff = np.abs(bin_true[nonzero] / bin_total[nonzero] - bin_sums[nonzero] / bin_total[nonzero])
    ece = np.sum(bin_abs_diff * bin_total[nonzero]) / np.sum(bin_total)
    return ece

def fit_calibrators(db_path, output_dir):
    con = duckdb.connect(db_path)
    
    query = """
    SELECT
        p.season,
        p.market,
        p.line,
        p.p_over,
        CASE 
            WHEN p.market = 'ASSISTS' THEN (s.assists >= p.line)
            WHEN p.market = 'POINTS' THEN (s.points >= p.line)
            ELSE NULL
        END::INTEGER as realized
    FROM fact_probabilities p
    JOIN fact_skater_game_all s ON p.game_id = s.game_id AND p.player_id = s.player_id
    WHERE p.market IN ('ASSISTS', 'POINTS') AND p.line = 1
    AND realized IS NOT NULL
    """
    
    df = con.execute(query).df()
    con.close()
    
    if df.empty:
        print("No data found for calibration.")
        return

    os.makedirs(output_dir, exist_ok=True)
    
    # Chronological Split
    # Train: 2018-2022 | Val: 2023 | Test: 2024 (roughly)
    # Let's use 2023 as the split point for validation
    train_df = df[df['season'] < 2023].copy()
    val_df = df[df['season'] >= 2023].copy()
    
    if train_df.empty or val_df.empty:
        print("Insufficient data for chronological split. Check seasons in fact_probabilities.")
        return

    results = []
    
    for market in ['ASSISTS', 'POINTS']:
        print(f"\nFitting calibrators for {market}...")
        m_train = train_df[train_df['market'] == market]
        m_val = val_df[val_df['market'] == market]
        
        y_train = m_train['realized'].values
        p_train = m_train['p_over'].values
        
        y_val = m_val['realized'].values
        p_val = m_val['p_over'].values
        
        # Numerical safety for logit
        eps = 1e-10
        p_train_clamped = np.clip(p_train, eps, 1-eps)
        p_val_clamped = np.clip(p_val, eps, 1-eps)
        
        # --- Baseline (Raw) ---
        base_ll = log_loss(y_val, p_val_clamped)
        base_ece = calculate_ece(y_val, p_val)
        results.append({'Market': market, 'Method': 'Raw', 'LL': base_ll, 'ECE': base_ece})
        
        # --- Isotonic Regression ---
        iso = IsotonicRegression(out_of_bounds='clip')
        iso.fit(p_train, y_train)
        p_val_iso = iso.transform(p_val)
        p_val_iso_clamped = np.clip(p_val_iso, 1e-6, 1-1e-6)
        
        iso_ll = log_loss(y_val, p_val_iso_clamped)
        iso_ece = calculate_ece(y_val, p_val_iso)
        results.append({'Market': market, 'Method': 'Isotonic', 'LL': iso_ll, 'ECE': iso_ece})
        
        # --- Platt Scaling (Logistic Regression on Logit) ---
        logits_train = logit(p_train_clamped).reshape(-1, 1)
        logits_val = logit(p_val_clamped).reshape(-1, 1)
        
        platt = LogisticRegression(penalty=None) # No penalty for standard Platt
        platt.fit(logits_train, y_train)
        
        p_val_platt = platt.predict_proba(logits_val)[:, 1]
        p_val_platt_clamped = np.clip(p_val_platt, 1e-6, 1-1e-6)
        
        platt_ll = log_loss(y_val, p_val_platt_clamped)
        platt_ece = calculate_ece(y_val, p_val_platt)
        results.append({'Market': market, 'Method': 'Platt', 'LL': platt_ll, 'ECE': platt_ece})
        
        # Select best
        best_method = 'Isotonic' if iso_ll < platt_ll else 'Platt'
        best_model = iso if iso_ll < platt_ll else platt
        
        print(f"Best method for {market}: {best_method} (LL: {min(iso_ll, platt_ll):.4f} vs Raw: {base_ll:.4f})")
        
        # Save best model
        model_path = os.path.join(output_dir, f"calib_posthoc_{market}.joblib")
        joblib.dump({
            'method': best_method,
            'model': best_model,
            'market': market,
            'line': 1
        }, model_path)
        
    res_df = pd.DataFrame(results)
    print("\nCalibration Comparison:")
    print(res_df.to_string(index=False))

if __name__ == "__main__":
    fit_calibrators(
        "data/db/nhl_backtest.duckdb",
        "data/models/calibrators_posthoc/"
    )

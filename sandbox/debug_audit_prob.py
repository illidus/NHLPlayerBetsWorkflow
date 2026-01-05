import pandas as pd
import numpy as np

# Load the audit
df = pd.read_csv('ev_prob_audit_2026-01-06.csv')

# Search for the repeating value
target_prob = 0.8188
mask = np.isclose(df['p_model_used_in_ev'], target_prob, atol=1e-4)
matches = df[mask]

print(f"--- Audit Rows containing {target_prob} ---")
cols = ['player_name', 'market_key', 'mu_after_all_multipliers', 'p_model_computed', 'p_model_used_in_ev', 'ProbSource']
print(matches[cols].head(20).to_string(index=False))

# Unique raw probs for this calibrated prob?
print(f"\nUnique Raw Computed Probs for P_calib={target_prob}:")
print(matches['p_model_computed'].unique())

import pandas as pd
import numpy as np

df = pd.read_csv('ev_prob_audit_2026-01-06.csv')
target_prob = 0.8188
mask = np.isclose(df['p_model_used_in_ev'], target_prob, atol=1e-4)
subset = df[mask]

print(f"--- Prob Audit for {target_prob} ---")
cols = ['player_name', 'market_key', 'source_prob_column', 'ProbSource', 'p_model_used_in_ev']
print(subset[cols].head(10).to_string(index=False))

print("\nValue counts for source_prob_column:")
print(subset['source_prob_column'].value_counts())

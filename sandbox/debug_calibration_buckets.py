import pandas as pd

import numpy as np

# Load the probabilities
df = pd.read_csv('outputs/projections/SingleGamePropProbabilities.csv')

# Search for the repeating value using isclose
target_prob = 0.8188
# Filter numeric columns for comparison
numeric_df = df.select_dtypes(include=[np.number])
mask = np.isclose(numeric_df, target_prob, atol=1e-4).any(axis=1)
matches = df[mask]

print(f"--- Rows containing {target_prob} ---")
print(matches[['Player', 'Team']].head(20).to_string(index=False))

# Look at the raw vs calibrated for these rows
raw_cols = [c for c in df.columns if 'p_' in c and 'calibrated' not in c]
cal_cols = [c for c in df.columns if 'calibrated' in c]

sample_row = matches.iloc[0]
print("\n--- Calibration Mapping Sample ---")
for c in cal_cols:
    base_col = c.replace('_calibrated', '')
    if base_col in df.columns:
        print(f"{base_col}: {sample_row[base_col]:.4f} -> {c}: {sample_row[c]:.4f}")

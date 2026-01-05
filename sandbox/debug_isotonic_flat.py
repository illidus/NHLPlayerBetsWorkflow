import joblib
import numpy as np

model_path = "data/models/calibrators_posthoc/calib_posthoc_POINTS.joblib"
calib_data = joblib.load(model_path)
model = calib_data['model']

# Look at the interpolation function points
# model.f_ is an interpolating function
# Let's test it across a range
x_test = np.linspace(0, 1, 10001)
y_test = model.transform(x_test)

unique_outputs, counts = np.unique(np.round(y_test, 4), return_counts=True)
top_idx = np.argsort(-counts)

print("--- Most frequent calibrated outputs (Top 10) ---")
for i in range(10):
    idx = top_idx[i]
    print(f"P={unique_outputs[idx]:.4f} - Frequency: {counts[idx]} steps")

# Check 0.8188 specifically
target = 0.8188
mask = np.isclose(unique_outputs, target, atol=1e-4)
if any(mask):
    matched_val = unique_outputs[mask][0]
    freq = counts[unique_outputs == matched_val][0]
    print(f"\nTarget {target} FOUND. Rounded value in set: {matched_val}. Frequency: {freq}")
    
    mask_range = np.isclose(y_test, target, atol=1e-4)
    x_min = x_test[mask_range][0]
    x_max = x_test[mask_range][-1]
    print(f"Raw Prob Range [{x_min:.4f}, {x_max:.4f}] maps to {target}")
else:
    print(f"\nTarget {target} NOT FOUND in linspace test.")
    print("\nAvailable high probabilities:")
    print(unique_outputs[unique_outputs > 0.75])

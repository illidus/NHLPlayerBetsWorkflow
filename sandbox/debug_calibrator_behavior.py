import joblib
import numpy as np

model_path = "data/models/calibrators_posthoc/calib_posthoc_POINTS.joblib"
calib_data = joblib.load(model_path)
model = calib_data['model']

# Test the actual model object with the raw probabilities from the audit
raw_probs = [0.95351512, 0.92431718, 0.993955]
calibrated = model.transform(raw_probs)

print("--- Testing Calibration Mapping ---")
for r, c in zip(raw_probs, calibrated):
    print(f"Raw: {r:.6f} -> Calibrated: {c:.6f}")

# Look at the step function values again
print("\nUnique y_thresholds_ in model:")
print(np.unique(model.y_thresholds_))

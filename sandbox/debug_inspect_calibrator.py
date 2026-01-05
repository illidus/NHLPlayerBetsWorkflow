import joblib
import pandas as pd
import numpy as np

# Load the point calibrator
model_path = "data/models/calibrators_posthoc/calib_posthoc_ASSISTS.joblib"
calib_data = joblib.load(model_path)
model = calib_data['model']

print(f"Model Method: {calib_data['method']}")

# For Isotonic Regression, look at the step function (X and Y values)
if calib_data['method'] == 'Isotonic':
    print("\nModel Attributes:", [attr for attr in dir(model) if not attr.startswith('_')])
    
    # Try common attributes for isotonic regression mapping
    try:
        # y_thresholds_ is the most likely for modern sklearn
        y_vals = model.y_thresholds_
        x_vals = model.X_thresholds_
        print("\nTop unique calibrated values (y_thresholds_):")
        unique_y = np.unique(y_vals)
        print(unique_y[unique_y > 0.5])
        
        if any(np.isclose(unique_y, 0.8188, atol=1e-4)):
            print("\nSUCCESS: 0.8188 IS A STEP IN THE ISOTONIC REGRESSION.")
            idx = np.where(np.isclose(y_vals, 0.8188, atol=1e-4))[0]
            print(f"Maps from Raw Probs near: {x_vals[idx]}")
    except AttributeError:
        print("Attribute y_thresholds_ not found. Printing all available data.")
        print(model.__dict__)
else:
    print("Not an Isotonic model.")

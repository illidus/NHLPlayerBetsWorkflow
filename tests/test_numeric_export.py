import pandas as pd
import os
import sys

# Setup paths
project_root = os.getcwd()
src_dir = os.path.join(project_root, "src")
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

def test_xlsx_hygiene():
    xlsx_path = 'outputs/ev_analysis/MultiBookBestBets.xlsx'
    if not os.path.exists(xlsx_path):
        print(f"SKIPPING: {xlsx_path} not found. Run production first.")
        return

    print(f"Verifying {xlsx_path}...")
    df = pd.read_excel(xlsx_path)
    
    # Assert numeric columns
    numeric_cols = ['EV%', 'Model_Prob', 'Implied_Prob']
    for col in numeric_cols:
        assert col in df.columns, f"Missing column {col}"
        # pandas might infer dtype as float64
        is_numeric = pd.api.types.is_numeric_dtype(df[col])
        print(f"Column {col} dtype: {df[col].dtype} (Numeric: {is_numeric})")
        assert is_numeric, f"Column {col} is not numeric"

    # Assert sorting
    if len(df) >= 2:
        # Check if sorted descending by EV%
        # We use numeric EV% for sorting check
        is_sorted = all(df['EV%'].iloc[i] >= df['EV%'].iloc[i+1] for i in range(len(df)-1))
        print(f"Verified sorting: {is_sorted}")
        assert is_sorted, "MultiBookBestBets.xlsx is not sorted by EV% descending"

    print("SUCCESS: Numeric export hygiene verified.")

if __name__ == "__main__":
    test_xlsx_hygiene()

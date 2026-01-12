import unittest
import os
import sys
import subprocess
import json
import duckdb
from datetime import datetime, timedelta

class TestProductionProfileSmoke(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Configuration
        cls.profile_path = "config/production_profile.json"
        cls.test_table = f"smoke_test_prod_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        cls.start_date = "2024-01-01"
        cls.end_date = "2024-01-07"
        
        # Scripts
        cls.builder_script = "pipelines/backtesting/build_probability_snapshots.py"
        cls.eval_script = "pipelines/backtesting/evaluate_forecast_accuracy.py"
        
        if not os.path.exists(cls.profile_path):
            raise unittest.SkipTest("production_profile.json missing")
            
        with open(cls.profile_path, 'r') as f:
            cls.profile = json.load(f)

    @classmethod
    def tearDownClass(cls):
        # Cleanup DuckDB table to save space
        try:
            con = duckdb.connect("data/db/nhl_backtest.duckdb")
            con.execute(f"DROP TABLE IF EXISTS {cls.test_table}")
            con.close()
        except Exception as e:
            print(f"Warning: Could not cleanup table {cls.test_table}: {e}")

    def test_01_assets_exist(self):
        """Preflight check: Validate assets from profile."""
        paths = self.profile.get('paths', {})
        for key, path in paths.items():
            self.assertTrue(os.path.exists(path), f"Asset missing ({key}): {path}")

    def test_02_snapshot_build(self):
        """Run a tiny snapshot build."""
        env = os.environ.copy()
        
        # Inject Profile Env Vars
        paths = self.profile.get('paths', {})
        if 'scoring_alpha_override' in paths:
            env["NHL_BETS_SCORING_ALPHA_OVERRIDE_PATH"] = paths['scoring_alpha_override']
        if 'beta_override' in paths:
             env["NHL_BETS_BETA_OVERRIDE_PATH"] = paths['beta_override']
        if 'interaction_override' in paths:
             env["NHL_BETS_INTERACTIONS_PATH"] = paths['interaction_override']
             
        settings = self.profile.get('settings', {})
        
        cmd = [
            sys.executable, self.builder_script,
            "--start_date", self.start_date,
            "--end_date", self.end_date,
            "--output_table", self.test_table,
            "--model-version", "smoke_test",
            "--calibration", settings.get('calibration_mode', 'tail_bucket'),
            "--variance_mode", settings.get('variance_mode', 'all_nb')
        ]
        
        if settings.get('use_interactions'):
            cmd.append("--use_interactions")
            
        print(f"Running Smoke Build: {' '.join(cmd)}")
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(result.stdout)
            print(result.stderr)
            
        self.assertEqual(result.returncode, 0, "Snapshot build failed")

    def test_03_verify_outputs(self):
        """Verify the table was created and has data."""
        con = duckdb.connect("data/db/nhl_backtest.duckdb")
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {self.test_table}").fetchone()[0]
            self.assertGreater(count, 0, f"Table {self.test_table} is empty")
        finally:
            con.close()

    def test_04_evaluate_metrics(self):
        """Run evaluation and check for sane metrics."""
        # We need a temp file for output
        out_csv = f"outputs/eval/smoke_{self.test_table}.csv"
        
        cmd = [
            sys.executable, self.eval_script,
            "--table", self.test_table,
            "--out-csv", out_csv
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, "Evaluation failed")
        self.assertTrue(os.path.exists(out_csv), "Output CSV not created")
        
        # Check constraints
        import pandas as pd
        df = pd.read_csv(out_csv)
        
        self.assertFalse(df.empty, "Evaluation output is empty")
        
        # Check for any valid metric row
        # Current columns: Market, Variant, Line, Log Loss, etc.
        # We just pick the first row to verify metrics are populated
        row = df.iloc[0]
        
        ll = row.get('Log Loss')
        self.assertFalse(pd.isna(ll), "Log Loss is NaN")
        self.assertGreater(ll, 0.10, "Log Loss suspiciously low")
        # Relaxed upper bound for small smoke test samples
        self.assertLess(ll, 1.50, "Log Loss suspiciously high") 
        
        # Cleanup CSV
        if os.path.exists(out_csv):
            os.remove(out_csv)

if __name__ == '__main__':
    unittest.main()

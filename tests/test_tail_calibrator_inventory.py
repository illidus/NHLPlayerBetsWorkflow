import unittest
import os
import glob
import json

class TestTailCalibratorInventory(unittest.TestCase):
    def setUp(self):
        self.calib_dir = "data/models/calibrators_posthoc"
        self.markets = ["SOG", "BLOCKS"] # Tail calibration mainly for these
        self.positions = ["F", "D"]
        self.volatility_buckets = ["low", "mid", "high"]
        # Lines are continuous but buckets are discrete, e.g. 0.5, 1.5, 2.5, 3.5
        # We need to verify that we have *some* coverage. 
        # Checking for *specific* lines might be brittle if buckets change, 
        # but we can check for the existence of files matching the pattern.

    def test_calibrator_directory_exists(self):
        self.assertTrue(os.path.exists(self.calib_dir), f"Directory not found: {self.calib_dir}")

    def test_market_coverage(self):
        """Ensure we have calibrators for key markets."""
        files = os.listdir(self.calib_dir)
        for market in self.markets:
            market_files = [f for f in files if f"calib_tail_{market}" in f]
            self.assertTrue(len(market_files) > 0, f"No calibrators found for market: {market}")

    def test_production_profile_requirements(self):
        """Verify that specific calibrators mentioned in production_profile.json exist."""
        profile_path = "config/production_profile.json"
        if not os.path.exists(profile_path):
            self.skipTest("production_profile.json not found")
        
        with open(profile_path, 'r') as f:
            profile = json.load(f)
            
        req_calibs = profile.get('required_calibrators', [])
        for calib in req_calibs:
            path = os.path.join(self.calib_dir, calib)
            self.assertTrue(os.path.exists(path), f"Required calibrator missing: {calib}")

    def test_bucket_integrity(self):
        """Sanity check: Ensure we don't have empty files."""
        for fpath in glob.glob(os.path.join(self.calib_dir, "*.joblib")):
            size = os.path.getsize(fpath)
            self.assertGreater(size, 0, f"Calibrator file is empty: {fpath}")

if __name__ == '__main__':
    unittest.main()

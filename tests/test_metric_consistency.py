import unittest
import pandas as pd
import numpy as np
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.getcwd()))
from src.nhl_bets.eval.metrics import compute_log_loss, compute_brier_score

class TestMetricConsistency(unittest.TestCase):
    def setUp(self):
        # Create a small deterministic dataframe
        self.df = pd.DataFrame({
            'market': ['GOALS', 'GOALS', 'ASSISTS', 'ASSISTS'],
            'line': [0.5, 0.5, 0.5, 0.5],
            'p_over': [0.1, 0.9, 0.4, 0.6],
            'y_true': [0, 1, 1, 0] # outcome
        })
        
        # Expected Results (calculated manually or via sklearn directly for ground truth)
        # 0: p=0.1, y=0 -> -log(0.9)
        # 1: p=0.9, y=1 -> -log(0.9)
        # 2: p=0.4, y=1 -> -log(0.4)
        # 3: p=0.6, y=0 -> -log(0.4)
        
        # All inputs result in loss = -log(0.9) or -log(0.4)
        # but let's just use the function itself as the ground truth wrapper
        # The test is about CONSISTENCY across potential different paths if they existed,
        # but since we refactored everything to use the SAME function, 
        # this test essentially verifies the function handles the data correctly
        # and that the scripts would get the same result.
        pass

    def test_shared_metric_function(self):
        """Verify the shared metric function returns expected values."""
        y_true = self.df['y_true'].values
        y_prob = self.df['p_over'].values
        
        ll = compute_log_loss(y_true, y_prob)
        bs = compute_brier_score(y_true, y_prob)
        
        # Manual check
        expected_ll = -(np.log(0.9) + np.log(0.9) + np.log(0.4) + np.log(0.4)) / 4
        expected_bs = ((0.1-0)**2 + (0.9-1)**2 + (0.4-1)**2 + (0.6-0)**2) / 4
        
        self.assertAlmostEqual(ll, expected_ll, places=9)
        self.assertAlmostEqual(bs, expected_bs, places=9)

    def test_script_usage_simulation(self):
        """Simulate how scripts call the metric to ensure no divergence."""
        
        # Simulation of evaluate_log_loss.py logic
        # It filters by market slices.
        sub_df = self.df[self.df['market'] == 'GOALS']
        ll_goals = compute_log_loss(sub_df['y_true'].values, sub_df['p_over'].values)
        
        # Simulation of check_regression_gate.py logic (per market)
        # It iterates groupby objects
        gate_ll_goals = None
        for market, group in self.df.groupby('market'):
            if market == 'GOALS':
                gate_ll_goals = compute_log_loss(group['y_true'].values, group['p_over'].values)
        
        self.assertAlmostEqual(ll_goals, gate_ll_goals, places=9, msg="Divergence between script logic simulations")

if __name__ == '__main__':
    unittest.main()
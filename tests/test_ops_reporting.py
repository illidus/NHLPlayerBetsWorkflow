import unittest
from unittest.mock import patch, MagicMock, mock_open
import sys
import os
import pandas as pd
from datetime import datetime, timezone

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'src'))

from nhl_bets.analysis import runner_duckdb

class TestOpsReporting(unittest.TestCase):

    @patch('nhl_bets.analysis.runner_duckdb.get_db_connection')
    @patch('nhl_bets.analysis.runner_duckdb.get_mapped_odds')
    @patch('nhl_bets.analysis.runner_duckdb.pd.read_csv')
    @patch('nhl_bets.analysis.runner_duckdb.os.makedirs')
    @patch('builtins.open', new_callable=mock_open)
    @patch('nhl_bets.analysis.runner_duckdb.pd.DataFrame.to_excel')
    def test_freshness_report_generation(self, mock_to_excel, mock_file_open, mock_makedirs, mock_read_csv, mock_get_mapped_odds, mock_get_db):
        
        # Setup mocks
        
        # 1. Mock Mapped Odds
        mock_odds = pd.DataFrame({
            'player_name_raw': ['Connor McDavid', 'Auston Matthews'],
            'book_name_raw': ['PlayNow', 'Bet365'],
            'market_type': ['POINTS', 'GOALS'],
            'line': [1.5, 0.5],
            'side': ['Over', 'Over'],
            'odds_decimal': [1.8, 2.0],
            'odds_american': [-125, +100],
            'source_vendor': ['PLAYNOW', 'ODDSSHARK'],
            'capture_ts_utc': [datetime.now(timezone.utc), datetime.now(timezone.utc)],
            'event_id_vendor': ['1', '2'],
            'raw_payload_hash': ['h1', 'h2']
        })
        mock_get_mapped_odds.return_value = mock_odds
        
        # 2. Mock Probs
        mock_probs = pd.DataFrame({
            'Player': ['Connor McDavid', 'Auston Matthews'],
            'Team': ['EDM', 'TOR'],
            'p_over_calibrated_POINTS_1.5plus': [0.6, None],
            'p_over_GOALS_0.5plus': [None, 0.55],
            'prob_snapshot_ts': [datetime.now(timezone.utc)] * 2
        })
        mock_read_csv.return_value = mock_probs
        
        # 3. Run Main
        # We need to mock os.environ to set freshness window if needed, or rely on default
        with patch.dict(os.environ, {'EV_ODDS_FRESHNESS_MINUTES': '1000'}): # Large window to keep them fresh
            runner_duckdb.main()
            
        # 4. Assertions
        
        # Check if reports were written
        # We expect calls to open with 'ev_freshness_coverage_...' and 'ev_freshness_coverage_latest.md'
        
        file_writes = [call.args[0] for call in mock_file_open.call_args_list if isinstance(call.args[0], str)]
        
        # Filter for report files
        report_files = [f for f in file_writes if 'ev_freshness_coverage' in f]
        
        self.assertTrue(len(report_files) >= 2, f"Expected at least 2 report file writes, got: {report_files}")
        
        has_timestamped = any('ev_freshness_coverage_20' in f for f in report_files)
        has_latest = any('ev_freshness_coverage_latest.md' in f for f in report_files)
        
        self.assertTrue(has_timestamped, "Timestamped report not found")
        self.assertTrue(has_latest, "Latest pointer report not found")
        
        # Verify content written (optional, but good)
        # We can check if handle().write was called with some content
        handle = mock_file_open()
        written_content = "".join([call.args[0] for call in handle.write.call_args_list])
        
        self.assertIn("# EV Freshness Coverage Report", written_content)
        self.assertIn("Total Candidates", written_content)

if __name__ == '__main__':
    unittest.main()

import unittest
from unittest.mock import patch, MagicMock
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parents[2]))

# We need to reload the module to pick up constant changes if we patch env vars, 
# but for unit testing functions we can just import.
from pipelines.backtesting import download_moneypuck_team_player_gbg as downloader

class TestMoneyPuckUrlFix(unittest.TestCase):
    
    def test_resolve_url_candidates(self):
        # Default behavior
        candidates = list(downloader.resolve_url_candidates("test/file.csv"))
        self.assertEqual(len(candidates), 2)
        self.assertIn("https://moneypuck.com/moneypuck/playerData/test/file.csv", candidates[0])
        self.assertIn("https://moneypuck.com/playerData/test/file.csv", candidates[1])
        
    def test_resolve_url_candidates_dedupe(self):
        # If we set env var to same as fallback, should only yield one
        orig = downloader.DEFAULT_BASE_URL
        downloader.DEFAULT_BASE_URL = "https://moneypuck.com/playerData"
        try:
            candidates = list(downloader.resolve_url_candidates("test/file.csv"))
            self.assertEqual(len(candidates), 1)
        finally:
            downloader.DEFAULT_BASE_URL = orig

    @patch('pipelines.backtesting.download_moneypuck_team_player_gbg._attempt_request')
    @patch('pipelines.backtesting.download_moneypuck_team_player_gbg.validate_cache')
    def test_download_fallback_sequence(self, mock_validate, mock_request):
        mock_validate.return_value = False
        
        # Mock responses: 
        # 1. HEAD primary -> 403
        # 2. HEAD fallback -> 200
        # 3. GET fallback -> 200
        
        resp_403 = MagicMock()
        resp_403.status_code = 403
        
        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.headers = {'Content-Length': '100'}
        resp_200.iter_content.return_value = [b"data"]
        
        # We expect calls:
        # 1. HEAD primary
        # 2. HEAD fallback
        # 3. GET fallback
        mock_request.side_effect = [resp_403, resp_200, resp_200]
        
        with patch('builtins.open', new_callable=MagicMock):
            res = downloader.download_file_with_fallback("path.csv", "local.csv")
            
        self.assertEqual(res, "downloaded")
        self.assertEqual(mock_request.call_count, 3)
        
        # Verify call args
        calls = mock_request.call_args_list
        self.assertIn("moneypuck/playerData", calls[0][0][0]) # Primary
        self.assertIn("moneypuck.com/playerData", calls[1][0][0]) # Fallback

if __name__ == '__main__':
    unittest.main()

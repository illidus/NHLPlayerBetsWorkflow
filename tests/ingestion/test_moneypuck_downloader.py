import unittest
from unittest.mock import patch, MagicMock, mock_open
import sys
import os
from pathlib import Path
import json

# Add project root to path
sys.path.append(str(Path(__file__).parents[2]))

from pipelines.backtesting import download_moneypuck_team_player_gbg as downloader

class TestMoneyPuckDownloader(unittest.TestCase):
    
    def setUp(self):
        self.data_root = Path("mock_data_root")
        
    @patch('pipelines.backtesting.download_moneypuck_team_player_gbg.requests.head')
    @patch('pipelines.backtesting.download_moneypuck_team_player_gbg.requests.get')
    @patch('pipelines.backtesting.download_moneypuck_team_player_gbg.validate_cache')
    def test_download_file_403_best_effort_no_cache(self, mock_validate, mock_get, mock_head):
        # Setup
        downloader.REFRESH_MODE = "best_effort"
        mock_head.return_value.status_code = 403
        mock_validate.return_value = False # No cache
        
        # Action
        result = downloader.download_file("http://test.com/file.csv", "mock_path.csv")
        
        # Assert
        self.assertEqual(result, "skipped") # In best_effort, it skips without cache? 
        # Wait, my logic says:
        # if validate_cache: return served_from_cache
        # else: if REFRESH_MODE == required: failed else: skipped
        
    @patch('pipelines.backtesting.download_moneypuck_team_player_gbg.requests.head')
    @patch('pipelines.backtesting.download_moneypuck_team_player_gbg.requests.get')
    @patch('pipelines.backtesting.download_moneypuck_team_player_gbg.validate_cache')
    def test_download_file_403_required_no_cache(self, mock_validate, mock_get, mock_head):
        # Setup
        downloader.REFRESH_MODE = "required"
        mock_head.return_value.status_code = 403
        mock_validate.return_value = False
        
        # Action
        result = downloader.download_file("http://test.com/file.csv", "mock_path.csv")
        
        # Assert
        self.assertEqual(result, "failed")

    @patch('pipelines.backtesting.download_moneypuck_team_player_gbg.requests.head')
    @patch('pipelines.backtesting.download_moneypuck_team_player_gbg.requests.get')
    @patch('pipelines.backtesting.download_moneypuck_team_player_gbg.validate_cache')
    def test_download_file_403_with_cache(self, mock_validate, mock_get, mock_head):
        # Setup
        mock_head.return_value.status_code = 403
        mock_validate.return_value = True # Cache exists
        
        # Action
        result = downloader.download_file("http://test.com/file.csv", "mock_path.csv")
        
        # Assert
        self.assertEqual(result, "served_from_cache")

    @patch('builtins.open', new_callable=mock_open)
    @patch('pipelines.backtesting.download_moneypuck_team_player_gbg.datetime')
    def test_save_manifest(self, mock_datetime, mock_file):
        mock_datetime.now.return_value.isoformat.return_value = "2026-01-01T00:00:00"
        
        stats = {"downloaded": 10}
        downloader.save_manifest(Path("root"), stats)
        
        mock_file.assert_called_with(Path("root") / "_manifest.json", 'w')
        handle = mock_file()
        # verify write was called
        self.assertTrue(handle.write.called)

if __name__ == '__main__':
    unittest.main()

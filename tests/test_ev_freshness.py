
import pytest
import pandas as pd
from datetime import datetime, timedelta, timezone
import sys
import os

# Add src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from nhl_bets.analysis.runner_duckdb import filter_by_freshness

def test_filter_by_freshness_logic():
    # Setup Snapshot TS (UTC)
    snapshot_ts = datetime.now(timezone.utc)
    
    # Create Mock DataFrame
    data = [
        # Fresh: 10 mins after snapshot
        {'id': 1, 'capture_ts_utc': (snapshot_ts + timedelta(minutes=10)).isoformat()},
        # Fresh: 10 mins before snapshot
        {'id': 2, 'capture_ts_utc': (snapshot_ts - timedelta(minutes=10)).isoformat()},
        # Stale: 100 mins before snapshot
        {'id': 3, 'capture_ts_utc': (snapshot_ts - timedelta(minutes=100)).isoformat()},
        # Missing TS
        {'id': 4, 'capture_ts_utc': None},
        # Invalid TS
        {'id': 5, 'capture_ts_utc': 'invalid'},
    ]
    df = pd.DataFrame(data)
    
    # Run Filter (Window=90)
    df_fresh, df_excluded = filter_by_freshness(df, snapshot_ts, 90)
    
    # Assertions
    assert len(df_fresh) == 2, f"Expected 2 fresh rows, got {len(df_fresh)}"
    assert 1 in df_fresh['id'].values
    assert 2 in df_fresh['id'].values
    
    assert len(df_excluded) == 3, f"Expected 3 excluded rows, got {len(df_excluded)}"
    assert 3 in df_excluded['id'].values
    assert 4 in df_excluded['id'].values
    assert 5 in df_excluded['id'].values
    
    # Check freshness column
    assert 'freshness_minutes' in df_fresh.columns
    assert df_fresh.loc[df_fresh['id'] == 1, 'freshness_minutes'].iloc[0] == pytest.approx(10.0, 0.1)

def test_filter_by_freshness_empty():
    df = pd.DataFrame()
    snapshot_ts = datetime.now(timezone.utc)
    df_fresh, df_excluded = filter_by_freshness(df, snapshot_ts, 90)
    assert df_fresh.empty
    assert df_excluded.empty

if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))

import pandas as pd
import pytest
from datetime import datetime, timezone, timedelta
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'src'))

from nhl_bets.analysis.runner_duckdb import filter_by_freshness

def test_freshness_gating():
    # 1. Define Snapshot TS
    snapshot_ts = datetime(2025, 1, 6, 12, 0, 0, tzinfo=timezone.utc)
    
    # 2. Define Test Cases (Odds rows)
    data = [
        # Fresh (exact match)
        {'capture_ts_utc': '2025-01-06T12:00:00+00:00', 'id': 1},
        # Fresh (within window - e.g. 5 mins before snapshot)
        {'capture_ts_utc': '2025-01-06T11:55:00+00:00', 'id': 2},
        # Fresh (within window - e.g. 5 mins after snapshot)
        {'capture_ts_utc': '2025-01-06T12:05:00+00:00', 'id': 3},
        # Stale (too old - 91 mins before)
        {'capture_ts_utc': '2025-01-06T10:29:00+00:00', 'id': 4},
        # Stale (too new? - 91 mins after - technically stale relative to snapshot if we care about "far from snapshot")
        # Freshness is abs diff. So yes.
        {'capture_ts_utc': '2025-01-06T13:31:00+00:00', 'id': 5},
        # Missing TS
        {'capture_ts_utc': None, 'id': 6}
    ]
    
    df = pd.DataFrame(data)
    
    # 3. Apply Filter (Window = 90 mins)
    df_fresh, df_excluded = filter_by_freshness(df, snapshot_ts, 90.0)
    
    # 4. Assertions
    fresh_ids = df_fresh['id'].tolist()
    excluded_ids = df_excluded['id'].tolist()
    
    assert 1 in fresh_ids
    assert 2 in fresh_ids
    assert 3 in fresh_ids
    assert 4 in excluded_ids # > 90 min diff
    assert 5 in excluded_ids # > 90 min diff
    assert 6 in excluded_ids # Missing
    
    print("Freshness Gating Test Passed!")

def test_freshness_missing_col():
    df = pd.DataFrame([{'id': 1}])
    snapshot_ts = datetime.now(timezone.utc)
    df_fresh, df_excluded = filter_by_freshness(df, snapshot_ts, 90.0)
    assert df_fresh.equals(df) # Should return original df if col missing? 
    # Logic: if df.empty or 'capture_ts_utc' not in df.columns: return df, pd.DataFrame()
    assert df_excluded.empty

if __name__ == "__main__":
    test_freshness_gating()
    test_freshness_missing_col()

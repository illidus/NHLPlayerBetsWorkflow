import pandas as pd
import pytest
import sys
import os
from datetime import datetime, timezone, timedelta

# Ensure project root is in path
sys.path.insert(0, os.getcwd())
sys.path.insert(0, os.path.join(os.getcwd(), "src"))

from nhl_bets.analysis.runner_duckdb import filter_by_event_eligibility

def test_filter_by_event_eligibility():
    now = datetime(2026, 1, 6, 12, 0, 0, tzinfo=timezone.utc)
    
    data = [
        # Future game
        {"Player": "P1", "event_start_time_utc": now + timedelta(hours=1), "is_live": False},
        # Past game (started)
        {"Player": "P2", "event_start_time_utc": now - timedelta(hours=1), "is_live": False},
        # Game within grace period (0 by default)
        {"Player": "P3", "event_start_time_utc": now - timedelta(minutes=5), "is_live": False},
        # Missing start time
        {"Player": "P4", "event_start_time_utc": None, "is_live": False},
        # Live game
        {"Player": "P5", "event_start_time_utc": now + timedelta(hours=1), "is_live": True},
    ]
    df = pd.DataFrame(data)
    
    # Test with 0 grace
    df_eligible, df_started, df_missing, df_live = filter_by_event_eligibility(df.copy(), now, grace_minutes=0)
    
    assert len(df_eligible) == 1
    assert df_eligible.iloc[0]["Player"] == "P1"
    assert len(df_started) == 2 # P2 and P3
    assert len(df_missing) == 1 # P4
    assert len(df_live) == 1 # P5
    
    # Test with 10 min grace
    df_eligible_g, df_started_g, df_missing_g, df_live_g = filter_by_event_eligibility(df.copy(), now, grace_minutes=10)
    assert len(df_eligible_g) == 2
    assert "P3" in df_eligible_g["Player"].values
    assert len(df_started_g) == 1
    assert df_started_g.iloc[0]["Player"] == "P2"

if __name__ == "__main__":
    test_filter_by_event_eligibility()
    print("Test passed!")

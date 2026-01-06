import pytest
import pandas as pd
import duckdb
from datetime import datetime, timezone, timedelta
from nhl_bets.common.db_init import insert_odds_records, initialize_phase11_tables
from nhl_bets.analysis.runner_duckdb import filter_by_freshness

def test_insert_odds_records_utc_handling():
    """
    Verifies that insert_odds_records correctly handles TZ-aware UTC timestamps
    by converting them to Naive UTC so DuckDB stores them as-is.
    """
    con = duckdb.connect(":memory:")
    initialize_phase11_tables(con)
    
    # Create a test dataframe with Aware UTC timestamps
    now_utc = datetime.now(timezone.utc)
    # Ensure microseconds for precision check
    now_utc = now_utc.replace(microsecond=123456)
    
    data = [{
        "source_vendor": "TEST",
        "capture_ts_utc": now_utc,
        "event_start_ts_utc": now_utc + timedelta(hours=1),
        "event_id_vendor": "E1",
        "market_type": "SOG",
        "player_name_raw": "Player A",
        "line": 2.5,
        "side": "OVER",
        "book_id_vendor": "B1"
    }]
    df = pd.DataFrame(data)
    
    # Insert
    insert_odds_records(con, df)
    
    # Read back
    res = con.execute("SELECT capture_ts_utc FROM fact_prop_odds").fetchall()
    ts_db = res[0][0]
    
    # DuckDB returns naive datetime
    assert ts_db.tzinfo is None
    
    # The value should match the UTC time (not converted to local)
    # Allow small float precision diffs if any, but datetime should be exact
    # We stripped tzinfo, so we compare to naive version of original
    assert ts_db == now_utc.replace(tzinfo=None)
    
    # Verify it is NOT local time (unless local is UTC)
    # Create a local time version of now_utc
    local_now = now_utc.astimezone() # Local time
    if local_now.utcoffset().total_seconds() != 0:
        # If we are not in UTC, the stored value should NOT match local wall clock
        # e.g. if now_utc is 20:00, and local is 12:00, stored should be 20:00.
        # ts_db is 20:00. local_now.replace(tzinfo=None) is 12:00.
        assert ts_db != local_now.replace(tzinfo=None)

def test_filter_by_freshness_logic():
    """
    Verifies freshness filtering logic with naive inputs from DB treated as UTC.
    """
    snapshot_ts = datetime(2026, 1, 6, 20, 0, 0, tzinfo=timezone.utc)
    
    # Case 1: Fresh (Same time)
    # DB returns Naive UTC
    ts_fresh = datetime(2026, 1, 6, 20, 0, 0) 
    
    # Case 2: Stale (2 hours old)
    ts_stale = datetime(2026, 1, 6, 18, 0, 0)
    
    # Case 3: Future (should be fresh/negative diff, freshness is abs diff? Code says abs())
    ts_future = datetime(2026, 1, 6, 20, 30, 0)
    
    df = pd.DataFrame([
        {"capture_ts_utc": ts_fresh, "id": 1},
        {"capture_ts_utc": ts_stale, "id": 2},
        {"capture_ts_utc": ts_future, "id": 3}
    ])
    
    df_fresh, df_excl = filter_by_freshness(df, snapshot_ts, window_minutes=90)
    
    # ID 1 (0 min diff) -> Fresh
    # ID 2 (120 min diff) -> Stale
    # ID 3 (30 min diff) -> Fresh
    
    assert 1 in df_fresh['id'].values
    assert 2 in df_excl['id'].values
    assert 3 in df_fresh['id'].values
    
    # Verify 'capture_ts_dt' created is Aware UTC
    assert df_fresh['capture_ts_dt'].dt.tz is not None
    # It should be UTC
    assert str(df_fresh['capture_ts_dt'].dt.tz) == "UTC" # pandas stores as UTC
    
    # Verify values match
    # ID 1: 20:00 Naive -> 20:00 UTC
    assert df_fresh.loc[df_fresh['id']==1, 'capture_ts_dt'].iloc[0] == snapshot_ts

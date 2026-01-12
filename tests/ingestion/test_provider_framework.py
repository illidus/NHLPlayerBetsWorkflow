import pytest
import shutil
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from src.nhl_bets.ingestion.providers.the_odds_api import TheOddsApiProvider
from src.nhl_bets.ingestion.storage import RawStorage

# Temp dir for test
TEST_RAW_DIR = Path("tests/ingestion/tmp_raw")
FIXTURE_PATH = Path("tests/fixtures/the_odds_api_mock_bad_row.json")

@pytest.fixture
def clean_raw_dir():
    if TEST_RAW_DIR.exists():
        shutil.rmtree(TEST_RAW_DIR)
    TEST_RAW_DIR.mkdir(parents=True)
    
    # Patch RawStorage
    original_dir = RawStorage.BASE_DIR
    RawStorage.BASE_DIR = TEST_RAW_DIR
    
    yield
    
    # Teardown
    RawStorage.BASE_DIR = original_dir
    if TEST_RAW_DIR.exists():
        shutil.rmtree(TEST_RAW_DIR)

def test_odds_conversion():
    provider = TheOddsApiProvider(mock_mode=True)
    # 2.0 -> +100
    assert provider.decimal_to_american(2.0) == 100
    # 1.909 -> -110
    assert provider.decimal_to_american(1.90909) == -110
    # 1.5 -> -200
    assert provider.decimal_to_american(1.5) == -200
    # 3.0 -> +200
    assert provider.decimal_to_american(3.0) == 200

def test_provider_ingestion_flow(clean_raw_dir):
    provider = TheOddsApiProvider(mock_mode=True)
    
    start = datetime(2023, 1, 1)
    end = datetime(2023, 1, 1)
    
    roi_df, unresolved_df = provider.ingest_date_range(start, end, league="NHL")
    
    assert not roi_df.empty
    # Unresolved might be empty if mock data is perfect
    
    assert "source_vendor" in roi_df.columns
    assert "odds_american" in roi_df.columns
    assert "market_type" in roi_df.columns
    assert "join_conf_event" in roi_df.columns
    
    # Verify content from mock data
    row = roi_df.iloc[0]
    assert row["source_vendor"] == "THE_ODDS_API"
    assert row["market_type"] == "POINTS"
    assert row["player_name_raw"] == "Connor McDavid"
    assert row["book_id_vendor"] == "draftkings"
    
    # Verify timestamps
    assert row["capture_ts_utc"] is not None
    assert row["ingested_at_utc"] is not None
    
    # Verify raw file was written
    # Path format: TEST_RAW_DIR / VENDOR / YYYY / MM / DD / timestamp_suffix
    vendor_dir = TEST_RAW_DIR / "THE_ODDS_API" / start.strftime("%Y") / start.strftime("%m") / start.strftime("%d")
    assert vendor_dir.exists()
    files = list(vendor_dir.glob("*.json"))
    assert len(files) >= 1

def test_unresolved_routing_with_fixture(clean_raw_dir, monkeypatch):
    """
    Tests that bad rows from a fixture are correctly routed to unresolved_df.
    """
    with open(FIXTURE_PATH, "r") as f:
        fixture_data = json.load(f)
        
    def mock_fetch(*args, **kwargs):
        meta = {
            "file_suffix": "fixture_test.json",
            "requested_asof": datetime(2023, 1, 1, tzinfo=timezone.utc),
            "capture_ts": datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        }
        return [(meta, fixture_data)]
        
    provider = TheOddsApiProvider(mock_mode=True)
    monkeypatch.setattr(provider, "_fetch_mock_data", mock_fetch)
    
    roi_df, unresolved_df = provider.ingest_date_range(datetime(2023, 1, 1), datetime(2023, 1, 1))
    
    # Fixture has 1 valid event (McDavid Points Over/Under = 2 rows) and 1 invalid event (Ghost Player = 1 row)
    assert len(roi_df) == 2
    assert len(unresolved_df) == 1
    
    assert unresolved_df.iloc[0]["player_name_raw"] == "Ghost Player"
    assert "MISSING_EVENT_ID" in unresolved_df.iloc[0]["failure_reasons"]

def test_idempotency_check(clean_raw_dir):
    """
    Simulates two runs of the same ingestion.
    Ensures raw files are reused or deterministic, and rows match.
    (Note: RawStorage uses timestamp in filename so strictly it creates new files per run,
     but content hash allows us to detect dupes if we wanted. 
     For DB idempotency, we rely on the schema_mgr.insert_idempotent logic.
     Here we just check that provider logic is stable.)
    """
    provider = TheOddsApiProvider(mock_mode=True)
    start = datetime(2023, 1, 1)
    
    # Run 1
    roi_1, _ = provider.ingest_date_range(start, start)
    
    # Run 2
    roi_2, _ = provider.ingest_date_range(start, start)
    
    # Should be identical in content (except captured/ingested timestamps if they are generated on fly)
    # Mock data has fixed commence_time but capture_ts is generated in storage.
    
    assert len(roi_1) == len(roi_2)
    assert roi_1.iloc[0]["player_name_raw"] == roi_2.iloc[0]["player_name_raw"]


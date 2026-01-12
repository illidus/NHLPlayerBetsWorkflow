import pytest
import duckdb
import pandas as pd
from datetime import datetime
from src.nhl_bets.ingestion.schema import OddsSchemaManager

@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test.duckdb")

def test_schema_initialization(db_path):
    mgr = OddsSchemaManager(db_path)
    mgr.ensure_schema()
    mgr.ensure_schema() # Idempotency check
    
    con = duckdb.connect(db_path)
    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    con.close()
    
    expected = ['dim_books', 'dim_events', 'dim_markets', 'dim_players', 'fact_prop_odds']
    for t in expected:
        assert t in tables

def test_idempotent_insert(db_path):
    mgr = OddsSchemaManager(db_path)
    mgr.ensure_schema()
    
    data = {
        "source_vendor": ["TEST"],
        "capture_ts_utc": [datetime(2026, 1, 1)],
        "event_id_vendor": ["E1"],
        "event_start_ts_utc": [None],
        "player_id_vendor": ["P1"],
        "player_name_raw": ["McDavid"],
        "market_type": ["GOALS"],
        "line": [1.5],
        "side": ["OVER"],
        "book_id_vendor": ["B1"],
        "book_name_raw": ["Bookie"],
        "odds_american": [-110],
        "odds_decimal": [1.91],
        "is_live": [False],
        "raw_payload_path": ["path/to/raw"],
        "raw_payload_hash": ["hash123"]
    }
    df = pd.DataFrame(data)
    
    # 1st Insert
    mgr.insert_idempotent(df)
    
    con = duckdb.connect(db_path)
    count = con.execute("SELECT COUNT(*) FROM fact_prop_odds").fetchone()[0]
    assert count == 1
    
    # 2nd Insert (Duplicate)
    mgr.insert_idempotent(df)
    count = con.execute("SELECT COUNT(*) FROM fact_prop_odds").fetchone()[0]
    assert count == 1
    
    # 3rd Insert (New Data)
    df2 = df.copy()
    df2['line'] = 2.5
    mgr.insert_idempotent(df2)
    count = con.execute("SELECT COUNT(*) FROM fact_prop_odds").fetchone()[0]
    assert count == 2
    
    con.close()

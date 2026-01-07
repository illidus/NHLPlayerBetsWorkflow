import os
import sys
import tempfile
from datetime import datetime, timezone

import duckdb
import pandas as pd

from nhl_bets.common.db_init import initialize_phase11_tables, insert_odds_records
from nhl_bets.common.vendor_utils import MAX_RETRIES, get_timeout_tuple, should_force_vendor_failure


def test_phase11_schema_creation():
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = os.path.join(tmp_dir, "test.duckdb")
        con = duckdb.connect(db_path)
        try:
            initialize_phase11_tables(con)
            tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
            assert "fact_prop_odds" in tables
            assert "raw_odds_payloads" in tables
        finally:
            con.close()


def test_insert_odds_records_idempotent():
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = os.path.join(tmp_dir, "test.duckdb")
        con = duckdb.connect(db_path)
        try:
            initialize_phase11_tables(con)
            df = pd.DataFrame(
                [
                    {
                        "source_vendor": "TEST",
                        "capture_ts_utc": datetime.now(timezone.utc),
                        "event_id_vendor": "E1",
        "event_id_vendor_raw": "E1",
        "event_name_raw": "Away @ Home",
        "event_start_time_utc": None,
        "home_team": "HOME",
                        "away_team": "ANA",
                        "player_id_vendor": None,
                        "player_name_raw": "Test Player",
                        "market_type": "SOG",
                        "line": 1.5,
                        "side": "OVER",
                        "book_id_vendor": "BOOK",
                        "book_name_raw": "Book",
                        "odds_american": 110,
                        "odds_decimal": 2.1,
                        "odds_quoted_raw": "+110",
                        "odds_quoted_format": "american",
                        "odds_american_derived": False,
                        "odds_decimal_derived": True,
                        "is_live": False,
                        "raw_payload_path": "path",
                        "raw_payload_hash": "hash",
                    }
                ]
            )
            insert_odds_records(con, df)
            insert_odds_records(con, df)
            count = con.execute("SELECT COUNT(*) FROM fact_prop_odds").fetchone()[0]
            assert count == 1
        finally:
            con.close()


def test_vendor_utils_caps(monkeypatch):
    assert MAX_RETRIES <= 3
    assert get_timeout_tuple(15) == (10, 15)
    monkeypatch.setenv("FORCE_VENDOR_FAILURE", "UNABATED")
    assert should_force_vendor_failure("UNABATED") is True


def test_run_daily_no_side_effects(monkeypatch):
    from pipelines.ops import run_daily

    run_date = datetime.now(timezone.utc).date().isoformat()
    log_path = os.path.join(run_daily.ROOT_DIR, "outputs", "monitoring", f"daily_run_{run_date}.md")
    prev_mtime = os.path.getmtime(log_path) if os.path.exists(log_path) else None

    monkeypatch.setattr(sys, "argv", ["run_daily.py"])
    exit_code = run_daily.main()
    assert exit_code == 0

    if os.path.exists(log_path):
        assert prev_mtime == os.path.getmtime(log_path)

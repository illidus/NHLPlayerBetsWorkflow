import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from nhl_bets.scrapers.unabated_client import UnabatedClient
from nhl_bets.common.db_init import initialize_phase11_tables, insert_odds_records, insert_unabated_metadata


def load_fixture():
    fixture_path = Path(__file__).parent / "fixtures" / "unabated_fixture.json"
    with fixture_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def test_unabated_side_and_odds_mapping_fixture():
    client = UnabatedClient()
    data = load_fixture()

    capture_ts = datetime(2026, 1, 6, 12, 0, 0, tzinfo=timezone.utc)
    parsed = client.parse_snapshot(data, "raw/path", "hash123", capture_ts)

    odds = parsed["odds"]
    assert len(odds) == 2

    over = next(r for r in odds if r["side"] == "OVER")
    under = next(r for r in odds if r["side"] == "UNDER")

    assert over["vendor_outcome_key"] == "si0:pid12345"
    assert over["odds_american"] == -188
    assert round(over["odds_decimal"], 4) == 1.5319
    assert over["player_team"] == "DAL"

    assert under["vendor_outcome_key"] == "si1:pid12345"
    assert under["odds_american"] == 142
    assert round(under["odds_decimal"], 4) == 2.42


def test_unabated_event_start_time_utc_no_shift():
    client = UnabatedClient()
    data = load_fixture()

    capture_ts = datetime(2026, 1, 6, 12, 0, 0, tzinfo=timezone.utc)
    parsed = client.parse_snapshot(data, "raw/path", "hash123", capture_ts)

    con = duckdb.connect(":memory:")
    initialize_phase11_tables(con)
    insert_odds_records(con, pd.DataFrame(parsed["odds"]))

    stored = con.execute(
        "SELECT event_start_time_utc FROM fact_prop_odds LIMIT 1"
    ).fetchone()[0]
    assert stored == datetime(2026, 1, 7, 0, 0, 0)


def test_unabated_metadata_upsert_idempotent():
    client = UnabatedClient()
    data = load_fixture()

    capture_ts = datetime(2026, 1, 6, 12, 0, 0, tzinfo=timezone.utc)
    parsed = client.parse_snapshot(data, "raw/path", "hash123", capture_ts)

    con = duckdb.connect(":memory:")
    initialize_phase11_tables(con)

    events_df = pd.DataFrame(parsed["events"])
    players_df = pd.DataFrame(parsed["players"])

    insert_unabated_metadata(con, events_df, players_df)
    insert_unabated_metadata(con, events_df, players_df)

    event_count = con.execute("SELECT count(*) FROM dim_events_unabated").fetchone()[0]
    player_row = con.execute("SELECT team_abbr FROM dim_players_unabated").fetchone()[0]

    assert event_count == 1
    assert player_row == "DAL"

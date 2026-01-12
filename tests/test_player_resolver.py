import pytest
import duckdb
import os
import sys
import json
from datetime import datetime, date

sys.path.append(os.getcwd())

from src.nhl_bets.identity.player_resolver import PlayerResolver

@pytest.fixture
def db_path(tmp_path):
    d = tmp_path / "test.duckdb"
    return str(d)

@pytest.fixture
def resolver(db_path):
    # Setup Schema
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE IF NOT EXISTS dim_players (player_id VARCHAR, player_name_canonical VARCHAR, nhl_id BIGINT)")
    con.execute("CREATE TABLE IF NOT EXISTS dim_player_alias (alias_id VARCHAR, source_vendor VARCHAR, alias_text_norm VARCHAR, canonical_player_id VARCHAR, team_abbrev VARCHAR, season VARCHAR, match_confidence DOUBLE)")
    con.execute("CREATE TABLE IF NOT EXISTS stg_player_alias_review_queue (source_vendor VARCHAR, alias_text_raw VARCHAR, alias_text_norm VARCHAR, event_id_vendor VARCHAR, game_start_ts_utc TIMESTAMP, home_team_raw VARCHAR, away_team_raw VARCHAR, candidate_players_json JSON, resolution_notes VARCHAR)")
    con.execute("CREATE TABLE IF NOT EXISTS dim_team_roster_snapshot (team_abbrev VARCHAR, snapshot_date DATE, roster_json JSON)")
    
    # Seed Data
    con.execute("INSERT INTO dim_players VALUES ('p1', 'Connor McDavid', 123), ('p2', 'Leon Draisaitl', 456), ('p3', 'Sebastian Aho', 789)")
    con.execute("INSERT INTO dim_player_alias VALUES ('a1', 'THE_ODDS_API', 'connor mcdavid', 'p1', NULL, NULL, 1.0)")
    
    # Seed Roster Snapshot for EDM
    edm_roster = [
        {'player_id': 'p1', 'player_name_canonical': 'Connor McDavid', 'nhl_id': 123},
        {'player_id': 'p2', 'player_name_canonical': 'Leon Draisaitl', 'nhl_id': 456}
    ]
    con.execute("INSERT INTO dim_team_roster_snapshot VALUES ('EDM', '2023-01-01', ?)", [json.dumps(edm_roster)])
    
    con.close()
    
    return PlayerResolver(db_path, allow_unrostered_resolution=False) # Strict Mode Default

def test_normalize_name(resolver):
    assert resolver.normalize_name("Connor McDavid") == "connor mcdavid"

def test_resolve_exact_alias(resolver):
    # Alias Match (Should work even without roster if logic prioritizes aliases, which it does)
    pid, method, conf, note = resolver.resolve("Connor McDavid", "evt1", datetime(2023,1,1), "EDM", "TOR")
    assert pid == "p1"
    assert method == "ALIAS"

def test_resolve_strict_missing_roster(resolver):
    # Strict Mode, Missing Roster for Team X (Not seeded)
    pid, method, conf, note = resolver.resolve("Unknown Guy", "evt1", datetime(2023,1,1), "XXX", "YYY")
    assert pid is None
    assert note == "MISSING_ROSTER_SNAPSHOT"

def test_resolve_strict_with_roster(resolver):
    # Strict Mode, Roster Present for EDM
    # "Leon Draisaitl" is in the roster snapshot
    pid, method, conf, note = resolver.resolve("Leon Draisaitl", "evt1", datetime(2023,1,1), "EDM", "TOR")
    assert pid == "p2"
    assert method == "EXACT"

def test_resolve_strict_fuzzy_with_roster(resolver):
    # Fuzzy Match within Roster
    pid, method, conf, note = resolver.resolve("Leon Draisait", "evt1", datetime(2023,1,1), "EDM", "TOR") # Typo
    assert pid == "p2"
    assert method == "FUZZY"
    assert conf >= 0.90

def test_resolve_permissive_fallback(resolver):
    # Use the DB path from the fixture-initialized resolver
    # Re-init resolver in permissive mode pointing to the same DB
    permissive_resolver = PlayerResolver(resolver.db_path, allow_unrostered_resolution=True)
    
    # Even without roster for CAR, should find Sebastian Aho (p3) from dim_players
    pid, method, conf, note = permissive_resolver.resolve("Sebastian Aho", "evt1", datetime(2023,1,1), "CAR", "NYI")
    assert pid == "p3"
    assert "Exact" in note
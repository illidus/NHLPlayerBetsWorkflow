import pytest
import json
import os
from src.nhl_bets.odds_historical.normalize_phase11 import normalize_batch
from src.nhl_bets.odds_historical.team_codes import resolve_team_code

FIXTURE_HAPPY = "examples/phase11/fixture_happy_path.json"
FIXTURE_EDGE = "examples/phase11/fixture_edge_case.json"

def load_fixture(path):
    with open(path, 'r') as f:
        return json.load(f)

def test_team_code_resolver():
    assert resolve_team_code("St. Louis Blues") == "STL"
    assert resolve_team_code("NY Rangers") == "NYR"
    assert resolve_team_code("Montréal Canadiens") == "MTL"
    assert resolve_team_code("Unknown Squad") is None

def test_happy_path_normalization():
    data = load_fixture(FIXTURE_HAPPY)
    rows = normalize_batch(data)
    
    assert len(rows) == 3 # McDavid, Draisaitl, Matthews
    
    row0 = rows[0]
    assert row0['player_name_raw'] == "Connor McDavid"
    assert row0['market_type'] == "player_goals"
    assert row0['line'] == 0.5
    assert row0['side'] == "Over"
    assert row0['book_id_vendor'] == "draftkings"
    assert 'row_id' in row0
    
    # Join Key Checks
    assert row0['game_date'] == "2023-11-01"
    assert row0['home_team_raw'] == "Edmonton Oilers"
    assert row0['home_team_norm'] == "EDMONTON OILERS"
    assert row0['home_team_code'] == "EDM"
    assert row0['away_team_code'] == "DAL"
    
    # Match Keys
    assert row0['match_key'] == "2023-11-01|DALLAS STARS|EDMONTON OILERS"
    assert row0['match_key_code'] == "2023-11-01|DAL|EDM"

def test_edge_case_normalization():
    data = load_fixture(FIXTURE_EDGE)
    rows = normalize_batch(data)
    
    # Expect 1 valid row (No Line Guy, defaulting line?)
    # or handle gracefully
    assert len(rows) == 1
    assert rows[0]['player_name_raw'] == "No Line Guy"
    assert rows[0]['line'] == 0.5 # Default logic triggered
    
    # Missing teams in edge fixture -> match_key should be None
    assert rows[0].get('match_key') is None
    assert rows[0].get('match_key_code') is None
    assert rows[0].get('home_team_code') is None

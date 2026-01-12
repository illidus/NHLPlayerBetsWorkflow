import pytest
import duckdb
import os
from src.nhl_bets.odds_historical.match_phase11_to_games import match_phase11_rows

def test_match_logic_in_memory():
    # Setup in-memory DB
    con = duckdb.connect(':memory:')
    
    # 1. Create Fake Game Table
    con.execute("CREATE TABLE dim_games (game_id VARCHAR, game_date DATE, home_team VARCHAR, away_team VARCHAR)")
    con.execute("INSERT INTO dim_games VALUES ('g1', '2023-11-01', 'EDM', 'DAL')")
    con.execute("INSERT INTO dim_games VALUES ('g2', '2023-11-01', 'TOR', 'BOS')")
    
    # 2. Create Fake Phase 11 Table
    con.execute("CREATE TABLE fact_odds_historical_phase11 (row_id VARCHAR, match_key_code VARCHAR)")
    # Matches g1
    con.execute("INSERT INTO fact_odds_historical_phase11 VALUES ('r1', '2023-11-01|DAL|EDM')") 
    # Matches g2
    con.execute("INSERT INTO fact_odds_historical_phase11 VALUES ('r2', '2023-11-01|BOS|TOR')")
    # No Match (wrong date)
    con.execute("INSERT INTO fact_odds_historical_phase11 VALUES ('r3', '2023-11-02|DAL|EDM')")
    # Missing Key
    con.execute("INSERT INTO fact_odds_historical_phase11 VALUES ('r4', NULL)")
    
    # 3. Run Matching
    metrics = match_phase11_rows(con, "fact_odds_historical_phase11", game_table_candidates=["dim_games"])
    
    assert metrics['status'] == 'success'
    assert metrics['game_table_selected'] == 'dim_games'
    assert metrics['total_phase11_rows'] == 4
    assert metrics['matched_count'] == 2
    assert metrics['match_rate'] == 0.5
    
    # Verify breakdown
    # r3 -> no_key_match, r4 -> null_match_key_code
    # Wait, implementation uses:
    # WHEN p.match_key_code IS NULL THEN 'null_match_key_code'
    # ELSE 'no_key_match'
    # So r3 -> no_key_match, r4 -> null_match_key_code
    breakdown = metrics['unmatched_reasons_breakdown']
    assert breakdown.get('no_key_match') == 1
    assert breakdown.get('null_match_key_code') == 1

def test_match_logic_no_game_table():
    con = duckdb.connect(':memory:')
    con.execute("CREATE TABLE fact_odds_historical_phase11 (row_id VARCHAR)")
    
    metrics = match_phase11_rows(con, "fact_odds_historical_phase11", game_table_candidates=["non_existent_table"])
    assert metrics['status'] == 'no_game_table'

def test_match_logic_missing_columns():
    con = duckdb.connect(':memory:')
    # Missing home/away columns
    con.execute("CREATE TABLE bad_games (game_id VARCHAR, game_date DATE)")
    con.execute("INSERT INTO bad_games VALUES ('g1', '2023-11-01')")
    
    metrics = match_phase11_rows(con, "fact_odds_historical_phase11", game_table_candidates=["bad_games"])
    assert metrics['status'] == 'no_game_table' # Because it skips bad candidates
    assert "missing required columns" in metrics['notes'][0].lower()

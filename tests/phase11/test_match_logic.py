import pytest
import duckdb
import os
from src.nhl_bets.odds_historical.match_phase11_to_games import match_phase11_rows

def test_match_logic_in_memory():
    # Setup in-memory DB
    con = duckdb.connect(':memory:')
    
    # 1. Create Fake Game Table
    con.execute("CREATE TABLE dim_games (game_id VARCHAR, game_date DATE, home_team VARCHAR, away_team VARCHAR)")
    
    # Insert Games (Using Codes that match match_key_code format)
    con.execute("INSERT INTO dim_games VALUES ('g1', '2023-11-01', 'EDM', 'DAL')")
    con.execute("INSERT INTO dim_games VALUES ('g2', '2023-11-01', 'TOR', 'BOS')")
    
    # 2. Create Fake Phase 11 Table
    con.execute("CREATE TABLE fact_odds_historical_phase11 (row_id VARCHAR, match_key_code VARCHAR, game_date DATE)")
    
    # Row 1: Matches g1 (DAL @ EDM) -> Key: 2023-11-01|DAL|EDM
    con.execute("INSERT INTO fact_odds_historical_phase11 VALUES ('r1', '2023-11-01|DAL|EDM', '2023-11-01')") 
    
    # Row 2: Matches g2 (BOS @ TOR) -> Key: 2023-11-01|BOS|TOR
    con.execute("INSERT INTO fact_odds_historical_phase11 VALUES ('r2', '2023-11-01|BOS|TOR', '2023-11-01')")
    
    # Row 3: No Match (Wrong Date) -> Key: 2023-11-02|DAL|EDM
    con.execute("INSERT INTO fact_odds_historical_phase11 VALUES ('r3', '2023-11-02|DAL|EDM', '2023-11-02')")
    
    # Row 4: Missing Key
    con.execute("INSERT INTO fact_odds_historical_phase11 VALUES ('r4', NULL, '2023-11-01')")
    
    # 3. Run Matching
    metrics = match_phase11_rows(con, "fact_odds_historical_phase11", game_table_candidates=["dim_games"])
    
    assert metrics['status'] == 'success'
    assert metrics['match_rate'] == 0.5
    
    # Verify daily summary exists
    daily = metrics['daily_summary']
    assert len(daily) > 0
    # 2023-11-01 has 3 rows (r1, r2, r4), 2 matched, 1 null key
    # Fix: Date is normalized to string YYYY-MM-DD
    day1 = next(d for d in daily if d['date'] == '2023-11-01')
    assert day1['total'] == 3
    assert day1['matched'] == 2
    assert day1['with_key'] == 2 # r1, r2

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

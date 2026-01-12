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
    con.execute("CREATE TABLE fact_odds_historical_phase11 (row_id VARCHAR, match_key_code VARCHAR)")
    
    # Row 1: Matches g1 (DAL @ EDM) -> Key: 2023-11-01|DAL|EDM
    con.execute("INSERT INTO fact_odds_historical_phase11 VALUES ('r1', '2023-11-01|DAL|EDM')") 
    
    # Row 2: Matches g2 (BOS @ TOR) -> Key: 2023-11-01|BOS|TOR
    con.execute("INSERT INTO fact_odds_historical_phase11 VALUES ('r2', '2023-11-01|BOS|TOR')")
    
    # Row 3: No Match (Wrong Date) -> Key: 2023-11-02|DAL|EDM
    con.execute("INSERT INTO fact_odds_historical_phase11 VALUES ('r3', '2023-11-02|DAL|EDM')")
    
    # Row 4: Missing Key
    con.execute("INSERT INTO fact_odds_historical_phase11 VALUES ('r4', NULL)")
    
    # 3. Run Matching
    metrics = match_phase11_rows(con, "fact_odds_historical_phase11", game_table_candidates=["dim_games"])
    
    assert metrics['status'] == 'success'
    assert metrics['game_table_selected'] == 'dim_games'
    assert metrics['total_phase11_rows'] == 4
    assert metrics['matched_count'] == 2
    assert metrics['match_rate'] == 0.5
    
    # Verify reasons
    reasons = metrics['unmatched_reasons']
    assert reasons.get('No Match Found') == 1
    assert reasons.get('Missing Match Key') == 1
import duckdb

db_path = "data/db/nhl_backtest.duckdb"
con = duckdb.connect(db_path)

try:
    # Check fact_skater_game_all for Nico Sturm
    query = """
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT game_id) as unique_games,
        MIN(game_date) as first_game,
        MAX(game_date) as last_game,
        team
    FROM fact_skater_game_all
    WHERE player_id IN (SELECT player_id FROM dim_players WHERE player_name = 'Nico Sturm')
    GROUP BY team
    """
    print("--- Nico Sturm Stats in DB ---")
    print(con.execute(query).fetchdf().to_string())

    # Check Emmitt Finnie
    query2 = """
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT game_id) as unique_games,
        MIN(game_date) as first_game,
        MAX(game_date) as last_game,
        team
    FROM fact_skater_game_all
    WHERE player_id IN (SELECT player_id FROM dim_players WHERE player_name = 'Emmitt Finnie')
    GROUP BY team
    """
    print("\n--- Emmitt Finnie Stats in DB ---")
    print(con.execute(query2).fetchdf().to_string())

except Exception as e:
    print(f"Error: {e}")
finally:
    con.close()

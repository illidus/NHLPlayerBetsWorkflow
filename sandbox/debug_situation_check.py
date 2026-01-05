import duckdb

db_path = "data/db/nhl_backtest.duckdb"
con = duckdb.connect(db_path)

try:
    query = """
    SELECT 
        game_date,
        team,
        situation,
        toi_seconds/60.0 as toi,
        x_goals
    FROM fact_skater_game_situation
    WHERE player_id IN (SELECT player_id FROM dim_players WHERE player_name = 'Nico Sturm')
      AND situation = 'all'
      AND game_date >= '2024-09-01'
    ORDER BY game_date DESC
    """
    print("--- Nico Sturm Games in Situation Table (Since Sept 2024) ---")
    print(con.execute(query).fetchdf().to_string())

except Exception as e:
    print(f"Error: {e}")
finally:
    con.close()

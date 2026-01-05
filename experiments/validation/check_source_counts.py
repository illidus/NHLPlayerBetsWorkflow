
import duckdb

db_path = "data/db/nhl_backtest.duckdb"
con = duckdb.connect(db_path)

print("Checking fact_skater_game_situation:")
try:
    df = con.execute("SELECT MIN(game_date), MAX(game_date), COUNT(*) FROM fact_skater_game_situation").df()
    print(df)
    
    print("\nCounts by Season:")
    df_seas = con.execute("SELECT season, COUNT(*) FROM fact_skater_game_situation GROUP BY season").df()
    print(df_seas)
except Exception as e:
    print(e)


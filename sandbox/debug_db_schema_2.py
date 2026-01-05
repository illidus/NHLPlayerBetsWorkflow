
import duckdb

db_path = "data/db/nhl_backtest.duckdb"
con = duckdb.connect(db_path)

tables_to_inspect = [
    "fact_backtest_v2_clean",
    "dim_games",
    "fact_skater_game_all",
    "fact_team_defense_features"
]

for t in tables_to_inspect:
    print("Schema of '" + t + "':")
    try:
        df = con.execute("DESCRIBE " + t).df()
        # Print column_name and column_type for brevity
        print(df[['column_name', 'column_type']].to_string(index=False))
    except Exception as e:
        print(e)

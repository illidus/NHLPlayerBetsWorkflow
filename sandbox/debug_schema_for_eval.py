import duckdb
con = duckdb.connect('data/db/nhl_backtest.duckdb')
print("Tables:")
print(con.execute("SHOW TABLES").df())
print("\nfact_skater_game_all columns:")
print(con.execute("DESCRIBE fact_skater_game_all").df()[['column_name', 'column_type']])
con.close()

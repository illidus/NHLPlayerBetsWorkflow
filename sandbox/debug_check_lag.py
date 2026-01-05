import duckdb
import pandas as pd

con = duckdb.connect('data/db/nhl_backtest.duckdb')

print("--- Raw Games (Ovechkin) ---")
print(con.execute("SELECT game_date, goals FROM fact_skater_game_situation WHERE player_id=8471214 AND situation='all' ORDER BY game_date DESC LIMIT 5").df())

print("\n--- Features (Ovechkin) ---")
print(con.execute("SELECT game_date, goals_per_game_L10 FROM fact_player_game_features WHERE player_id=8471214 ORDER BY game_date DESC LIMIT 5").df())

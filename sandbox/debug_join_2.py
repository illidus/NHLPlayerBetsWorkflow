import duckdb

con = duckdb.connect('data/db/nhl_backtest.duckdb')

pid = 8476432
date = '2024-10-17'

print(f"Checking {pid} on {date}")

print("Odds:", con.sql(f"SELECT * FROM fact_odds_props WHERE player_id = {pid} AND game_date = '{date}'").df())
print("Probs:", con.sql(f"SELECT * FROM fact_probabilities WHERE player_id = {pid} AND CAST(game_date AS DATE) = '{date}'").df())

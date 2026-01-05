import duckdb

con = duckdb.connect('data/db/nhl_backtest.duckdb')

query = """
SELECT COUNT(*) 
FROM fact_odds_props m
JOIN fact_probabilities p 
ON m.player_id = p.player_id 
AND CAST(m.game_date AS DATE) = CAST(p.game_date AS DATE)
AND m.market = p.market
AND CAST(FLOOR(m.line) + 1 AS BIGINT) = p.line
WHERE m.game_date BETWEEN '2024-10-01' AND '2024-12-31'
"""

print("Join count:", con.sql(query).fetchall())

# distinct markets in both
print("Odds markets:", con.sql("SELECT DISTINCT market FROM fact_odds_props").fetchall())
print("Probs markets:", con.sql("SELECT DISTINCT market FROM fact_probabilities").fetchall())

# check one player match
print("Odds sample player:", con.sql("SELECT player_id, game_date, market, line FROM fact_odds_props LIMIT 1").fetchall())
pid = con.sql("SELECT player_id FROM fact_odds_props LIMIT 1").fetchone()[0]
print(f"Probs for player {pid}:", con.sql(f"SELECT player_id, game_date, market, line FROM fact_probabilities WHERE player_id = {pid} LIMIT 5").fetchall())


import duckdb

db_path = "data/db/nhl_backtest.duckdb"
con = duckdb.connect(db_path)

# List tables
print("Tables:")
tables = con.execute("SHOW TABLES").fetchall()
for t in tables:
    print(t[0])

# Inspect likely relevant tables
print("\nSchema of 'fact_probs_v2' (likely source of backtest data):")
try:
    print(con.execute("DESCRIBE fact_probs_v2").df())
except Exception as e:
    print(e)

print("\nSchema of 'fact_outcomes' (for actuals):")
try:
    print(con.execute("DESCRIBE fact_outcomes").df())
except:
    print("fact_outcomes not found")

print("\nSchema of 'dim_schedule' (for rest days):")
try:
    print(con.execute("DESCRIBE dim_schedule").df())
except:
    print("dim_schedule not found")

print("\nSchema of 'features_team_defense' (for SA60/Rest potentially):")
try:
    print(con.execute("DESCRIBE features_team_defense").df())
except:
    print("features_team_defense not found")

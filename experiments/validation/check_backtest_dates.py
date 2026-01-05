
import duckdb

db_path = "data/db/nhl_backtest.duckdb"
con = duckdb.connect(db_path)

query = """
SELECT 
    MIN(game_date) as start_date,
    MAX(game_date) as end_date,
    COUNT(DISTINCT game_date) as unique_days,
    COUNT(*) as total_bets
FROM fact_backtest_v2_clean
"""

print("Backtest Date Range Analysis:")
try:
    df = con.execute(query).df()
    print(df)
except Exception as e:
    print(f"Error: {e}")

# check specific dates if count is low
query_dates = "SELECT DISTINCT game_date FROM fact_backtest_v2_clean ORDER BY game_date"
try:
    df_dates = con.execute(query_dates).df()
    print("\nDistinct Dates Found:")
    print(df_dates)
except Exception as e:
    print(f"Error: {e}")

import duckdb
import json

db_path = "data/db/test_phase12.duckdb"
con = duckdb.connect(db_path)

print("--- Fact Prop Odds (ROI) ---")
roi_rows = con.execute("SELECT count(*), count(DISTINCT raw_payload_hash || player_name_raw || market_type || book_id_vendor) FROM fact_prop_odds").fetchall()
print(f"Total Rows: {roi_rows[0][0]}")
print(f"Unique Rows: {roi_rows[0][1]}")

print("\n--- Unresolved Staging ---")
unresolved_rows = con.execute("SELECT count(*), count(DISTINCT raw_payload_hash || player_name_raw || market_type || book_id_vendor) FROM stg_prop_odds_unresolved").fetchall()
print(f"Total Rows: {unresolved_rows[0][0]}")
print(f"Unique Rows: {unresolved_rows[0][1]}")

print("\n--- Unresolved Content ---")
content = con.execute("SELECT player_name_raw, failure_reasons FROM stg_prop_odds_unresolved").fetchall()
for row in content:
    print(f"Player: {row[0]}, Reasons: {row[1]}")

con.close()


import duckdb
import pandas as pd

DB_PATH = 'data/db/nhl_backtest.duckdb'

def check_db():
    con = duckdb.connect(DB_PATH)
    
    print("--- Tables ---")
    tables = con.execute("SHOW TABLES").fetchall()
    for t in tables:
        print(t[0])
        
    print("\n--- Row Counts ---")
    try:
        print("fact_odds_props:", con.execute("SELECT COUNT(*) FROM fact_odds_props").fetchone()[0])
        print("Sample fact_odds_props date:", con.execute("SELECT MIN(game_date), MAX(game_date) FROM fact_odds_props").fetchall())
    except Exception as e:
        print("Error reading fact_odds_props:", e)

    try:
        print("fact_probabilities:", con.execute("SELECT COUNT(*) FROM fact_probabilities").fetchone()[0])
    except Exception as e:
        print("Error reading fact_probabilities:", e)

    try:
        print("fact_probabilities_calibrated:", con.execute("SELECT COUNT(*) FROM fact_probabilities_calibrated").fetchone()[0])
    except Exception as e:
        print("Error reading fact_probabilities_calibrated:", e)

    try:
        print("fact_skater_game_all:", con.execute("SELECT COUNT(*) FROM fact_skater_game_all").fetchone()[0])
    except Exception as e:
        print("Error reading fact_skater_game_all:", e)

    print("\n--- Sample Join Check (Odds vs Probs) ---")
    try:
        query = """
        SELECT count(*)
        FROM fact_odds_props m
        JOIN fact_probabilities p 
        ON (
            m.player_id = p.player_id 
            AND CAST(m.game_date AS DATE) = CAST(p.game_date AS DATE)
            AND m.market = p.market
            AND CAST(FLOOR(m.line) + 1 AS BIGINT) = p.line
        )
        """
        print("Matching rows (Odds <-> Probs):", con.execute(query).fetchone()[0])
    except Exception as e:
        print("Error checking join:", e)

    print("\n--- Side Column Check ---")
    try:
        print("Unique sides:", con.execute("SELECT DISTINCT side FROM fact_odds_props").fetchall())
    except Exception as e:
        print("Error checking sides:", e)

    print("\n--- Accurate EV Simulation ---")
    try:
        query = """
        WITH matched AS (
            SELECT 
                m.odds_decimal,
                m.side,
                p.p_over as p_over_val
            FROM fact_odds_props m
            JOIN fact_probabilities p 
            ON (
                m.player_id = p.player_id 
                AND CAST(m.game_date AS DATE) = CAST(p.game_date AS DATE)
                AND m.market = p.market
                AND CAST(FLOOR(m.line) + 1 AS BIGINT) = p.line
            )
            WHERE m.game_date BETWEEN '2023-10-01' AND '2026-01-01'
              AND m.market != 'Goal Scorer'
        ),
        with_prob AS (
            SELECT 
                *,
                CASE 
                    WHEN side = 'OVER' THEN p_over_val
                    WHEN side = 'UNDER' THEN (1.0 - p_over_val)
                    ELSE 0
                END as model_prob
            FROM matched
        )
        SELECT 
            COUNT(*) as total_matched,
            COUNT(CASE WHEN (model_prob * odds_decimal) - 1 > 0.05 THEN 1 END) as ev_positive_count,
            MAX((model_prob * odds_decimal) - 1) as max_ev
        FROM with_prob
        """
        print(con.execute(query).fetchall())
    except Exception as e:
        print("Error simulating EV:", e)

    con.close()

if __name__ == "__main__":
    check_db()

import duckdb
import pandas as pd
import argparse
import sys
import os

def validate_snapshots(db_path):
    conn = duckdb.connect(db_path)
    
    print("Validating probability snapshots...")
    
    # 1. Basic Counts
    counts = conn.sql("""
        SELECT 
            (SELECT COUNT(*) FROM fact_model_mu) as mu_count,
            (SELECT COUNT(*) FROM fact_probabilities) as prob_count,
            (SELECT COUNT(DISTINCT game_id) FROM fact_probabilities) as games_covered
    """).fetchone()
    
    print(f"Rows in fact_model_mu: {counts[0]}")
    print(f"Rows in fact_probabilities: {counts[1]}")
    print(f"Games covered: {counts[2]}")
    
    # 2. Value Range Check
    range_check = conn.sql("""
        SELECT COUNT(*) 
        FROM fact_probabilities 
        WHERE p_over < 0 OR p_over > 1
    """).fetchone()[0]
    
    if range_check > 0:
        print(f"ERROR: Found {range_check} probabilities outside [0,1].")
    else:
        print("PASS: All probabilities in [0,1].")
        
    # 3. Monotonicity Check
    # For a given (player, game, market), line x < line y implies p_over(x) >= p_over(y)
    # We check if there exists any case where line1 < line2 AND prob1 < prob2
    
    mono_check = conn.sql("""
        WITH ordered_probs AS (
            SELECT 
                player_id, game_id, market, line, p_over,
                LEAD(line) OVER (PARTITION BY player_id, game_id, market ORDER BY line) as next_line,
                LEAD(p_over) OVER (PARTITION BY player_id, game_id, market ORDER BY line) as next_prob
            FROM fact_probabilities
        )
        SELECT COUNT(*)
        FROM ordered_probs
        WHERE next_line IS NOT NULL 
          AND next_prob > p_over
          AND next_line > line -- Should be implicitly true by ORDER BY
    """).fetchone()[0]
    
    if mono_check > 0:
        print(f"ERROR: Found {mono_check} monotonicity violations.")
    else:
        print("PASS: Monotonicity check passed.")
        
    # 4. Mu Sanity
    mu_stats = conn.sql("""
        SELECT 
            MIN(mu_goals) as min_mu_g, MAX(mu_goals) as max_mu_g,
            MIN(mu_sog) as min_mu_sog, MAX(mu_sog) as max_mu_sog,
            COUNT(*) FILTER (WHERE mu_goals IS NULL OR mu_sog IS NULL) as null_mus
        FROM fact_model_mu
    """).fetchone()
    
    print(f"Mu Goals Range: {mu_stats[0]} - {mu_stats[1]}")
    print(f"Mu SOG Range: {mu_stats[2]} - {mu_stats[3]}")
    if mu_stats[4] > 0:
        print(f"WARNING: Found {mu_stats[4]} NULL mu records.")
    else:
        print("PASS: No NULL mus.")

    # 5. Generate Report
    report_data = {
        'metric': ['row_count_mu', 'row_count_probs', 'games_covered', 'range_violations', 'monotonicity_violations', 'null_mus'],
        'value': [counts[0], counts[1], counts[2], range_check, mono_check, mu_stats[4]],
        'status': [
            'INFO', 'INFO', 'INFO', 
            'FAIL' if range_check > 0 else 'PASS', 
            'FAIL' if mono_check > 0 else 'PASS',
            'FAIL' if mu_stats[4] > 0 else 'PASS'
        ]
    }
    
    # Export Report
    df_report = pd.DataFrame(report_data)
    report_path = "outputs/backtest_reports/probability_snapshot_inventory.csv"
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    df_report.to_csv(report_path, index=False)
    print(f"Validation report saved to {report_path}")
    
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb-path", default="data/db/nhl_backtest.duckdb")
    args = parser.parse_args()
    
    validate_snapshots(args.duckdb_path)

import duckdb
import pandas as pd
import argparse
import os
from datetime import datetime

DB_PATH = 'data/db/nhl_backtest.duckdb'

def run_backtest(start_date, end_date, min_ev, stake, prob_source, skip_months, output_table=None):
    con = duckdb.connect(DB_PATH)

    # Optimization
    con.execute("SET memory_limit = '8GB'")
    con.execute("SET threads = 8")
    con.execute("SET temp_directory = './duckdb_temp/'")
    
    table_suffix = "calibrated" if prob_source == "calibrated" else "baseline"
    
    if output_table:
        table_name = output_table
    else:
        table_name = "fact_backtest_bets_" + table_suffix
    
    # Ensure tables exist
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        bet_id VARCHAR,
        game_date DATE,
        player_name VARCHAR,
        player_id BIGINT,
        team VARCHAR,
        market VARCHAR,
        line DOUBLE,
        side VARCHAR,
        odds_decimal DOUBLE,
        p_over_baseline DOUBLE,
        p_over_calibrated DOUBLE,
        model_prob DOUBLE,
        ev DOUBLE,
        stake DOUBLE,
        actual_value DOUBLE,
        result VARCHAR,
        profit DOUBLE,
        run_ts TIMESTAMP
    )
    """)
    
    msg = f"Running backtest from {start_date} to {end_date} with EV>{min_ev} using {prob_source} probabilities"
    if skip_months:
        msg += f" (Skipping months: {skip_months})"
    print(msg)
    
    if prob_source == "calibrated":
        prob_table = "fact_probabilities_calibrated"
        # In calibrated table, we have p_over_baseline and p_over_calibrated
        # We use p_over_calibrated for the decision (p_over_val)
        prob_select = """
            p.p_over_calibrated as p_over_val,
            p.p_over_baseline as p_over_baseline_val,
            p.p_over_calibrated as p_over_calibrated_val,
            p.game_id
        """
    else:
        prob_table = "fact_probabilities"
        # In baseline table, we only have p_over
        # We use p_over for the decision
        prob_select = """
            p.p_over as p_over_val,
            p.p_over as p_over_baseline_val,
            NULL as p_over_calibrated_val,
            p.game_id
        """
    
    # Month Filter for SQL
    month_filter = ""
    if skip_months:
        months_str = ",".join(map(str, skip_months))
        month_filter = f"AND extract(month from o.game_date) NOT IN ({months_str})"
    
    # --- DEBUG SECTION ---
    print(f"--- Debugging {prob_source} ---")
    
    # 1. Check raw odds count in range
    debug_odds = con.execute(f"""
        SELECT COUNT(*) FROM fact_odds_props o
        WHERE o.game_date BETWEEN '{start_date}' AND '{end_date}'
          AND o.market != 'Goal Scorer'
          {month_filter}
    """).fetchone()[0]
    print(f"Odds in date range: {debug_odds}")

    # 2. Check join count
    debug_join = con.execute(f"""
        SELECT COUNT(*) 
        FROM fact_odds_props o
        JOIN {prob_table} p 
        ON (
            o.player_id = p.player_id 
            AND CAST(o.game_date AS DATE) = CAST(p.game_date AS DATE)
            AND o.market = p.market
            AND CAST(FLOOR(o.line) + 1 AS BIGINT) = p.line
        )
        WHERE o.game_date BETWEEN '{start_date}' AND '{end_date}'
          AND o.market != 'Goal Scorer'
          {month_filter}
    """).fetchone()[0]
    print(f"Odds matching probabilities: {debug_join}")
    
    # 3. Check EV stats if matches exist
    if debug_join > 0:
        if prob_source == "calibrated":
            p_col = "p.p_over_calibrated"
        else:
            p_col = "p.p_over"
            
        debug_ev = con.execute(f"""
            WITH raw AS (
                SELECT 
                    o.odds_decimal,
                    o.side,
                    {p_col} as prob
                FROM fact_odds_props o
                JOIN {prob_table} p 
                ON (
                    o.player_id = p.player_id 
                    AND CAST(o.game_date AS DATE) = CAST(p.game_date AS DATE)
                    AND o.market = p.market
                    AND CAST(FLOOR(o.line) + 1 AS BIGINT) = p.line
                )
                WHERE o.game_date BETWEEN '{start_date}' AND '{end_date}'
                  AND o.market != 'Goal Scorer'
                  {month_filter}
            )
            SELECT 
                AVG((prob * odds_decimal) - 1) as avg_ev,
                MAX((prob * odds_decimal) - 1) as max_ev
            FROM raw
        """).fetchone()
        print(f"Avg EV: {debug_ev[0]}, Max EV: {debug_ev[1]}")
    # --- END DEBUG SECTION ---

    query = f"""
    WITH matched_odds AS (
        SELECT 
            o.game_date,
            o.player_name,
            o.player_id,
            o.team,
            o.market,
            o.line as line_val,
            o.side,
            o.odds_decimal
        FROM fact_odds_props o
        WHERE o.game_date BETWEEN '{start_date}' AND '{end_date}'
          AND o.market != 'Goal Scorer' -- Exclude bad market
          {month_filter}
    ),
    
    with_probs AS (
        SELECT 
            m.*,
            {prob_select}
        FROM matched_odds m
        LEFT JOIN {prob_table} p 
        ON (
            m.player_id = p.player_id 
            AND CAST(m.game_date AS DATE) = CAST(p.game_date AS DATE) -- Join on Date
            AND m.market = p.market
            AND CAST(FLOOR(m.line_val) + 1 AS BIGINT) = p.line
        )
    ),
    
    with_ev AS (
        SELECT 
            *,
            CASE 
                WHEN UPPER(side) = 'OVER' THEN p_over_val
                WHEN UPPER(side) = 'UNDER' THEN (1.0 - p_over_val)
                ELSE NULL
            END as model_prob
        FROM with_probs
        WHERE p_over_val IS NOT NULL
    ),
    
    calculated AS (
        SELECT 
            *,
            (model_prob * odds_decimal) - 1.0 as ev
        FROM with_ev
        WHERE model_prob IS NOT NULL
    ),
    
    filtered_bets AS (
        SELECT * FROM calculated WHERE ev > {min_ev}
    ),
    
    with_results AS (
        SELECT 
            b.*,
            CASE 
                WHEN b.market = 'GOALS' THEN s.goals
                WHEN b.market = 'ASSISTS' THEN s.assists
                WHEN b.market = 'POINTS' THEN s.points
                WHEN b.market = 'SOG' THEN s.sog
                WHEN b.market = 'BLOCKS' THEN s.blocks
                ELSE NULL
            END as actual_value
        FROM filtered_bets b
        LEFT JOIN fact_skater_game_all s
        ON (
            b.player_id = s.player_id
            AND b.game_id = s.game_id -- Strict join on game_id for settlement now possible
        )
    )
    
    SELECT 
        md5(concat(player_name, CAST(game_date AS VARCHAR), market, CAST(line_val AS VARCHAR), side)) as bet_id,
        game_date,
        player_name,
        player_id,
        team,
        market,
        line_val as line,
        side,
        odds_decimal,
        p_over_baseline_val as p_over_baseline,
        p_over_calibrated_val as p_over_calibrated,
        model_prob,
        ev,
        {stake} as stake,
        actual_value,
        CASE 
            WHEN actual_value IS NULL THEN 'PENDING'
            WHEN UPPER(side) = 'OVER' AND actual_value > line_val THEN 'WIN' -- Assuming line_val is e.g. 0.5, 1.5, etc.
            WHEN UPPER(side) = 'UNDER' AND actual_value < line_val THEN 'WIN'
            ELSE 'LOSS'
        END as result,
        CASE 
            WHEN actual_value IS NULL THEN 0
            WHEN UPPER(side) = 'OVER' AND actual_value > line_val THEN ({stake} * (odds_decimal - 1))
            WHEN UPPER(side) = 'UNDER' AND actual_value < line_val THEN ({stake} * (odds_decimal - 1))
            ELSE -{stake}
        END as profit,
        CURRENT_TIMESTAMP as run_ts
    FROM with_results
    WHERE market NOT IN ('PPP') 
    """
    
    con.execute(f"INSERT INTO {table_name} {query}")
    print("Backtest simulation complete.")
    
    # Export Report
    df_res = con.execute(f"SELECT * FROM {table_name} ORDER BY game_date, ev DESC").df()
    
    out_path = f'outputs/backtest_reports/{table_name}.csv'
    df_res.to_csv(out_path, index=False)
    print(f"Saved results to {out_path}")
    
    # Summary
    print("\nSummary:")
    if not df_res.empty:
        print(df_res.groupby('result')['profit'].sum())
        print(f"Total Profit: {df_res['profit'].sum():.2f}")
        
        # Profitability Summary
        summary = df_res.groupby('market').agg({
            'profit': 'sum',
            'stake': 'sum',
            'bet_id': 'count'
        }).reset_index()
        summary['roi'] = summary['profit'] / summary['stake']
        summary.to_csv(f'outputs/backtest_reports/backtest_profitability_summary_{table_suffix}.csv', index=False)
    else:
        print("No bets found.")
    
    con.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2023-10-01")
    parser.add_argument("--end", default="2026-01-01")
    parser.add_argument("--ev-threshold", type=float, default=0.05, dest="ev")
    parser.add_argument("--stake", type=float, default=100.0)
    parser.add_argument("--prob-source", default="baseline", choices=["baseline", "calibrated"])
    parser.add_argument("--skip-months", type=int, nargs='+', help="List of month integers to skip (e.g. 10 for October)")
    parser.add_argument("--output-table", default=None, help="Override output table name")
    args = parser.parse_args()
    
    run_backtest(args.start, args.end, args.ev, args.stake, args.prob_source, args.skip_months, args.output_table)

import duckdb
import argparse
import sys

def build_goalie_features(db_path, start_season=None, end_season=None, force=False):
    conn = duckdb.connect(db_path)
    
    # Enable performance pragmas
    conn.execute("SET memory_limit = '8GB';")
    conn.execute("SET threads = 8;")
    conn.execute("SET temp_directory = './duckdb_temp/';")

    if not force:
        tables = conn.sql("SHOW TABLES").fetchall()
        if ('fact_goalie_features',) in tables:
            print("Table 'fact_goalie_features' already exists. Use --force to overwrite.")
            return

    print("Building fact_goalie_features...")
    
    season_filter = ""
    if start_season:
        season_filter += f" AND season >= {start_season}"
    if end_season:
        season_filter += f" AND season <= {end_season}"

    query = """
    CREATE OR REPLACE TABLE fact_goalie_features AS
    WITH goalie_games AS (
        SELECT
            player_id as goalie_id,
            game_id,
            game_date,
            season,
            team,
            goals_against,
            x_goals_against,
            toi_seconds
        FROM fact_goalie_game_situation
        WHERE situation = 'all'
    ),
    rolling_sums AS (
        SELECT
            goalie_id,
            game_id,
            game_date,
            season,
            team,
            
            -- L10 Rolling Sums (Excluding current game)
            SUM(goals_against) OVER (
                PARTITION BY goalie_id 
                ORDER BY game_date 
                ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
            ) as sum_ga_L10,
            
            SUM(x_goals_against) OVER (
                PARTITION BY goalie_id 
                ORDER BY game_date 
                ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
            ) as sum_xga_L10,
            
            SUM(toi_seconds) OVER (
                PARTITION BY goalie_id 
                ORDER BY game_date 
                ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
            ) as sum_toi_L10

        FROM goalie_games
    ),
    calc_metrics AS (
        SELECT
            *,
            -- Safe division for GSAx60
            -- Formula: (Sum_xGA_L10 - Sum_GA_L10) / (Sum_TOI_L10 / 3600)
            CASE 
                WHEN sum_toi_L10 IS NULL OR sum_toi_L10 = 0 THEN 0 
                ELSE (sum_xga_L10 - sum_ga_L10) / (sum_toi_L10 / 3600)
            END as goalie_gsax60_L10
        FROM rolling_sums
    )
    SELECT
        *
    FROM calc_metrics
    WHERE 1=1 {season_filter}
    """
    
    formatted_query = query.format(season_filter=season_filter)
    
    try:
        conn.execute(formatted_query)
        count = conn.sql("SELECT COUNT(*) FROM fact_goalie_features").fetchone()[0]
        print(f"Created fact_goalie_features with {count} rows.")
    except Exception as e:
        print(f"Error executing query: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-season", type=int)
    parser.add_argument("--end-season", type=int)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    db_path = "data/db/nhl_backtest.duckdb"
    build_goalie_features(db_path, args.start_season, args.end_season, args.force)

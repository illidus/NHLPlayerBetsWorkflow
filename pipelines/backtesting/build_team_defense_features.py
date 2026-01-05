import duckdb
import argparse
import sys

def build_team_defense_features(db_path, start_season=None, end_season=None, force=False):
    conn = duckdb.connect(db_path)
    
    if not force:
        tables = conn.sql("SHOW TABLES").fetchall()
        if ('fact_team_defense_features',) in tables:
            print("Table 'fact_team_defense_features' already exists. Use --force to overwrite.")
            return

    print("Building fact_team_defense_features...")
    
    season_filter = ""
    if start_season:
        season_filter += f" AND season >= {start_season}"
    if end_season:
        season_filter += f" AND season <= {end_season}"

    # Logic:
    # 1. Aggregate goalie stats to get Team Game Totals (for games where situation='all')
    # 2. Calculate rolling sums of SA, GA, xGA, TOI over last 10 games per team.
    # 3. Calculate rates.

    query = """
    CREATE OR REPLACE TABLE fact_team_defense_features AS
    WITH team_game_stats AS (
        SELECT
            team,
            game_id,
            game_date,
            season,
            SUM(shots_against) as team_sa,
            SUM(goals_against) as team_ga,
            SUM(x_goals_against) as team_xga,
            SUM(toi_seconds) as team_toi_seconds
        FROM fact_goalie_game_situation
        WHERE situation = 'all'
        GROUP BY team, game_id, game_date, season
    ),
    rolling_team_stats AS (
        SELECT
            team,
            game_id,
            game_date,
            season,
            
            -- Rolling Sums L10
            SUM(team_sa) OVER (PARTITION BY team ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as sum_sa_L10,
            SUM(team_ga) OVER (PARTITION BY team ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as sum_ga_L10,
            SUM(team_xga) OVER (PARTITION BY team ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as sum_xga_L10,
            SUM(team_toi_seconds) OVER (PARTITION BY team ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as sum_toi_seconds_L10
        FROM team_game_stats
    )
    SELECT
        team,
        game_id,
        game_date,
        season,
        
        -- Calculate Rates per 60
        CASE 
            WHEN sum_toi_seconds_L10 > 0 THEN (sum_sa_L10 / (sum_toi_seconds_L10 / 3600)) 
            ELSE NULL 
        END as opp_sa60_L10,
        
        CASE 
            WHEN sum_toi_seconds_L10 > 0 THEN (sum_xga_L10 / (sum_toi_seconds_L10 / 3600)) 
            ELSE NULL 
        END as opp_xga60_L10,
        
        CASE 
            WHEN sum_toi_seconds_L10 > 0 THEN (sum_ga_L10 / (sum_toi_seconds_L10 / 3600)) 
            ELSE NULL 
        END as opp_goals_against_L10,
        
        -- Also raw avg for reference if needed, but prompt asked for specific metrics.
        -- Let's stick to the requested ones.
        sum_ga_L10 / 10.0 as opp_goals_against_per_game_L10_raw

    FROM rolling_team_stats
    WHERE 1=1 {season_filter}
    """
    
    formatted_query = query.format(season_filter=season_filter)
    conn.execute(formatted_query)
    
    count = conn.sql("SELECT COUNT(*) FROM fact_team_defense_features").fetchone()[0]
    print(f"Created fact_team_defense_features with {count} rows.")
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-season", type=int)
    parser.add_argument("--end-season", type=int)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    db_path = "data/db/nhl_backtest.duckdb"
    build_team_defense_features(db_path, args.start_season, args.end_season, args.force)

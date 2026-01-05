import duckdb
import argparse
import sys
from pathlib import Path

def build_player_features(db_path, start_season=None, end_season=None, force=False):
    conn = duckdb.connect(db_path)
    
    if not force:
        tables = conn.sql("SHOW TABLES").fetchall()
        if ('fact_player_game_features',) in tables:
            print("Table 'fact_player_game_features' already exists. Use --force to overwrite.")
            return

    print("Building fact_player_game_features (Enhanced)...")

    season_filter = ""
    if start_season:
        season_filter += f" AND season >= {start_season}"
    if end_season:
        season_filter += f" AND season <= {end_season}"

    query = """
    CREATE OR REPLACE TABLE fact_player_game_features AS
    WITH player_game_base AS (
        SELECT 
            player_id,
            game_id,
            game_date,
            season,
            team,
            opp_team,
            home_or_away,
            position,
            goals,
            primary_assists + secondary_assists as assists,
            primary_assists,
            points,
            sog,
            blocks,
            shot_attempts,
            toi_seconds / 60.0 as toi_minutes,
            x_goals,
            on_ice_xgoals,
            on_ice_goals
        FROM fact_skater_game_situation
        WHERE situation = 'all'
    ),
    player_ev_stats AS (
        SELECT 
            player_id, 
            game_id, 
            primary_assists + secondary_assists as ev_assists, 
            points as ev_points, 
            toi_seconds / 60.0 as ev_toi_minutes,
            x_goals as ev_xgoals,
            on_ice_xgoals as ev_on_ice_xgoals,
            on_ice_goals as ev_on_ice_goals
        FROM fact_skater_game_situation 
        WHERE situation = '5on5'
    ),
    player_pp_stats AS (
        SELECT 
            player_id, 
            game_id, 
            primary_assists + secondary_assists as pp_assists, 
            points as pp_points, 
            toi_seconds / 60.0 as pp_toi_minutes,
            x_goals as pp_xgoals,
            on_ice_xgoals as pp_on_ice_xgoals,
            on_ice_goals as pp_on_ice_goals
        FROM fact_skater_game_situation 
        WHERE situation = '5on4'
    ),
    team_pp_totals AS (
        -- For each team-game, what was the total PP xG and PP Time?
        -- We take the MAX of on_ice_xgoals and toi_seconds for any player on the 5on4 situation 
        -- This isn't perfect if there are two units, but it's a good proxy for "PP environment" 
        -- Better: Sum of all goals in that situation? MoneyPuck doesn't give team stats directly here.
        -- Actually, for a given game and team and situation, the team total goals is constant.
        -- Let's just group by game_id, team, situation and take the first value of on_ice_goals.
        SELECT 
            game_id, 
            team, 
            MAX(on_ice_xgoals) as team_pp_xgoals,
            MAX(toi_seconds / 60.0) as team_pp_toi_minutes
        FROM fact_skater_game_situation
        WHERE situation = '5on4'
        GROUP BY game_id, team
    ),
    merged_stats AS (
        SELECT
            b.*,
            COALESCE(e.ev_assists, 0) as ev_assists,
            COALESCE(e.ev_points, 0) as ev_points,
            COALESCE(e.ev_toi_minutes, 0) as ev_toi_minutes,
            COALESCE(e.ev_xgoals, 0) as ev_xgoals,
            COALESCE(e.ev_on_ice_xgoals, 0) as ev_on_ice_xgoals,
            COALESCE(e.ev_on_ice_goals, 0) as ev_on_ice_goals,
            COALESCE(p.pp_assists, 0) as pp_assists,
            COALESCE(p.pp_points, 0) as pp_points,
            COALESCE(p.pp_toi_minutes, 0) as pp_toi_minutes,
            COALESCE(p.pp_xgoals, 0) as pp_xgoals,
            COALESCE(p.pp_on_ice_xgoals, 0) as pp_on_ice_xgoals,
            COALESCE(p.pp_on_ice_goals, 0) as pp_on_ice_goals,
            COALESCE(t.team_pp_xgoals, 0) as team_pp_xgoals,
            COALESCE(t.team_pp_toi_minutes, 0) as team_pp_toi_minutes
        FROM player_game_base b
        LEFT JOIN player_ev_stats e ON b.player_id = e.player_id AND b.game_id = e.game_id
        LEFT JOIN player_pp_stats p ON b.player_id = p.player_id AND b.game_id = p.game_id
        LEFT JOIN team_pp_totals t ON b.team = t.team AND b.game_id = t.game_id
    ),
    rolling_stats AS (
        SELECT
            *,
            -- L5 Rolling (Hot/Cold)
            AVG(ev_assists) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as ev_assists_L5,
            AVG(ev_points) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as ev_points_L5,
            AVG(ev_toi_minutes) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as ev_toi_minutes_L5,
            AVG(ev_on_ice_xgoals) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as ev_on_ice_xg_L5,
            AVG(ev_on_ice_goals) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as ev_on_ice_goals_L5,

            -- Corsi/SOG Rolling
            AVG(shot_attempts) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as shot_attempts_L5,
            AVG(sog) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as sog_L5,
            
            AVG(shot_attempts) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as shot_attempts_L10,
            AVG(sog) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as sog_L10,

            AVG(shot_attempts) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as shot_attempts_L20,
            AVG(sog) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as sog_L20,

            AVG(shot_attempts) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING) as shot_attempts_L40,
            AVG(sog) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING) as sog_L40,
            
            AVG(shot_attempts) OVER (PARTITION BY player_id, season ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as shot_attempts_Season,
            AVG(sog) OVER (PARTITION BY player_id, season ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as sog_Season,

            -- L20 Rolling (Primary Standard)
            AVG(ev_assists) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as ev_assists_L20,
            AVG(ev_points) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as ev_points_L20,
            AVG(ev_toi_minutes) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as ev_toi_minutes_L20,
            AVG(ev_on_ice_xgoals) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as ev_on_ice_xg_L20,
            AVG(ev_on_ice_goals) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as ev_on_ice_goals_L20,
            
            AVG(pp_assists) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as pp_assists_L20,
            AVG(pp_points) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as pp_points_L20,
            AVG(pp_toi_minutes) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as pp_toi_minutes_L20,
            AVG(pp_on_ice_xgoals) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as pp_on_ice_xg_L20,
            AVG(pp_on_ice_goals) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as pp_on_ice_goals_L20,
            
            AVG(team_pp_xgoals) OVER (PARTITION BY team ORDER BY game_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as team_pp_xg_L20,
            AVG(team_pp_toi_minutes) OVER (PARTITION BY team ORDER BY game_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as team_pp_toi_L20,

            -- L40 Rolling (Long Term Stability)
            AVG(ev_assists) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING) as ev_assists_L40,
            AVG(ev_points) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING) as ev_points_L40,
            AVG(ev_toi_minutes) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING) as ev_toi_minutes_L40,
            AVG(ev_on_ice_xgoals) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING) as ev_on_ice_xg_L40,
            AVG(ev_on_ice_goals) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING) as ev_on_ice_goals_L40,

            -- Season-to-Date (YTD) - Resets every season
            AVG(ev_assists) OVER (PARTITION BY player_id, season ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as ev_assists_Season,
            AVG(ev_points) OVER (PARTITION BY player_id, season ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as ev_points_Season,
            AVG(ev_toi_minutes) OVER (PARTITION BY player_id, season ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as ev_toi_minutes_Season,
            AVG(ev_on_ice_xgoals) OVER (PARTITION BY player_id, season ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as ev_on_ice_xg_Season,
            AVG(ev_on_ice_goals) OVER (PARTITION BY player_id, season ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as ev_on_ice_goals_Season,
            
            AVG(pp_assists) OVER (PARTITION BY player_id, season ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as pp_assists_Season,
            AVG(pp_points) OVER (PARTITION BY player_id, season ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as pp_points_Season,
            AVG(pp_toi_minutes) OVER (PARTITION BY player_id, season ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as pp_toi_minutes_Season,
            AVG(pp_on_ice_xgoals) OVER (PARTITION BY player_id, season ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as pp_on_ice_xg_Season,
            AVG(pp_on_ice_goals) OVER (PARTITION BY player_id, season ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as pp_on_ice_goals_Season,

            -- L10 (Legacy/Goals/SOG)
            AVG(goals) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as goals_per_game_L10,
            AVG(x_goals) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as xg_per_game_L10,
            AVG(assists) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as assists_per_game_L10,
            AVG(points) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as points_per_game_L10,
            AVG(sog) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as sog_per_game_L10,
            AVG(blocks) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as blocks_per_game_L10,
            AVG(toi_minutes) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as avg_toi_minutes_L10,
            
            -- L10 primary assists for involvement proxy
            AVG(primary_assists) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as primary_assists_L10,

            -- Season L10 analogs (for SOG comparisons)
            AVG(sog) OVER (PARTITION BY player_id, season ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as sog_per_game_Season,
            AVG(blocks) OVER (PARTITION BY player_id, season ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as blocks_per_game_Season

        FROM merged_stats
    )
    SELECT 
        *,
        -- Derived Rates (Per 60) - L20
        CASE WHEN ev_toi_minutes_L20 > 0 THEN (ev_assists_L20 / ev_toi_minutes_L20) * 60 ELSE 0 END as ev_ast_60_L20,
        CASE WHEN ev_toi_minutes_L20 > 0 THEN (ev_points_L20 / ev_toi_minutes_L20) * 60 ELSE 0 END as ev_pts_60_L20,
        CASE WHEN ev_toi_minutes_L20 > 0 THEN (ev_on_ice_xg_L20 / ev_toi_minutes_L20) * 60 ELSE 0 END as ev_on_ice_xg_60_L20,
        
        CASE WHEN pp_toi_minutes_L20 > 0 THEN (pp_assists_L20 / pp_toi_minutes_L20) * 60 ELSE 0 END as pp_ast_60_L20,
        CASE WHEN pp_toi_minutes_L20 > 0 THEN (pp_points_L20 / pp_toi_minutes_L20) * 60 ELSE 0 END as pp_pts_60_L20,
        CASE WHEN pp_toi_minutes_L20 > 0 THEN (pp_on_ice_xg_L20 / pp_toi_minutes_L20) * 60 ELSE 0 END as pp_on_ice_xg_60_L20,
        
        -- Corsi/SOG Rates (L20/L40)
        CASE WHEN avg_toi_minutes_L10 > 0 THEN (shot_attempts_L20 / avg_toi_minutes_L10) * 60 ELSE 0 END as corsi_per_60_L20,
        CASE WHEN avg_toi_minutes_L10 > 0 THEN (sog_L20 / avg_toi_minutes_L10) * 60 ELSE 0 END as sog_per_60_L20_Derived,
        
        CASE WHEN avg_toi_minutes_L10 > 0 THEN (shot_attempts_L40 / avg_toi_minutes_L10) * 60 ELSE 0 END as corsi_per_60_L40,
        CASE WHEN avg_toi_minutes_L10 > 0 THEN (sog_L40 / avg_toi_minutes_L10) * 60 ELSE 0 END as sog_per_60_L40_Derived,

        -- Derived Rates - L5
        CASE WHEN ev_toi_minutes_L5 > 0 THEN (ev_assists_L5 / ev_toi_minutes_L5) * 60 ELSE 0 END as ev_ast_60_L5,
        CASE WHEN ev_toi_minutes_L5 > 0 THEN (ev_points_L5 / ev_toi_minutes_L5) * 60 ELSE 0 END as ev_pts_60_L5,
        
        -- Derived Rates - L40
        CASE WHEN ev_toi_minutes_L40 > 0 THEN (ev_assists_L40 / ev_toi_minutes_L40) * 60 ELSE 0 END as ev_ast_60_L40,
        CASE WHEN ev_toi_minutes_L40 > 0 THEN (ev_points_L40 / ev_toi_minutes_L40) * 60 ELSE 0 END as ev_pts_60_L40,

        -- Derived Rates - Season
        CASE WHEN ev_toi_minutes_Season > 0 THEN (ev_assists_Season / ev_toi_minutes_Season) * 60 ELSE 0 END as ev_ast_60_Season,
        CASE WHEN ev_toi_minutes_Season > 0 THEN (ev_points_Season / ev_toi_minutes_Season) * 60 ELSE 0 END as ev_pts_60_Season,
        CASE WHEN ev_toi_minutes_Season > 0 THEN (ev_on_ice_xg_Season / ev_toi_minutes_Season) * 60 ELSE 0 END as ev_on_ice_xg_60_Season,
        
        CASE WHEN pp_toi_minutes_Season > 0 THEN (pp_assists_Season / pp_toi_minutes_Season) * 60 ELSE 0 END as pp_ast_60_Season,
        CASE WHEN pp_toi_minutes_Season > 0 THEN (pp_points_Season / pp_toi_minutes_Season) * 60 ELSE 0 END as pp_pts_60_Season,
        
        CASE WHEN (ev_toi_minutes_Season + pp_toi_minutes_Season) > 0 THEN (shot_attempts_Season / (ev_toi_minutes_Season + pp_toi_minutes_Season)) * 60 ELSE 0 END as corsi_per_60_Season,

        CASE WHEN team_pp_toi_L20 > 0 THEN (team_pp_xg_L20 / team_pp_toi_L20) * 60 ELSE 0 END as team_pp_xg_60_L20,
        
        -- Involvement Proxy: Player Points / On-Ice xG (clipped)
        CASE WHEN ev_on_ice_xg_L20 > 0 THEN LEAST(2.0, ev_points_L20 / ev_on_ice_xg_L20) ELSE 0 END as ev_ipp_x_L20,
        CASE WHEN pp_on_ice_xg_L20 > 0 THEN LEAST(2.0, pp_points_L20 / pp_on_ice_xg_L20) ELSE 0 END as pp_ipp_x_L20,

        -- IPP for Assists (L20)
        CASE WHEN ev_on_ice_goals_L20 > 0 THEN LEAST(1.0, ev_assists_L20 / ev_on_ice_goals_L20) ELSE 0 END as ev_ipp_assists_L20,
        CASE WHEN pp_on_ice_goals_L20 > 0 THEN LEAST(1.0, pp_assists_L20 / pp_on_ice_goals_L20) ELSE 0 END as pp_ipp_assists_L20,
        
        -- IPP for Assists (Season)
        CASE WHEN ev_on_ice_goals_Season > 0 THEN LEAST(1.0, ev_assists_Season / ev_on_ice_goals_Season) ELSE 0 END as ev_ipp_assists_Season,

        -- Primary Assist Ratio
        CASE WHEN assists_per_game_L10 > 0 THEN primary_assists_L10 / assists_per_game_L10 ELSE 0.5 END as primary_ast_ratio_L10,
        
        -- Compatibility columns
        CASE WHEN avg_toi_minutes_L10 > 0 THEN (xg_per_game_L10 / avg_toi_minutes_L10) * 60 ELSE 0 END as xg_per_60_L10,
        CASE WHEN avg_toi_minutes_L10 > 0 THEN (sog_per_game_L10 / avg_toi_minutes_L10) * 60 ELSE 0 END as sog_per_60_L10,
        
        CASE WHEN ev_toi_minutes_Season > 0 THEN (sog_per_game_Season / (ev_toi_minutes_Season + pp_toi_minutes_Season)) * 60 ELSE 0 END as sog_per_60_Season

    FROM rolling_stats
    WHERE 1=1 {season_filter}
    """
    
    formatted_query = query.format(season_filter=season_filter)
    conn.execute(formatted_query)
    
    count = conn.sql("SELECT COUNT(*) FROM fact_player_game_features").fetchone()[0]
    print(f"Created fact_player_game_features with {count} rows.")
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-season", type=int)
    parser.add_argument("--end-season", type=int)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    db_path = "data/db/nhl_backtest.duckdb"
    build_player_features(db_path, args.start_season, args.end_season, args.force)

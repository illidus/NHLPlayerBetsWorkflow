import duckdb
import pandas as pd
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Determine project root from src/nhl_bets/projections/produce_live_base_projections.py
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))

DB_PATH = os.path.join(project_root, 'data', 'db', 'nhl_backtest.duckdb')
OUTPUT_PATH = os.path.join(project_root, 'outputs', 'projections', 'BaseSingleGameProjections.csv')

def produce_live_projections():
    if not os.path.exists(DB_PATH):
        logger.error(f"DuckDB not found at {DB_PATH}. Cannot produce live projections.")
        return False

    con = duckdb.connect(DB_PATH)
    
    logger.info("Computing Live Projections from Raw Game Logs (Zero Lag)...")
    
    query = """
    WITH base_stats AS (
        SELECT 
            player_id, game_id, game_date, team,
            goals, x_goals, 
            primary_assists + secondary_assists as assists,
            points, sog, blocks, toi_seconds/60.0 as toi,
            shot_attempts
        FROM fact_skater_game_situation 
        WHERE situation = 'all'
    ),
    ev_stats AS (
        SELECT player_id, game_id, 
            primary_assists + secondary_assists as ev_assists,
            points as ev_points, 
            toi_seconds/60.0 as ev_toi,
            on_ice_xgoals as ev_on_ice_xg
        FROM fact_skater_game_situation WHERE situation = '5on5'
    ),
    pp_stats AS (
        SELECT player_id, game_id,
            primary_assists + secondary_assists as pp_assists,
            points as pp_points,
            toi_seconds/60.0 as pp_toi,
            on_ice_xgoals as pp_on_ice_xg
        FROM fact_skater_game_situation WHERE situation = '5on4'
    ),
    combined_games AS (
        SELECT 
            b.player_id, b.game_date, b.team,
            b.goals, b.x_goals, b.assists, b.points, b.sog, b.blocks, b.toi, b.shot_attempts,
            COALESCE(e.ev_assists, 0) as ev_assists,
            COALESCE(e.ev_points, 0) as ev_points,
            COALESCE(e.ev_toi, 0) as ev_toi,
            COALESCE(e.ev_on_ice_xg, 0) as ev_on_ice_xg,
            COALESCE(p.pp_assists, 0) as pp_assists,
            COALESCE(p.pp_points, 0) as pp_points,
            COALESCE(p.pp_toi, 0) as pp_toi,
            COALESCE(p.pp_on_ice_xg, 0) as pp_on_ice_xg,
            ROW_NUMBER() OVER (PARTITION BY b.player_id ORDER BY b.game_date DESC) as rn
        FROM base_stats b
        LEFT JOIN ev_stats e ON b.player_id = e.player_id AND b.game_id = e.game_id
        LEFT JOIN pp_stats p ON b.player_id = p.player_id AND b.game_id = p.game_id
        WHERE b.game_date >= '2024-01-01' 
    ),
    aggregated AS (
        SELECT 
            player_id,
            FIRST(team) as Team,
            COUNT(*) FILTER (WHERE rn <= 10) as games_used_L10,
            
            -- L10 Base
            AVG(toi) FILTER (WHERE rn <= 10) as avg_toi_minutes_L10,
            AVG(x_goals) FILTER (WHERE rn <= 10) as xg_per_game_L10,
            AVG(goals) FILTER (WHERE rn <= 10) as goals_per_game_L10,
            AVG(assists) FILTER (WHERE rn <= 10) as assists_per_game_L10,
            AVG(points) FILTER (WHERE rn <= 10) as points_per_game_L10,
            AVG(sog) FILTER (WHERE rn <= 10) as sog_per_game_L10,
            AVG(blocks) FILTER (WHERE rn <= 10) as blocks_per_game_L10,
            
            -- L20 Process
            AVG(ev_assists) FILTER (WHERE rn <= 20) as ev_assists_L20,
            AVG(ev_points) FILTER (WHERE rn <= 20) as ev_points_L20,
            AVG(ev_toi) FILTER (WHERE rn <= 20) as ev_toi_minutes_L20,
            AVG(ev_on_ice_xg) FILTER (WHERE rn <= 20) as ev_on_ice_xg_L20,
            AVG(pp_assists) FILTER (WHERE rn <= 20) as pp_assists_L20,
            AVG(pp_points) FILTER (WHERE rn <= 20) as pp_points_L20,
            AVG(pp_toi) FILTER (WHERE rn <= 20) as pp_toi_minutes_L20,
            AVG(pp_on_ice_xg) FILTER (WHERE rn <= 20) as pp_on_ice_xg_L20,
            AVG(shot_attempts) FILTER (WHERE rn <= 20) as corsi_L20,
            
            -- L40 Stability
            AVG(ev_assists) FILTER (WHERE rn <= 40) as ev_assists_L40,
            AVG(ev_points) FILTER (WHERE rn <= 40) as ev_points_L40,
            AVG(ev_toi) FILTER (WHERE rn <= 40) as ev_toi_minutes_L40,
            AVG(ev_on_ice_xg) FILTER (WHERE rn <= 40) as ev_on_ice_xg_L40,
            AVG(shot_attempts) FILTER (WHERE rn <= 40) as corsi_L40,
            AVG(sog) FILTER (WHERE rn <= 40) as sog_L40
            
        FROM combined_games
        WHERE rn <= 40
        GROUP BY player_id
    )
    SELECT 
        dp.player_name as Player,
        a.Team,
        dp.position as Pos,
        a.games_used_L10 as GP,
        a.avg_toi_minutes_L10 as TOI,
        a.xg_per_game_L10 as "mu_base_goals",
        a.goals_per_game_L10 as "Realized Goals Per Game",
        a.assists_per_game_L10 as "Assists Per Game",
        a.points_per_game_L10 as "Points Per Game",
        a.sog_per_game_L10 as "SOG Per Game",
        a.blocks_per_game_L10 as "Blocks Per Game",
        
        -- Corsi Features
        CASE WHEN a.avg_toi_minutes_L10 > 0 THEN (a.corsi_L20 / a.avg_toi_minutes_L10) * 60 ELSE 0 END as corsi_per_60_L20,
        CASE WHEN a.corsi_L40 > 0 THEN (a.sog_L40 / a.corsi_L40) ELSE 0 END as thru_pct_L40,

        -- L20 Rates
        CASE WHEN a.ev_toi_minutes_L20 > 0 THEN (a.ev_assists_L20 / a.ev_toi_minutes_L20) * 60 ELSE 0 END as ev_ast_60_L20,
        CASE WHEN a.pp_toi_minutes_L20 > 0 THEN (a.pp_assists_L20 / a.pp_toi_minutes_L20) * 60 ELSE 0 END as pp_ast_60_L20,
        CASE WHEN a.ev_toi_minutes_L20 > 0 THEN (a.ev_points_L20 / a.ev_toi_minutes_L20) * 60 ELSE 0 END as ev_pts_60_L20,
        CASE WHEN a.pp_toi_minutes_L20 > 0 THEN (a.pp_points_L20 / a.pp_toi_minutes_L20) * 60 ELSE 0 END as pp_pts_60_L20,
        a.ev_toi_minutes_L20,
        a.pp_toi_minutes_L20,
        CASE WHEN a.ev_toi_minutes_L20 > 0 THEN (a.ev_on_ice_xg_L20 / a.ev_toi_minutes_L20) * 60 ELSE 0 END as ev_on_ice_xg_60_L20,
        CASE WHEN a.pp_toi_minutes_L20 > 0 THEN (a.pp_on_ice_xg_L20 / a.pp_toi_minutes_L20) * 60 ELSE 0 END as pp_on_ice_xg_60_L20,
        
        -- L40 Rates
        CASE WHEN a.ev_toi_minutes_L40 > 0 THEN (a.ev_assists_L40 / a.ev_toi_minutes_L40) * 60 ELSE 0 END as ev_ast_60_L40,
        CASE WHEN a.ev_toi_minutes_L40 > 0 THEN (a.ev_points_L40 / a.ev_toi_minutes_L40) * 60 ELSE 0 END as ev_pts_60_L40,
        CASE WHEN a.ev_toi_minutes_L40 > 0 THEN (a.ev_on_ice_xg_L40 / a.ev_toi_minutes_L40) * 60 ELSE 0 END as ev_on_ice_xg_60_L40,

        -- IPP Proxies
        CASE WHEN a.ev_on_ice_xg_L20 > 0 THEN LEAST(2.0, a.ev_points_L20 / a.ev_on_ice_xg_L20) ELSE 0 END as ev_ipp_x_L20,
        CASE WHEN a.pp_on_ice_xg_L20 > 0 THEN LEAST(2.0, a.pp_points_L20 / a.pp_on_ice_xg_L20) ELSE 0 END as pp_ipp_x_L20,
        CASE WHEN a.ev_on_ice_xg_L40 > 0 THEN LEAST(2.0, a.ev_points_L40 / a.ev_on_ice_xg_L40) ELSE 0 END as ev_ipp_x_L40

    FROM aggregated a
    JOIN dim_players dp ON a.player_id = dp.player_id
    """
    
    try:
        df = con.execute(query).df()
        if df.empty:
            logger.warning("No live projection data found.")
            return False
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        df.to_csv(OUTPUT_PATH, index=False, float_format="%.6f")
        logger.info(f"Successfully exported {len(df)} live projections to {OUTPUT_PATH}")
        return True
    except Exception as e:
        logger.error(f"Error producing live projections: {e}")
        return False
    finally:
        con.close()

if __name__ == "__main__":
    produce_live_projections()

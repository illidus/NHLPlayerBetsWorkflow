import duckdb
import pandas as pd
import sys
import argparse
import os
from datetime import datetime

# Add project root to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from nhl_bets.projections.single_game_model import compute_game_probs
    from nhl_bets.projections.config import ALPHAS
except ImportError as e:
    print(f"Error importing nhl_bets package: {e}")
    sys.exit(1)

def build_snapshots(db_path, start_season=None, end_season=None, force=False, model_version="baseline_v1"):
    conn = duckdb.connect(db_path)
    
    # Enable performance pragmas
    conn.execute("SET memory_limit = '8GB';")
    conn.execute("SET threads = 8;")
    conn.execute("SET temp_directory = './duckdb_temp/';")

    # Check existing
    if not force:
        tables = conn.sql("SHOW TABLES").fetchall()
        existing = [t[0] for t in tables]
        if 'fact_probabilities' in existing and 'fact_model_mu' in existing:
            print("Tables 'fact_probabilities' and 'fact_model_mu' exist. Use --force to rebuild.")
            return

    print(f"Building Probability Snapshots (Model: {model_version})...")
    
    # 1. Fetch Data
    # Join Player Features + Team Defense + Goalie Features
    # Use L10 as default window per instructions
    
    season_filter = ""
    if start_season:
        season_filter += f" AND p.season >= {start_season}"
    if end_season:
        season_filter += f" AND p.season <= {end_season}"

    query = f"""
    WITH team_schedule AS (
        -- 1. Build Schedule & Identify B2B Context
        SELECT 
            game_id,
            home_team as team,
            game_date,
            season,
            LAG(game_date) OVER (PARTITION BY home_team ORDER BY game_date) as prev_game_date,
            LAG(game_id) OVER (PARTITION BY home_team ORDER BY game_date) as prev_game_id
        FROM dim_games
        UNION ALL
        SELECT 
            game_id,
            away_team as team,
            game_date,
            season,
            LAG(game_date) OVER (PARTITION BY away_team ORDER BY game_date) as prev_game_date,
            LAG(game_id) OVER (PARTITION BY away_team ORDER BY game_date) as prev_game_id
        FROM dim_games
    ),
    schedule_context AS (
        SELECT 
            *,
            CASE WHEN date_diff('day', prev_game_date, game_date) = 1 THEN 1 ELSE 0 END as is_b2b
        FROM team_schedule
    ),
    
    -- 2. Determine Roster Depth (Rank 1 = Starter, Rank 2 = Backup) based on PAST Volume
    -- We look at the most recent snapshot available for each goalie before the target game
    recent_roster AS (
        SELECT 
            s.game_id,
            s.team,
            s.is_b2b,
            s.prev_game_id,
            gf.goalie_id,
            gf.sum_toi_L10,
            -- Rank by Volume (L10 TOI) to find "Implied Starter" vs "Backup"
            ROW_NUMBER() OVER (PARTITION BY s.game_id, s.team ORDER BY gf.sum_toi_L10 DESC) as depth_rank
        FROM schedule_context s
        JOIN fact_goalie_features gf 
            ON s.team = gf.team 
            AND gf.game_date < s.game_date 
            AND gf.game_date >= (s.game_date - INTERVAL 14 DAY) -- Look back 2 weeks for active roster
        -- Dedup: keep most recent stats per goalie per target game
        QUALIFY ROW_NUMBER() OVER (PARTITION BY s.game_id, s.team, gf.goalie_id ORDER BY gf.game_date DESC) = 1
    ),
    
    -- 3. Identify Who Started the Previous Game
    prev_starter_info AS (
        SELECT 
            s.game_id, -- Target Game ID
            s.team,
            pg.player_id as prev_starter_id
        FROM schedule_context s
        JOIN fact_goalie_game_situation pg 
            ON s.prev_game_id = pg.game_id 
            AND s.team = pg.team
        -- Use Max TOI to identify the starter of the prev game
        QUALIFY ROW_NUMBER() OVER (PARTITION BY s.game_id, s.team ORDER BY pg.toi_seconds DESC) = 1
    ),
    
    -- 4. Final Selection Heuristic
    primary_goalies AS (
        SELECT 
            r.game_id,
            r.team,
            r.goalie_id,
            1 as rn -- Maintain compatibility with downstream join
        FROM recent_roster r
        LEFT JOIN prev_starter_info p ON r.game_id = p.game_id AND r.team = p.team
        WHERE 
            CASE 
                -- If B2B AND The "#1 Guy" started yesterday -> Pick the "#2 Guy"
                WHEN r.is_b2b = 1 AND r.depth_rank = 1 AND r.goalie_id = p.prev_starter_id THEN 0
                WHEN r.is_b2b = 1 AND r.depth_rank = 2 AND (p.prev_starter_id IS NULL OR p.prev_starter_id != r.goalie_id) THEN 1
                
                -- Standard: Pick #1
                WHEN r.is_b2b = 0 AND r.depth_rank = 1 THEN 1
                
                -- Fallback for weird B2B cases (e.g. #1 didn't play yesterday, or no backup found)
                -- If we filtered out Rank 1 above, we need to ensure Rank 2 is picked. 
                -- The logic above is slightly exclusive. Let's simplify with a Priority Sort.
                ELSE 0 
            END = 1
            
        -- Ensure we pick exactly one per game/team (in case logic matches multiple or none, fallback to Rank 1)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY r.game_id, r.team ORDER BY r.depth_rank ASC) = 1
    )
    SELECT 
        p.player_id,
        p.game_id,
        p.game_date,
        p.season,
        dp.player_name as Player,
        p.team as Team,
        p.opp_team as OppTeam,
        p.home_or_away,
        p.position as Pos,
        
        -- Base Stats (L10) -> Map to G, A, PTS, etc.
        p.xg_per_game_L10 as G,
        p.goals_per_game_L10 as G_realized,
        p.assists_per_game_L10 as A,
        p.points_per_game_L10 as PTS,
        p.sog_per_game_L10 as SOG,
        p.blocks_per_game_L10 as BLK,
        
        -- Enhanced Process Features (L20)
        p.ev_ast_60_L20,
        p.pp_ast_60_L20,
        p.ev_pts_60_L20,
        p.pp_pts_60_L20,
        p.ev_toi_minutes_L20,
        p.pp_toi_minutes_L20,
        p.ev_on_ice_xg_60_L20,
        p.pp_on_ice_xg_60_L20,
        p.team_pp_xg_60_L20,
        p.ev_ipp_x_L20,
        p.pp_ipp_x_L20,
        p.primary_ast_ratio_L10,
        
        -- TOI
        p.avg_toi_minutes_L10 as TOI,
        p.avg_toi_minutes_L10 as proj_toi, -- Use L10 as projection proxy
        
        -- Context (Opponent)
        d.opp_sa60_L10 as opp_sa60,
        d.opp_xga60_L10 as opp_xga60,
        
        -- Goalie Features
        COALESCE(gf.goalie_gsax60_L10, 0.0) as goalie_gsax60,
        CASE 
            WHEN gf.sum_toi_L10 IS NULL OR gf.sum_toi_L10 = 0 THEN 0.0 
            ELSE gf.sum_xga_L10 / (gf.sum_toi_L10 / 3600)
        END as goalie_xga60,
        
        -- Placeholders for missing context
        NULL as implied_team_total,
        NULL as is_b2b
        
    FROM fact_player_game_features p
    LEFT JOIN fact_team_defense_features d 
        ON p.opp_team = d.team 
        AND p.game_date = d.game_date
    LEFT JOIN dim_players dp
        ON p.player_id = dp.player_id
        
    -- Join Primary Goalie (Opponent)
    LEFT JOIN primary_goalies pg
        ON p.game_id = pg.game_id
        AND p.opp_team = pg.team
        AND pg.rn = 1
        
    -- Join Goalie Features
    LEFT JOIN fact_goalie_features gf
        ON pg.goalie_id = gf.goalie_id
        AND p.game_id = gf.game_id
        
    WHERE 1=1 {season_filter}
    -- Ensure we have valid rolling stats
    AND p.goals_per_game_L10 IS NOT NULL
    """
    
    print("Executing query...")
    try:
        df = conn.execute(query).df()
        print(f"Loaded {len(df)} rows.")
    except Exception as e:
        print(f"Error executing query: {e}")
        conn.close()
        sys.exit(1)

    # 2. Compute Probabilities
    print("Computing probabilities...")
    
    mu_records = []
    prob_records = []
    
    # Using itertuples for speed
    # row will have attributes matching columns
    for row in df.itertuples(index=False):
        # Prepare inputs
        # We can pass the row object itself if we access as dict, but compute_game_probs expects dict access keys
        # row._asdict() is available in namedtuples from itertuples
        
        row_dict = row._asdict()
        
        # Call model
        try:
            res = compute_game_probs(row_dict, row_dict)
        except Exception as e:
            # Handle potential errors (e.g. math domain)
            print(f"Error processing row: {e}")
            break # Stop after first error to avoid flood
            continue
            
        # -- Prepare fact_model_mu record --
        mu_rec = {
            'player_id': row.player_id,
            'player_name': row.Player,
            'game_id': row.game_id,
            'game_date': row.game_date,
            'team': row.Team,
            'opp_team': row.OppTeam,
            'mu_goals': res['mu_goals'],
            'mu_assists': res['mu_assists'],
            'mu_points': res['mu_points'],
            'mu_sog': res['mu_sog'],
            'mu_blocks': res['mu_blocks'],
            'mult_opp_sog': res['mult_opp_sog'],
            'mult_opp_g': res['mult_opp_g'],
            'mult_goalie': res['mult_goalie'],
            'goalie_gsax60': row.goalie_gsax60,
            'model_version': model_version
        }
        mu_records.append(mu_rec)
        
        # -- Prepare fact_probabilities records (Long format) --
        # Markets: GOALS, ASSISTS, POINTS, SOG, BLOCKS
        
        # Helper to add lines
        def add_probs(market, probs_dict, mu_val, dist):
            for line, p_val in probs_dict.items():
                prob_records.append({
                    'asof_ts': row.game_date, # simplified
                    'game_id': row.game_id,
                    'game_date': row.game_date,
                    'season': row.season,
                    'player_id': row.player_id,
                    'player_name': row.Player,
                    'team': row.Team,
                    'opp_team': row.OppTeam,
                    'market': market,
                    'line': line,
                    'p_over': p_val,
                    'mu_used': mu_val,
                    'dist_type': dist,
                    'model_version': model_version,
                    'feature_window': 'L10'
                })

        add_probs('GOALS', res['probs_goals'], res['mu_goals'], 'poisson')
        add_probs('ASSISTS', res['probs_assists'], res['mu_assists'], 'poisson')
        add_probs('POINTS', res['probs_points'], res['mu_points'], 'poisson')
        add_probs('SOG', res['probs_sog'], res['mu_sog'], 'negbin')
        add_probs('BLOCKS', res['probs_blocks'], res['mu_blocks'], 'negbin')

    # 3. Write to DuckDB
    print("Writing results to DuckDB...")
    
    if not mu_records:
        print("No records generated.")
        conn.close()
        return

    df_mu = pd.DataFrame(mu_records)
    df_probs = pd.DataFrame(prob_records)
    
    # Create tables
    conn.execute("CREATE OR REPLACE TABLE fact_model_mu AS SELECT * FROM df_mu")
    conn.execute("CREATE OR REPLACE TABLE fact_probabilities AS SELECT * FROM df_probs")
    
    print(f"Written {len(df_mu)} rows to fact_model_mu")
    print(f"Written {len(df_probs)} rows to fact_probabilities")
    
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-season", type=int, default=2018)
    parser.add_argument("--end-season", type=int, default=2025)
    parser.add_argument("--duckdb-path", default="data/db/nhl_backtest.duckdb")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--model-version", default="baseline_v1")
    
    args = parser.parse_args()
    
    build_snapshots(
        args.duckdb_path,
        args.start_season,
        args.end_season,
        args.force,
        args.model_version
    )

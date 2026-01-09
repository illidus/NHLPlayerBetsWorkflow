import pandas as pd
import duckdb
import os
import sys
import argparse
import joblib
from datetime import datetime, timedelta

# Add src to path for nhl_bets import
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(os.path.dirname(current_dir))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

try:
    from nhl_bets.analysis.normalize import TEAM_MAP, get_teams_from_slug
    from nhl_bets.projections.config import LG_SA60, LG_XGA60, LG_PACE
except ImportError as e:
    print(f"Error: Could not import utils or config: {e}")
    # Fallback defaults if config fails
    LG_SA60 = 30.0
    LG_XGA60 = 2.8
    LG_PACE = 62.0

project_root = os.path.dirname(src_dir)

DB_PATH = os.path.join(project_root, 'data', 'db', 'nhl_backtest.duckdb')
PROPS_PATH = os.path.join(project_root, 'data', 'raw', 'nhl_player_props_all.csv')
BASE_PROJ_PATH = os.path.join(project_root, 'outputs', 'projections', 'BaseSingleGameProjections.csv')
OUTPUT_PATH = os.path.join(project_root, 'outputs', 'projections', 'GameContext.csv')
MODEL_PATH = os.path.join(project_root, 'data', 'models', 'toi_model.pkl')

def get_db_connection():
    return duckdb.connect(DB_PATH)

def load_schedule_from_props():
    if not os.path.exists(PROPS_PATH):
        print(f"Props file not found: {PROPS_PATH}")
        return None, None
    
    df = pd.read_csv(PROPS_PATH)
    if 'Game' not in df.columns or 'Game_Date' not in df.columns:
        print("Props file missing 'Game' or 'Game_Date' columns.")
        return None, None
        
    # Get unique games
    games = df[['Game', 'Game_Date']].drop_duplicates()
    
    # Parse matchups
    matchups = [] # (Team, Opponent, Date)
    
    for _, row in games.iterrows():
        slug = row['Game']
        date_str = row['Game_Date']
        
        away, home = get_teams_from_slug(slug)
        if away and home:
            # Add both perspectives
            matchups.append({'Team': away, 'OppTeam': home, 'Date': date_str, 'IsHome': False})
            matchups.append({'Team': home, 'OppTeam': away, 'Date': date_str, 'IsHome': True})
            
    return pd.DataFrame(matchups), df['Game_Date'].max()

def get_latest_stats(con, table_name, team_col, date_col, metrics, target_date):
    """
    Fetches the most recent stats for each team prior to target_date.
    """
    metrics_sql = ", ".join([f"f.{m}" for m in metrics])
    
    query = f"""
    WITH ranked AS (
        SELECT 
            {team_col},
            {metrics_sql},
            ROW_NUMBER() OVER (PARTITION BY {team_col} ORDER BY {date_col} DESC) as rn
        FROM {table_name} f
        WHERE {date_col} < '{target_date}'
    )
    SELECT * FROM ranked WHERE rn = 1
    """
    return con.execute(query).df()

def get_goalie_stats_l30(con, target_date):
    """
    Aggregates L30 day GSAx for all goalies.
    """
    # L30 Window
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    start_dt = (dt - timedelta(days=30)).strftime("%Y-%m-%d")
    
    # We query fact_goalie_game_situation
    # Note: player_id in situation table maps to goalie_id
    query = f"""
    SELECT 
        player_id as goalie_id,
        SUM(x_goals_against - goals_against) as sum_gsax,
        SUM(toi_seconds) / 60.0 as sum_toi_mins
    FROM fact_goalie_game_situation
    WHERE situation = 'all'
      AND game_date >= '{start_dt}' 
      AND game_date < '{target_date}'
    GROUP BY player_id
    HAVING sum_toi_mins > 60
    """
    df = con.execute(query).df()
    # Calculate Rate
    df['goalie_gsax60_L30'] = df['sum_gsax'] / (df['sum_toi_mins'] / 60.0)
    return df.set_index('goalie_id')[['goalie_gsax60_L30']].to_dict('index')

def get_team_pace_l10(con, target_date):
    """
    Calculates L10 Game Pace (SOG For + SOG Against) per 60.
    """
    # 1. Get last 10 game IDs for each team
    query = f"""
    WITH recent_games AS (
        SELECT 
            team, 
            game_id,
            game_date,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC) as rn
        FROM fact_goalie_game_situation
        WHERE situation = 'all' AND game_date < '{target_date}'
    ),
    game_stats AS (
        SELECT
            team,
            game_id,
            SUM(shots_against) as sa,
            SUM(toi_seconds) / 60.0 as toi_mins
        FROM fact_goalie_game_situation
        WHERE situation = 'all'
        GROUP BY team, game_id
    )
    SELECT
        t1.team,
        SUM(t1.sa + t2.sa) as total_events, -- My SA + Opp SA (My SF)
        SUM(t1.toi_mins) as total_mins
    FROM recent_games rg
    JOIN game_stats t1 ON rg.team = t1.team AND rg.game_id = t1.game_id
    JOIN game_stats t2 ON t1.game_id = t2.game_id AND t1.team != t2.team
    WHERE rg.rn <= 10
    GROUP BY t1.team
    """
    try:
        df = con.execute(query).df()
        df['pace_L10'] = df['total_events'] / (df['total_mins'] / 60.0)
        return df.set_index('team')[['pace_L10']].to_dict('index')
    except Exception as e:
        print(f"Warning: Pace calc failed: {e}")
        return {}

def get_team_b2b_status(con, target_date_str):
    """
    Checks if teams played on the day before target_date.
    Returns set of team codes.
    """
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    prev_date = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Check dim_games for games on prev_date
    query = f"""
    SELECT DISTINCT home_team as team FROM dim_games WHERE game_date = '{prev_date}'
    UNION
    SELECT DISTINCT away_team as team FROM dim_games WHERE game_date = '{prev_date}'
    """
    df = con.execute(query).df()
    return set(df['team'].tolist())

def get_likely_goalie(con, team, opp_b2b, target_date):
    """
    Determines likely goalie for a team.
    Logic:
    1. Rank active goalies (L14 days TOI).
    2. If Opponent is B2B (Wait, B2B logic applies to the Goalie's team).
       If 'team' is B2B, checks if Starter played yesterday.
    """
    # 1. Get active goalies (last 14 days)
    # We use a window ending yesterday
    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    start_window = (target_dt - timedelta(days=14)).strftime("%Y-%m-%d")
    prev_day = (target_dt - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Get Volume Rank
    q_vol = f"""
    SELECT 
        goalie_id,
        sum_toi_L10,
        ROW_NUMBER() OVER (ORDER BY sum_toi_L10 DESC) as rank
    FROM fact_goalie_features
    WHERE team = '{team}' 
      AND game_date >= '{start_window}' 
      AND game_date < '{target_date}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY goalie_id ORDER BY game_date DESC) = 1
    ORDER BY rank
    """
    df_vol = con.execute(q_vol).df()
    
    if df_vol.empty:
        return None
        
    starter_id = df_vol.iloc[0]['goalie_id']
    backup_id = df_vol.iloc[1]['goalie_id'] if len(df_vol) > 1 else starter_id
    
    # 2. Check Previous Game (if B2B)
    if opp_b2b: # 'team' is on B2B
        q_prev = f"""
        SELECT 
            g.player_id
        FROM fact_goalie_game_situation g
        JOIN dim_games d ON g.game_id = d.game_id
        WHERE g.team = '{team}' AND d.game_date = '{prev_day}'
        ORDER BY g.toi_seconds DESC
        LIMIT 1
        """
        df_prev = con.execute(q_prev).df()
        if not df_prev.empty:
            prev_id = df_prev.iloc[0]['player_id']
            # If Starter played yesterday, pick Backup
            if prev_id == starter_id:
                return backup_id
            
    return starter_id

def load_lineup_overrides():
    """
    Loads manual overrides from data/overrides/manual_lineup_overrides.csv.
    Returns a dictionary keyed by Player Name (normalized) -> dict of overrides.
    """
    path = os.path.join(project_root, 'data', 'overrides', 'manual_lineup_overrides.csv')
    if not os.path.exists(path):
        return {}
        
    try:
        df = pd.read_csv(path)
        # Normalize keys
        overrides = {}
        for _, row in df.iterrows():
            p_name = row['player_name'] # Normalize if needed
            overrides[p_name] = {
                'proj_toi': row.get('projected_toi'),
                'pp_unit': row.get('pp_unit'),
                'line_number': row.get('line_number')
            }
        print(f"Loaded {len(overrides)} lineup overrides.")
        return overrides
    except Exception as e:
        print(f"Warning: Failed to load overrides: {e}")
        return {}

def get_player_id_map(con):
    """Maps Player Name -> Player ID using dim_players."""
    df = con.execute("SELECT player_name, player_id FROM dim_players").df()
    return df.set_index('player_name')['player_id'].to_dict()

def get_player_recent_features(con, game_date):
    """
    Fetches recent features (L10, L5, Last TOI) for all players.
    Returns dict: player_id -> feature_dict
    """
    query = f"""
    WITH base AS (
        SELECT 
            player_id,
            game_date,
            avg_toi_minutes_L10 as toi_L10,
            ev_toi_minutes_L5 as ev_toi_L5,
            pp_toi_minutes_L20 as pp_toi_L20,
            toi_minutes as last_toi, -- We want the PREVIOUS game's TOI
            LAG(toi_minutes, 1) OVER (PARTITION BY player_id ORDER BY game_date) as last_toi_2,
            ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_date DESC) as rn
        FROM fact_player_game_features
        WHERE game_date < '{game_date}'
    )
    SELECT * FROM base WHERE rn = 1
    """
    try:
        df = con.execute(query).df()
        # For 'last_toi', we actually grabbed the TOI from the most recent game (rn=1).
        # So 'last_toi' column IS the lagged value relative to *tomorrow*.
        # 'last_toi_2' is the game before that.
        return df.set_index('player_id').to_dict('index')
    except Exception as e:
        print(f"Warning: Failed to fetch player features: {e}")
        return {}

def predict_toi(player_row, feature_map, pid_map, model):
    """
    Predicts TOI using the loaded model.
    """
    if model is None:
        return None
        
    p_name = player_row.get('Player')
    if p_name not in pid_map:
        return None
        
    pid = pid_map[p_name]
    if pid not in feature_map:
        return None
        
    feats = feature_map[pid]
    
    # Feature Vector must match training: ['toi_L10', 'ev_toi_L5', 'pp_toi_L20', 'last_toi', 'last_toi_2', 'is_home']
    # Handle missing values (model handles NaNs, but let's be safe)
    try:
        # Check if last_toi is valid
        if pd.isna(feats.get('last_toi')):
            return None
            
        is_home = 1 # Assuming neutral/home if unknown, or check schedule? 
        # Schedule has 'IsHome'.
        # We can pass is_home from caller context if needed, but for now defaulting 0 or extracting from row if avail.
        # But 'df_base' doesn't usually have is_home. produce_game_context schedule loop does.
        # Wait, predict_toi is called inside the loop where we KNOW is_home?
        # No, predict_toi is called per player.
        # Let's assume neutral (0) or add is_home to player_row in the loop.
        
        # Actually, df_schedule has IsHome. We can pass it.
        is_home = player_row.get('is_home', 0)

        vector = [[
            feats.get('toi_L10'),
            feats.get('ev_toi_L5'),
            feats.get('pp_toi_L20'),
            feats.get('last_toi'),
            feats.get('last_toi_2'),
            is_home
        ]]
        
        pred = model.predict(vector)[0]
        return round(pred, 2)
    except Exception:
        return None

def main():
    print("--- Building Game Context ---")
    
    # 1. Load Schedule
    df_schedule, game_date = load_schedule_from_props()
    if df_schedule is None or df_schedule.empty:
        print("No schedule found. Exiting.")
        sys.exit(1)
        
    print(f"Target Game Date: {game_date}")
    
    # 2. Load Base Projections (to get list of players)
    if not os.path.exists(BASE_PROJ_PATH):
        print("Base projections not found.")
        sys.exit(1)
    df_base = pd.read_csv(BASE_PROJ_PATH)
    
    # 2.5 Load Overrides
    overrides = load_lineup_overrides()
    
    # 2.6 Load TOI Model
    toi_model = None
    if os.path.exists(MODEL_PATH):
        try:
            toi_model = joblib.load(MODEL_PATH)
            print(f"Loaded TOI Model from {MODEL_PATH}")
        except Exception as e:
            print(f"Failed to load TOI model: {e}")
    
    con = get_db_connection()
    
    # 3. Pre-fetch Data
    # A. Team Defense (Opponents)
    print("Fetching Team Defense Stats...")
    df_def = get_latest_stats(con, 'fact_team_defense_features', 'team', 'game_date', 
                              ['opp_sa60_L10', 'opp_xga60_L10'], game_date)
    def_map = df_def.set_index('team')[['opp_sa60_L10', 'opp_xga60_L10']].to_dict('index')
    
    # B. B2B Status
    print("Determining B2B Status...")
    b2b_teams = get_team_b2b_status(con, game_date)
    
    # C. Goalie Stats (L30 GSAx)
    print("Fetching Goalie Stats (L30)...")
    goalie_l30_map = get_goalie_stats_l30(con, game_date)

    # D. Team Pace (L10)
    print("Calculating Team Pace (L10)...")
    pace_map = get_team_pace_l10(con, game_date)
    
    # E. Player Features for TOI Model
    print("Fetching Player Features for TOI Model...")
    pid_map = get_player_id_map(con)
    player_feats = get_player_recent_features(con, game_date)

    # 4. Build Context Rows
    context_rows = []
    
    team_context_cache = {} 
    
    import numpy as np # Ensure numpy is avail
    
    # Helper to find if team is home/away in schedule
    # df_schedule has Team, OppTeam, IsHome
    team_home_map = df_schedule.set_index('Team')['IsHome'].to_dict()
    
    for _, row in df_schedule.iterrows():
        team = row['Team']
        opp = row['OppTeam']
        
        # 1. Team B2B
        is_b2b = 1 if team in b2b_teams else 0
        
        # 2. Opponent Defense (Raw)
        opp_def = def_map.get(opp, {'opp_sa60_L10': LG_SA60, 'opp_xga60_L10': LG_XGA60})
        raw_opp_sa60 = opp_def['opp_sa60_L10']
        raw_opp_xga60 = opp_def['opp_xga60_L10']
        
        # 3. Opponent Goalie
        opp_b2b = opp in b2b_teams
        goalie_id = get_likely_goalie(con, opp, opp_b2b, game_date)
        
        raw_gsax60 = 0.0
        if goalie_id and goalie_id in goalie_l30_map:
            raw_gsax60 = goalie_l30_map[goalie_id]['goalie_gsax60_L30']
            
        # 4. Pace (Team Pace + Opp Pace)
        # We average them or sum them?
        # Logic: Expected Pace ~ (Pace_A + Pace_B) / 2? Or relative to league?
        # Let's use (Team_Pace + Opp_Pace) / 2 as the Game Pace Estimate
        p_team = pace_map.get(team, {'pace_L10': LG_PACE})['pace_L10']
        p_opp = pace_map.get(opp, {'pace_L10': LG_PACE})['pace_L10']
        game_pace = (p_team + p_opp) / 2.0
        
        # 5. Compute Deltas (Log Space)
        # Avoid log(0)
        eps = 1e-6
        delta_opp_sog = np.log((raw_opp_sa60 + eps) / LG_SA60)
        delta_opp_xga = np.log((raw_opp_xga60 + eps) / LG_XGA60)
        delta_pace = np.log((game_pace + eps) / LG_PACE)
        
        # Goalie Delta (GSAx is already +/- 0.0, not a ratio)
        # So we just pass raw_gsax60. The beta will handle scaling.
        # But for consistency, let's call it delta_goalie
        delta_goalie = raw_gsax60 
        
        team_context_cache[team] = {
            'OppTeam': opp,
            'is_b2b': is_b2b,
            # Raw Fields (for Reference/Fallback)
            'opp_sa60': raw_opp_sa60,
            'opp_xga60': raw_opp_xga60,
            'goalie_gsax60': raw_gsax60,
            'game_pace': game_pace,
            # Deltas (for Model)
            'delta_opp_sog': delta_opp_sog,
            'delta_opp_xga': delta_opp_xga,
            'delta_goalie': delta_goalie,
            'delta_pace': delta_pace
        }
        
    con.close()
    
    # 5. Apply to Base Projections
    print("Applying context to players...")
    final_rows = []
    
    for _, row in df_base.iterrows():
        player = row['Player']
        team = row['Team']
        
        if team in team_context_cache:
            ctx = team_context_cache[team].copy() 
            
            # --- TOI RESOLUTION ---
            # 1. Base (Rolling Avg from Moneypuck or similar)
            # Use 'TOI' from base file (usually L10/L20 avg)
            base_toi = row.get('TOI', 15.0)
            
            # 2. Model Prediction
            proj_toi_model = -1.0
            if toi_model:
                # Prepare row for prediction (needs is_home)
                row_for_pred = row.to_dict()
                row_for_pred['is_home'] = team_home_map.get(team, 0)
                pred = predict_toi(row_for_pred, player_feats, pid_map, toi_model)
                if pred:
                    proj_toi_model = pred
            
            # 3. Overrides
            proj_toi_override = -1.0
            is_manual = 0
            if player in overrides:
                ovr = overrides[player]
                if pd.notna(ovr['proj_toi']):
                    proj_toi_override = float(ovr['proj_toi'])
                    is_manual = 1
                    if pd.notna(ovr['pp_unit']):
                        ctx['pp_unit'] = int(ovr['pp_unit'])

            # 4. Resolve Final
            # Hierarchy: Override > Model > Base
            if is_manual:
                proj_toi_final = proj_toi_override
            elif proj_toi_model > 0:
                proj_toi_final = proj_toi_model
            else:
                proj_toi_final = base_toi # Fallback to rolling avg
            
            # Update Context
            ctx['proj_toi'] = proj_toi_final
            ctx['proj_toi_model'] = proj_toi_model
            ctx['is_manual_toi'] = is_manual
            
            final_rows.append({
                'Player': player,
                'Team': team,
                'OppTeam': ctx['OppTeam'],
                'is_b2b': ctx['is_b2b'],
                # TOI Fields
                'proj_toi': ctx['proj_toi'],
                'proj_toi_model': ctx['proj_toi_model'],
                'is_manual_toi': ctx['is_manual_toi'],
                'pp_unit': ctx.get('pp_unit', -1),
                # Deltas
                'delta_opp_sog': round(ctx['delta_opp_sog'], 4),
                'delta_opp_xga': round(ctx['delta_opp_xga'], 4),
                'delta_goalie': round(ctx['delta_goalie'], 4),
                'delta_pace': round(ctx['delta_pace'], 4),
                # Raw (Optional, but good for debug)
                'opp_sa60': round(ctx['opp_sa60'], 2),
                'opp_xga60': round(ctx['opp_xga60'], 2),
                'goalie_gsax60': round(ctx['goalie_gsax60'], 3),
                'game_pace': round(ctx['game_pace'], 2)
            })
            
    if not final_rows:
        print("Warning: No context rows generated. Check Team Mappings.")
    
    df_final = pd.DataFrame(final_rows)
    df_final.to_csv(OUTPUT_PATH, index=False)
    print(f"Successfully wrote {len(df_final)} rows to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()

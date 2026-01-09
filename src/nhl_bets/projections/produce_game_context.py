import pandas as pd
import duckdb
import os
import sys
import argparse
from datetime import datetime, timedelta

# Add src to path for nhl_bets import
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(os.path.dirname(current_dir))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

try:
    from nhl_bets.analysis.normalize import TEAM_MAP, get_teams_from_slug
except ImportError as e:
    print(f"Error: Could not import normalization utils from nhl_bets.analysis: {e}")
    sys.exit(1)

project_root = os.path.dirname(src_dir)

DB_PATH = os.path.join(project_root, 'data', 'db', 'nhl_backtest.duckdb')
PROPS_PATH = os.path.join(project_root, 'data', 'raw', 'nhl_player_props_all.csv')
BASE_PROJ_PATH = os.path.join(project_root, 'outputs', 'projections', 'BaseSingleGameProjections.csv')
OUTPUT_PATH = os.path.join(project_root, 'outputs', 'projections', 'GameContext.csv')

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
    
    con = get_db_connection()
    
    # 3. Pre-fetch Data
    # A. Team Defense (Opponents)
    # We fetch L10 stats for ALL teams to be safe/fast
    print("Fetching Team Defense Stats...")
    df_def = get_latest_stats(con, 'fact_team_defense_features', 'team', 'game_date', 
                              ['opp_sa60_L10', 'opp_xga60_L10'], game_date)
    def_map = df_def.set_index('team')[['opp_sa60_L10', 'opp_xga60_L10']].to_dict('index')
    
    # B. B2B Status
    print("Determining B2B Status...")
    b2b_teams = get_team_b2b_status(con, game_date)
    
    # C. Goalie Stats (Latest for all goalies)
    print("Fetching Goalie Stats...")
    df_goalie_stats = get_latest_stats(con, 'fact_goalie_features', 'goalie_id', 'game_date', 
                                       ['goalie_gsax60_L10', 'sum_xga_L10', 'sum_toi_L10'], game_date)
    # Calculate xGA60
    df_goalie_stats['goalie_xga60'] = df_goalie_stats.apply(
        lambda r: r['sum_xga_L10'] / (r['sum_toi_L10'] / 3600) if r['sum_toi_L10'] > 0 else 0, axis=1
    )
    goalie_map = df_goalie_stats.set_index('goalie_id')[['goalie_gsax60_L10', 'goalie_xga60']].to_dict('index')

    # 4. Build Context Rows
    context_rows = []
    
    # Group Base by Team to minimize team-level lookups
    # But we need to handle players individually or join. 
    # Let's iterate unique teams from schedule.
    
    team_context_cache = {} # Team -> {OppTeam, opp_sa60, opp_xga60, goalie_gsax60, goalie_xga60, is_b2b}
    
    for _, row in df_schedule.iterrows():
        team = row['Team']
        opp = row['OppTeam']
        
        # 1. Team B2B (Player's Team)
        is_b2b = 1 if team in b2b_teams else 0
        
        # 2. Opponent Defense
        opp_def = def_map.get(opp, {'opp_sa60_L10': 30.0, 'opp_xga60_L10': 2.5}) # Defaults
        
        # 3. Opponent Goalie
        opp_b2b = opp in b2b_teams
        goalie_id = get_likely_goalie(con, opp, opp_b2b, game_date)
        
        g_stats = {'goalie_gsax60_L10': 0.0, 'goalie_xga60': 2.5}
        if goalie_id and goalie_id in goalie_map:
            g_stats = goalie_map[goalie_id]
            
        team_context_cache[team] = {
            'OppTeam': opp,
            'opp_sa60': opp_def['opp_sa60_L10'],
            'opp_xga60': opp_def['opp_xga60_L10'],
            'goalie_gsax60': g_stats['goalie_gsax60_L10'],
            'goalie_xga60': g_stats['goalie_xga60'],
            'is_b2b': is_b2b
        }
        
    con.close()
    
    # 5. Apply to Base Projections
    print("Applying context to players...")
    final_rows = []
    
    for _, row in df_base.iterrows():
        player = row['Player']
        team = row['Team']
        
        if team in team_context_cache:
            ctx = team_context_cache[team].copy() # Copy to avoid polluting shared team cache
            
            # CHECK OVERRIDES
            if player in overrides:
                ovr = overrides[player]
                # Documentation Constraint: projected_toi is REQUIRED for any override to take effect.
                # "If pp_unit is specified but no projected_toi is provided, no change occurs."
                if pd.notna(ovr['proj_toi']):
                    ctx['proj_toi'] = float(ovr['proj_toi'])
                    ctx['is_manual_toi'] = 1 # Flag for audit
                    
                    # Only apply PP unit if TOI is also overridden
                    if pd.notna(ovr['pp_unit']):
                        ctx['pp_unit'] = int(ovr['pp_unit'])
            
            final_rows.append({
                'Player': player,
                'Team': team,
                'OppTeam': ctx['OppTeam'],
                'opp_sa60': ctx['opp_sa60'],
                'opp_xga60': ctx['opp_xga60'],
                'goalie_gsax60': ctx['goalie_gsax60'],
                'goalie_xga60': ctx['goalie_xga60'],
                'is_b2b': ctx['is_b2b'],
                'proj_toi': ctx.get('proj_toi', -1.0),
                'pp_unit': ctx.get('pp_unit', -1),
                'is_manual_toi': ctx.get('is_manual_toi', 0)
            })
            
    if not final_rows:
        print("Warning: No context rows generated. Check Team Mappings.")
    
    df_final = pd.DataFrame(final_rows)
    df_final.to_csv(OUTPUT_PATH, index=False)
    print(f"Successfully wrote {len(df_final)} rows to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()

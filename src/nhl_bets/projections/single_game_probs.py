import pandas as pd
import numpy as np
import logging
import os
import sys
import argparse
from datetime import datetime

# Ensure we can import from project root
# current_dir: .../src/nhl_bets/projections
# src_dir: .../src
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(os.path.dirname(current_dir))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

try:
    from nhl_bets.projections.single_game_model import compute_game_probs
    from nhl_bets.projections.config import BETAS, ALPHAS, LG_SA60, LG_XGA60, ITT_BASE
except ImportError as e:
    # Fallback if running from root directly without package structure recognition issues
    # But usually sys.path insertion fixes it.
    print(f"Error importing nhl_bets: {e}")
    sys.exit(1)

# --- Configuration & Constants ---
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Column mapping for robustness
COLUMN_ALIASES = {
    'Player': 'Player', 'player': 'Player', 'Name': 'Player',
    'Team': 'Team', 'team': 'Team',
    'OppTeam': 'OppTeam', 'opp_team': 'OppTeam',
    'Pos': 'Pos', 'pos': 'Pos',
    'GP': 'GP', 'Games': 'GP',
    'TOI': 'TOI', 'toi': 'TOI', 'Time On Ice': 'TOI',
    'PP TOI': 'PPTOI', 'PPTOI': 'PPTOI', 'pp_toi': 'PPTOI',
    'G': 'G', 'Goals': 'G', 'mu_base_goals': 'G',
    'A': 'A', 'Assists': 'A', 'Assists Per Game': 'A',
    'PTS': 'PTS', 'Points': 'PTS', 'Points Per Game': 'PTS',
    'SOG': 'SOG', 'Shots': 'SOG', 'SOG Per Game': 'SOG',
    'BLK': 'BLK', 'Blocks': 'BLK', 'Blocks Per Game': 'BLK'
}

REQUIRED_STATS = ['G', 'A', 'PTS', 'SOG', 'BLK']

def normalize_columns(df):
    """Normalize column names based on COLUMN_ALIASES."""
    # Create a mapping dictionary where key is current col name, value is target
    rename_map = {}
    for col in df.columns:
        # Check exact match or case-insensitive match
        if col in COLUMN_ALIASES:
            rename_map[col] = COLUMN_ALIASES[col]
        else:
            # Try finding a match in keys (case insensitive check could be added if needed)
            for alias, target in COLUMN_ALIASES.items():
                if col.lower() == alias.lower():
                    rename_map[col] = target
                    break
    
    if rename_map:
        df = df.rename(columns=rename_map)
        # Keep original columns if not mapped, but mapped ones are now standard
    return df

def load_data():
    # Attempt to locate files in outputs/projections relative to project root
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
    
    base_file = os.path.join(project_root, 'outputs', 'projections', 'BaseSingleGameProjections.csv')
    context_file = os.path.join(project_root, 'outputs', 'projections', 'GameContext.csv')
    
    if not os.path.exists(base_file):
        # Fallback to current directory or arguments (if we added args, but we hardcode for now)
        base_file = 'BaseSingleGameProjections.csv'
        context_file = 'GameContext.csv'
    
    if not os.path.exists(base_file):
        logger.error(f"Base file not found: {base_file}")
        sys.exit(1)
        
    try:
        df_base = pd.read_csv(base_file)
        logger.info(f"Loaded {base_file} with {len(df_base)} rows.")
    except Exception as e:
        logger.error(f"Failed to read {base_file}: {e}")
        sys.exit(1)
        
    df_base = normalize_columns(df_base)
    
    # Check for missing required columns
    missing_cols = []
    for stat in REQUIRED_STATS:
        if stat not in df_base.columns:
            missing_cols.append(stat)
    
    if missing_cols:
        logger.error(f"Missing required stat columns in base file: {missing_cols}")
        # Note: Depending on logic, we might proceed if some exist, but prompt implies strictness or clear error.
        sys.exit(1)

    # Check for Game Context
    df_context = None
    if os.path.exists(context_file):
        try:
            df_context = pd.read_csv(context_file)
            df_context = normalize_columns(df_context)
            logger.info(f"Loaded {context_file} with {len(df_context)} rows.")
        except Exception as e:
            logger.warning(f"Found {context_file} but failed to read it: {e}. Proceeding without context.")
    else:
        logger.info("No GameContext.csv found. Running with default multipliers (1.0).")
        
    return df_base, df_context

def process_base_projections(df):
    """
    Convert total stats to per-game stats if necessary.
    Returns dataframe with per-game columns and TOI columns.
    """
    df = df.copy()
    
    # Detect if per-game or totals
    is_per_game = False
    
    # Check for markers of already-processed rate data
    rate_markers = ['Realized Goals Per Game', 'Assists Per Game', 'SOG Per Game', 'mu_base_goals']
    if any(m in df.columns for m in rate_markers):
        logger.info("Detected Live Bridge or rate-based columns. Treating as Per-Game.")
        is_per_game = True
    elif 'GP' not in df.columns:
        logger.info("GP column missing. Assuming data is already Per-Game.")
        is_per_game = True
    else:
        # Check if GP is mostly 1
        if df['GP'].mean() < 1.1: # Heuristic
            logger.info("GP exists but looks like single-game data (mean ~ 1). Treating as Per-Game.")
            is_per_game = True
        else:
            logger.info("GP detected with values > 1. Converting totals to Per-Game.")
            
    # Normalize TOI if it's totals
    # If per-game, TOI is usually minutes (e.g. 15.5). If totals, it might be hundreds.
    # Simple heuristic: if max(TOI) > 60, it's probably totals or seconds (unlikely seconds if GP is high).
    # Assuming minutes for simplicity if is_per_game is True.
    
    if not is_per_game:
        # Convert Totals to Per Game
        for col in REQUIRED_STATS:
            if col in df.columns:
                df[col] = df[col] / df['GP']
        
        # Normalize TOI
        if 'TOI' in df.columns:
            df['TOI'] = df['TOI'] / df['GP']
        if 'PPTOI' in df.columns:
            df['PPTOI'] = df['PPTOI'] / df['GP']
            
        # TOI handling
        if 'TOI' in df.columns:
            df['TOI'] = df['TOI'] / df['GP']
        if 'PPTOI' in df.columns:
            df['PPTOI'] = df['PPTOI'] / df['GP']
            
    # Add note
    df['notes'] = "Base: Per-Game" if is_per_game else "Base: Totals (Converted)"
    
    return df

def main():
    parser = argparse.ArgumentParser(description="Generate Single Game Probabilities")
    parser.add_argument("--date", help="Game Date (YYYY-MM-DD)", default=None)
    parser.add_argument("--calibration_mode", choices=['global', 'segmented'], default='global', help="Calibration Mode")
    args = parser.parse_args()

    game_date = args.date
    if not game_date:
        game_date = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"No date provided. Using today's date: {game_date}")
    else:
        logger.info(f"Using provided date: {game_date}")

    df_base, df_context = load_data()
    
    # 1. Standardize Base Projections (Per Game)
    df_proc = process_base_projections(df_base)
    
    # 2. Merge Context if available
    if df_context is not None:
        # Columns of interest in context
        ctx_cols = ['Player', 'opp_sa60', 'opp_xga60', 'goalie_gsax60', 'goalie_xga60', 
                    'implied_team_total', 'is_b2b', 'proj_toi', 'proj_pp_toi', 'OppTeam',
                    'pp_unit', 'is_manual_toi', 'proj_toi_model',
                    'delta_opp_sog', 'delta_opp_xga', 'delta_goalie', 'delta_pace',
                    'cluster_id']
        
        # Filter only existing columns
        existing_ctx_cols = [c for c in ctx_cols if c in df_context.columns]
        
        # Merge
        # Check if Player exists in both
        if 'Player' in df_proc.columns and 'Player' in df_context.columns:
             df_proc = df_proc.merge(df_context[existing_ctx_cols], on='Player', how='left')
             logger.info("Merged GameContext data.")
        else:
            logger.warning("Could not merge Context: 'Player' column missing in one of the files.")
    
    # 3. Calculate Final Mu for each stat
    results = []
    
    for idx, row in df_proc.iterrows():
        # Identifiers
        player_name = row.get('Player', 'Unknown')
        team = row.get('Team', '')
        opp_team = row.get('OppTeam', '')
        
        # Prepare Data for shared engine - Pass all columns to ensure features like ev_ast_60_L40 are available
        player_data = row.to_dict()
        
        # Ensure standard keys exist for basic stats if they were renamed
        # normalize_columns already does this, but we explicitly map G_realized if needed
        if 'G_realized' not in player_data:
            player_data['G_realized'] = row.get('Realized Goals Per Game', 0)
        
        context_data = {
            'opp_sa60': row.get('opp_sa60'),
            'opp_xga60': row.get('opp_xga60'),
            'goalie_gsax60': row.get('goalie_gsax60'),
            'goalie_xga60': row.get('goalie_xga60'),
            'implied_team_total': row.get('implied_team_total'),
            'is_b2b': row.get('is_b2b'),
            'pp_unit': row.get('pp_unit'),
            'proj_toi': row.get('proj_toi'),
            'delta_opp_sog': row.get('delta_opp_sog'),
            'delta_opp_xga': row.get('delta_opp_xga'),
            'delta_goalie': row.get('delta_goalie'),
            'delta_pace': row.get('delta_pace'),
            'cluster_id': row.get('cluster_id'),
            'calibration_mode': args.calibration_mode
        }
        
        # Call shared engine
        calcs = compute_game_probs(player_data, context_data)
        
        # Unpack Results
        probs_g = calcs['probs_goals']
        probs_a = calcs['probs_assists']
        probs_pts = calcs['probs_points']
        probs_a_cal = calcs['probs_assists_calibrated']
        probs_pts_cal = calcs['probs_points_calibrated']
        probs_sog = calcs['probs_sog']
        probs_blk = calcs['probs_blocks']
        
        # -- Record Result --
        res = {
            'Date': game_date,
            'Player': player_name,
            'Team': team,
            'OppTeam': opp_team,
            
            'mu_adj_G': round(calcs['mu_goals'], 4),
            'mu_adj_A': round(calcs['mu_assists'], 4),
            'mu_adj_PTS': round(calcs['mu_points'], 4),
            'mu_adj_SOG': round(calcs['mu_sog'], 4),
            'mu_adj_BLK': round(calcs['mu_blocks'], 4),
            
            'p_G_1plus': round(probs_g.get(1, 0), 4),
            'p_G_2plus': round(probs_g.get(2, 0), 4),
            'p_G_3plus': round(probs_g.get(3, 0), 4),
            
            'p_A_1plus': round(probs_a.get(1, 0), 4),
            'p_A_1plus_calibrated': round(probs_a_cal.get(1, 0), 4),
            'p_A_2plus': round(probs_a.get(2, 0), 4),
            'p_A_2plus_calibrated': round(probs_a_cal.get(2, 0), 4),
            'p_A_3plus': round(probs_a.get(3, 0), 4),
            
            'p_PTS_1plus': round(probs_pts.get(1, 0), 4),
            'p_PTS_1plus_calibrated': round(probs_pts_cal.get(1, 0), 4),
            'p_PTS_2plus': round(probs_pts.get(2, 0), 4),
            'p_PTS_2plus_calibrated': round(probs_pts_cal.get(2, 0), 4),
            'p_PTS_3plus': round(probs_pts.get(3, 0), 4),
            
            'p_SOG_1plus': round(probs_sog.get(1, 0), 4),
            'p_SOG_2plus': round(probs_sog.get(2, 0), 4),
            'p_SOG_3plus': round(probs_sog.get(3, 0), 4),
            'p_SOG_4plus': round(probs_sog.get(4, 0), 4),
            'p_SOG_5plus': round(probs_sog.get(5, 0), 4),
            
            'p_BLK_1plus': round(probs_blk.get(1, 0), 4),
            'p_BLK_2plus': round(probs_blk.get(2, 0), 4),
            'p_BLK_3plus': round(probs_blk.get(3, 0), 4),
            'p_BLK_4plus': round(probs_blk.get(4, 0), 4),
            
            'mult_opp_sog': round(calcs['mult_opp_sog'], 3),
            'mult_opp_g': round(calcs['mult_opp_g'], 3),
            'mult_goalie': round(calcs['mult_goalie'], 3),
            'mult_itt': round(calcs['mult_itt'], 3),
            'mult_b2b': round(calcs['mult_b2b'], 3),
            'mult_pace': round(calcs.get('mult_pace', 1.0), 3),
            
            'delta_opp_sog': row.get('delta_opp_sog', 0.0),
            'delta_opp_xga': row.get('delta_opp_xga', 0.0),
            'delta_goalie': row.get('delta_goalie', 0.0),
            'delta_pace': row.get('delta_pace', 0.0),

            'notes': row.get('notes', ''),
            'is_manual_toi': row.get('is_manual_toi', 0),
            'proj_toi_model': row.get('proj_toi_model', -1.0)
        }
        results.append(res)

    # Create DF and Save
    df_results = pd.DataFrame(results)
    
    # Save to same directory as script (or current working dir if relative)
    # Determine project root and output path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
    output_dir = os.path.join(project_root, 'outputs', 'projections')
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'SingleGamePropProbabilities.csv')
    
    df_results.to_csv(output_file, index=False)

if __name__ == "__main__":
    main()

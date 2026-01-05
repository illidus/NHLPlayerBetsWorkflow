
import duckdb
import pandas as pd
import re
from datetime import datetime

# DB Connection for date resolution
DB_PATH = '../20_data_store/nhl_backtest.duckdb'

# Team Map (Slug -> DB Code)
TEAM_SLUG_MAP = {
    'anaheim-ducks': 'ANA',
    'boston-bruins': 'BOS',
    'buffalo-sabres': 'BUF',
    'carolina-hurricanes': 'CAR',
    'columbus-blue-jackets': 'CBJ',
    'calgary-flames': 'CGY',
    'chicago-blackhawks': 'CHI',
    'colorado-avalanche': 'COL',
    'dallas-stars': 'DAL',
    'detroit-red-wings': 'DET',
    'edmonton-oilers': 'EDM',
    'florida-panthers': 'FLA',
    'los-angeles-kings': 'LAK',
    'minnesota-wild': 'MIN',
    'montreal-canadiens': 'MTL',
    'new-jersey-devils': 'NJD',
    'nashville-predators': 'NSH',
    'new-york-islanders': 'NYI',
    'new-york-rangers': 'NYR',
    'ottawa-senators': 'OTT',
    'philadelphia-flyers': 'PHI',
    'pittsburgh-penguins': 'PIT',
    'san-jose-sharks': 'SJS',
    'seattle-kraken': 'SEA',
    'st-louis-blues': 'STL',
    'tampa-bay-lightning': 'TBL',
    'toronto-maple-leafs': 'TOR',
    'utah-mammoth': 'UTA',
    'utah-hockey-club': 'UTA',
    'vancouver-canucks': 'VAN',
    'vegas-golden-knights': 'VGK',
    'winnipeg-jets': 'WPG',
    'washington-capitals': 'WSH'
}

def get_teams_from_slug(slug):
    if '-at-' not in slug:
        return None, None
    away_slug, home_slug = slug.split('-at-')
    away = TEAM_SLUG_MAP.get(away_slug)
    home = TEAM_SLUG_MAP.get(home_slug)
    return away, home

def resolve_game_date(con, away_team, home_team, min_date='2024-09-01'):
    """
    Finds the game date for Away @ Home in the DB.
    Prioritizes dates > min_date (2024-2025 season).
    """
    if not away_team or not home_team:
        return None
        
    query = f"""
    SELECT game_date 
    FROM fact_skater_game_all 
    WHERE team = '{away_team}' 
    AND opp_team = '{home_team}'
    AND game_date >= '{min_date}'
    ORDER BY game_date ASC
    LIMIT 1
    """
    
    res = con.execute(query).fetchone()
    if res:
        return res[0] # datetime object
    
    # Fallback to older dates if not found in current season (for robustness)
    query_fallback = f"""
    SELECT game_date 
    FROM fact_skater_game_all 
    WHERE team = '{away_team}' 
    AND opp_team = '{home_team}'
    ORDER BY game_date DESC
    LIMIT 1
    """
    res = con.execute(query_fallback).fetchone()
    if res:
        return res[0]
        
    return None

def normalize_market(market_str, player_name_col, line_col):
    """
    Parses 'Market' column to extract Player, Market Type, Line.
    Example: 'Boone Jenner Total Points 0.5'
    Returns: player_name, market_type, line, side_hint
    """
    # Known markets
    # Goal Scorer (Anytime) - REMOVED: Odds analysis shows this is FGS (10.0+ odds), not Anytime.
    if market_str == 'Goal Scorer':
        return None, None, None, None
    
    # Regex for "Player Total Stat Line"
    # e.g. "Boone Jenner Total Points 0.5"
    # e.g. "Tage Thompson Total Shots On Goal 3.5"
    
    # We strip the player name from the start? 
    # Or just Regex the end.
    
    # Pattern: (Player Name) Total (Stat) (Line)
    match = re.search(r'^(.*) Total (Points|Shots On Goal|Assists|Powerplay Points) (\d+\.?\d*)$', market_str)
    if match:
        p_name = match.group(1).strip()
        stat_raw = match.group(2).strip()
        line = float(match.group(3))
        
        stat_map = {
            'Points': 'POINTS',
            'Shots On Goal': 'SOG',
            'Assists': 'ASSISTS',
            'Powerplay Points': 'PPP'
        }
        market_type = stat_map.get(stat_raw, 'UNKNOWN')
        
        return p_name, market_type, line, None # Side unknown here
        
    # Check First Goalscorer
    if 'First Goalscorer' in market_str:
        # e.g. "Buffalo Sabres First Goalscorer"
        # usually ignored for EV unless we have model
        return None, None, None, None

    return None, None, None, None

def infer_side(odds_decimal, market_type, is_first_row=True):
    """
    Infers side (over/under) based on odds and row position.
    Heuristic: 
    - For Goal Scorer (Anytime), it's always Over.
    - For Total props, usually Over is first.
    """
    if market_type == 'GOALS':
        return 'over'
        
    # For others, assume Row 1 = Over, Row 2 = Under
    if is_first_row:
        return 'over'
    else:
        return 'under'


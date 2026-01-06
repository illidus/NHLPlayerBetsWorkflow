import re
import difflib
import duckdb
import logging
import pandas as pd
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

TEAM_MAP = {
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

# Reverse team map for matching
REVERSE_TEAM_MAP = {v: k for k, v in TEAM_MAP.items()}

def normalize_name(name):
    """
    Normalizes player name:
    - Lowercase
    - Strip whitespace
    - Remove punctuation
    - Remove parentheticals like (F), (CAR)
    """
    if not isinstance(name, str):
        return ""

    # Remove parentheticals (e.g. "Elias Pettersson (F)")
    name = re.sub(r'\s*\(.*?\)', '', name)

    # Remove punctuation (keep spaces)
    name = re.sub(r'[^\w\s]', '', name)

    # Compress spaces
    name = re.sub(r'\s+', ' ', name)

    return name.strip().lower()

def get_teams_from_slug(game_slug):
    """
    Extracts (Away, Home) abbreviations from game slug.
    Slug format: 'away-team-slug-at-home-team-slug'
    """
    if '-at-' not in game_slug:
        return None, None

    away_slug, home_slug = game_slug.split('-at-')

    away_abbr = TEAM_MAP.get(away_slug)
    home_abbr = TEAM_MAP.get(home_slug)

    return away_abbr, home_abbr

def fuzzy_match_player(name, candidates, threshold=0.90):
    """
    Finds best match for name in candidates list.
    Returns (matched_name, score).
    """
    best_match = None
    best_score = 0.0

    norm_name = normalize_name(name)

    for cand in candidates:
        norm_cand = normalize_name(cand)

        # Exact match check first (normalized)
        if norm_name == norm_cand:
            return cand, 1.0

        # SequenceMatcher
        ratio = difflib.SequenceMatcher(None, norm_name, norm_cand).ratio()

        if ratio > best_score:
            best_score = ratio
            best_match = cand

    if best_score >= threshold:
        return best_match, best_score

    return None, best_score

def update_player_mappings(con: duckdb.DuckDBPyConnection):
    """
    Attempts to map raw player names from fact_prop_odds to canonical player_ids.
    """
    logger.info("Updating player mappings...")
    
    # 1. Exact Name Match
    con.execute("""
    INSERT INTO dim_players_mapping (vendor_player_name, source_vendor, canonical_player_id)
    SELECT DISTINCT raw.player_name_raw, raw.source_vendor, p.player_id
    FROM fact_prop_odds raw
    JOIN dim_players p ON LOWER(raw.player_name_raw) = LOWER(p.player_name)
    LEFT JOIN dim_players_mapping m ON 
        raw.player_name_raw = m.vendor_player_name AND 
        raw.source_vendor = m.source_vendor
    WHERE m.vendor_player_name IS NULL
    """)
    
    # 2. Add vendor_player_id if available
    con.execute("""
    UPDATE dim_players_mapping m
    SET vendor_player_id = raw.player_id_vendor
    FROM fact_prop_odds raw
    WHERE m.vendor_player_name = raw.player_name_raw
      AND m.source_vendor = raw.source_vendor
      AND m.vendor_player_id IS NULL
      AND raw.player_id_vendor IS NOT NULL
    """)
    
    res = con.execute("SELECT count(*) FROM dim_players_mapping").fetchone()
    logger.info(f"Mapped {res[0]} unique player-vendor pairs.")

# Mapping full team names to abbreviations
TEAM_NAME_TO_ABBR = {
    'Anaheim Ducks': 'ANA',
    'Boston Bruins': 'BOS',
    'Buffalo Sabres': 'BUF',
    'Carolina Hurricanes': 'CAR',
    'Columbus Blue Jackets': 'CBJ',
    'Calgary Flames': 'CGY',
    'Chicago Blackhawks': 'CHI',
    'Colorado Avalanche': 'COL',
    'Dallas Stars': 'DAL',
    'Detroit Red Wings': 'DET',
    'Edmonton Oilers': 'EDM',
    'Florida Panthers': 'FLA',
    'Los Angeles Kings': 'LAK',
    'Minnesota Wild': 'MIN',
    'Montreal Canadiens': 'MTL',
    'New Jersey Devils': 'NJD',
    'Nashville Predators': 'NSH',
    'New York Islanders': 'NYI',
    'New York Rangers': 'NYR',
    'Ottawa Senators': 'OTT',
    'Philadelphia Flyers': 'PHI',
    'Pittsburgh Penguins': 'PIT',
    'San Jose Sharks': 'SJS',
    'Seattle Kraken': 'SEA',
    'St. Louis Blues': 'STL',
    'Tampa Bay Lightning': 'TBL',
    'Toronto Maple Leafs': 'TOR',
    'Utah Mammoth': 'UTA',
    'Utah Hockey Club': 'UTA',
    'Vancouver Canucks': 'VAN',
    'Vegas Golden Knights': 'VGK',
    'Winnipeg Jets': 'WPG',
    'Washington Capitals': 'WSH'
}

def update_event_mappings(con: duckdb.DuckDBPyConnection):
    """
    Attempts to map vendor event IDs to canonical game_ids.
    Strategy: Link vendor event to dim_games if teams and date match.
    """
    logger.info("Updating event mappings...")
    
    # 1. Map based on abbreviations (Unabated, OddsShark)
    con.execute("""
    INSERT INTO dim_events_mapping (vendor_event_id, source_vendor, canonical_game_id)
    SELECT DISTINCT raw.event_id_vendor, raw.source_vendor, g.game_id
    FROM fact_prop_odds raw
    JOIN dim_games g ON 
        (TRIM(raw.home_team) = g.home_team AND TRIM(raw.away_team) = g.away_team) OR
        (TRIM(raw.home_team) = g.away_team AND TRIM(raw.away_team) = g.home_team)
    LEFT JOIN dim_events_mapping em ON raw.event_id_vendor = em.vendor_event_id AND raw.source_vendor = em.source_vendor
    WHERE em.vendor_event_id IS NULL
      AND ABS(DATEDIFF('day', CAST(raw.capture_ts_utc AS DATE), CAST(g.game_date AS DATE))) <= 1
      AND raw.home_team IS NOT NULL AND raw.away_team IS NOT NULL
    """)
    
    # 2. Map based on full names (PlayNow)
    con.register("team_name_map", pd.DataFrame(list(TEAM_NAME_TO_ABBR.items()), columns=['name', 'abbr']))
    con.execute("""
    INSERT INTO dim_events_mapping (vendor_event_id, source_vendor, canonical_game_id)
    SELECT DISTINCT raw.event_id_vendor, raw.source_vendor, g.game_id
    FROM fact_prop_odds raw
    JOIN team_name_map h ON TRIM(raw.home_team) = h.name
    JOIN team_name_map a ON TRIM(raw.away_team) = a.name
    JOIN dim_games g ON 
        (h.abbr = g.home_team AND a.abbr = g.away_team) OR
        (h.abbr = g.away_team AND a.abbr = g.home_team)
    LEFT JOIN dim_events_mapping em ON raw.event_id_vendor = em.vendor_event_id AND raw.source_vendor = em.source_vendor
    WHERE em.vendor_event_id IS NULL
      AND ABS(DATEDIFF('day', CAST(raw.capture_ts_utc AS DATE), CAST(g.game_date AS DATE))) <= 1
    """)
    
    res = con.execute("SELECT count(*) FROM dim_events_mapping").fetchone()
    logger.info(f"Mapped {res[0]} unique event-vendor pairs.")

def get_mapped_odds(con: duckdb.DuckDBPyConnection):
    """
    Returns a view of fact_prop_odds joined with canonical keys.
    """
    return con.execute("""
    SELECT 
        o.*,
        pm.canonical_player_id,
        em.canonical_game_id
    FROM fact_prop_odds o
    LEFT JOIN dim_players_mapping pm ON o.player_name_raw = pm.vendor_player_name AND o.source_vendor = pm.source_vendor
    LEFT JOIN dim_events_mapping em ON o.event_id_vendor = em.vendor_event_id AND o.source_vendor = em.source_vendor
    """).df()

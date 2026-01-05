import re
import difflib

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
    'los-angeles-kings': 'L.A',
    'minnesota-wild': 'MIN',
    'montreal-canadiens': 'MTL',
    'new-jersey-devils': 'N.J',
    'nashville-predators': 'NSH',
    'new-york-islanders': 'NYI',
    'new-york-rangers': 'NYR',
    'ottawa-senators': 'OTT',
    'philadelphia-flyers': 'PHI',
    'pittsburgh-penguins': 'PIT',
    'san-jose-sharks': 'S.J',
    'seattle-kraken': 'SEA',
    'st-louis-blues': 'STL',
    'tampa-bay-lightning': 'T.B',
    'toronto-maple-leafs': 'TOR',
    'utah-mammoth': 'UTA',
    'utah-hockey-club': 'UTA', # Just in case
    'vancouver-canucks': 'VAN',
    'vegas-golden-knights': 'VGK',
    'winnipeg-jets': 'WPG',
    'washington-capitals': 'WSH'
}

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
    # Use difflib.get_close_matches but getting the score is manual.
    # We'll implementation ratio check.
    
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

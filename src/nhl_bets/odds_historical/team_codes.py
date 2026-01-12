import re

NHL_TEAM_CODES = {
    # Atlantic
    "BOSTON BRUINS": "BOS",
    "BUFFALO SABRES": "BUF",
    "DETROIT RED WINGS": "DET",
    "FLORIDA PANTHERS": "FLA",
    "MONTREAL CANADIENS": "MTL",
    "MONTREAL CANADIENS": "MTL", # Normalized form usually handles accents, but explicit ok
    "OTTAWA SENATORS": "OTT",
    "TAMPA BAY LIGHTNING": "TBL",
    "TORONTO MAPLE LEAFS": "TOR",
    
    # Metropolitan
    "CAROLINA HURRICANES": "CAR",
    "COLUMBUS BLUE JACKETS": "CBJ",
    "NEW JERSEY DEVILS": "NJD",
    "NEW YORK ISLANDERS": "NYI",
    "NY ISLANDERS": "NYI",
    "NEW YORK RANGERS": "NYR",
    "NY RANGERS": "NYR",
    "PHILADELPHIA FLYERS": "PHI",
    "PITTSBURGH PENGUINS": "PIT",
    "WASHINGTON CAPITALS": "WSH",
    
    # Central
    "ARIZONA COYOTES": "ARI", # Legacy support
    "UTAH HOCKEY CLUB": "UTA",
    "UTAH HC": "UTA",
    "CHICAGO BLACKHAWKS": "CHI",
    "COLORADO BLACKHAWKS": "COL", # Wait, typo in standard map? No, COLORADO AVALANCHE
    "COLORADO AVALANCHE": "COL",
    "DALLAS STARS": "DAL",
    "MINNESOTA WILD": "MIN",
    "NASHVILLE PREDATORS": "NSH",
    "ST LOUIS BLUES": "STL",
    "SAINT LOUIS BLUES": "STL",
    "WINNIPEG JETS": "WPG",
    
    # Pacific
    "ANAHEIM DUCKS": "ANA",
    "CALGARY FLAMES": "CGY",
    "EDMONTON OILERS": "EDM",
    "LOS ANGELES KINGS": "LAK",
    "LA KINGS": "LAK",
    "SAN JOSE SHARKS": "SJS",
    "SEATTLE KRAKEN": "SEA",
    "VANCOUVER CANUCKS": "VAN",
    "VEGAS GOLDEN KNIGHTS": "VGK"
}

def resolve_team_code(team_raw):
    """
    Resolves a raw team name to a 3-letter NHL code.
    Normalization: Upper, strip, remove punctuation (dots, apostrophes).
    """
    if not team_raw:
        return None
    
    # 1. Normalize
    # Remove dots (St. Louis), apostrophes (O'Connor - wait, teams? No apostrophes in teams usually, maybe St. John's?)
    # St. Louis -> ST LOUIS
    norm = str(team_raw).upper()
    norm = re.sub(r"[.']", "", norm)
    norm = re.sub(r"\s+", " ", norm).strip() # Collapse spaces
    
    # 2. Lookup
    # Try exact match first
    if norm in NHL_TEAM_CODES:
        return NHL_TEAM_CODES[norm]
        
    # 3. Fuzzy / Common Variants (Manual mappings can be expanded)
    # Handle "MONTREAL" vs "MONTRÉAL" - normalize accents
    # Simple accent removal:
    norm_no_accent = norm.replace("É", "E")
    if norm_no_accent in NHL_TEAM_CODES:
        return NHL_TEAM_CODES[norm_no_accent]
        
    return None

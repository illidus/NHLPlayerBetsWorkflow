import hashlib
import json
import re
from datetime import datetime, timezone

def normalize_team_name(name):
    """Basic normalization: strip, upper, remove punctuation."""
    if not name:
        return None
    # Remove dots, dashes, apostrophes
    s = re.sub(r"[.']", "", str(name))
    return s.strip().upper()

def parse_game_date(iso_ts):
    """Extract YYYY-MM-DD from ISO timestamp if possible."""
    if not iso_ts:
        return None
    try:
        # Assume ISO format YYYY-MM-DDTHH:MM:SS...
        # Just taking first 10 chars is usually safe for ISO 8601
        return iso_ts[:10]
    except Exception:
        return None

def generate_row_id(row):
    """Generates a deterministic hash for the row."""
    # Key fields for uniqueness
    key_parts = [
        str(row.get('event_id_vendor')),
        str(row.get('book_id_vendor')),
        str(row.get('player_name_raw')),
        str(row.get('market_type')),
        str(row.get('line')),
        str(row.get('side'))
    ]
    raw = "|".join(key_parts)
    return hashlib.md5(raw.encode('utf-8')).hexdigest()

def normalize_fixture_row(raw_item, capture_ts):
    """
    Normalizes a single raw item from the fixture list.
    Expected raw_item structure depends on the fixture, but we'll assume a generic structure
    that might mimic a common API response (Event -> Markets -> Outcomes).
    
    Returns a list of normalized dictionaries.
    """
    normalized_rows = []
    
    # Basic validation (generic)
    if 'eventId' not in raw_item and 'id' not in raw_item:
        # Maybe it's a flat structure?
        pass

    event_id = raw_item.get('eventId') or raw_item.get('id')
    event_time = raw_item.get('startTime')
    
    # Team info (if available)
    # Generic keys: homeTeam, awayTeam, competitors list?
    home_raw = raw_item.get('homeTeam')
    away_raw = raw_item.get('awayTeam')
    
    # Normalization
    game_date = parse_game_date(event_time)
    home_norm = normalize_team_name(home_raw)
    away_norm = normalize_team_name(away_raw)
    
    match_key = None
    if game_date and home_norm and away_norm:
        # Construct deterministic key: YYYY-MM-DD|AWAY|HOME (or sorted?)
        # Let's stick to Away|Home convention if possible, or just raw Home|Away
        # Standard: Date|Away|Home is common.
        match_key = f"{game_date}|{away_norm}|{home_norm}"
    
    # We assume the fixture might contain a list of markets
    markets = raw_item.get('markets', [])
    
    for market in markets:
        market_key = market.get('key') or market.get('type')
        outcomes = market.get('outcomes', [])
        
        for outcome in outcomes:
            player_name = outcome.get('participant') or outcome.get('name')
            if not player_name:
                continue
                
            line = outcome.get('line', 0.5) # Default line if missing but market implies it?
            odds_dec = outcome.get('oddsDecimal')
            odds_us = outcome.get('oddsAmerican')
            
            # Side logic (Over/Under)
            label = outcome.get('label', '').lower()
            side = 'Over' if 'over' in label else 'Under' if 'under' in label else label.title()
            
            book_id = raw_item.get('bookId', 'unknown_book')
            
            row = {
                'source_vendor': 'phase11_fixture',
                'capture_ts_utc': capture_ts,
                'event_id_vendor': event_id,
                'event_start_ts_utc': event_time,
                'player_name_raw': player_name,
                'market_type': market_key,
                'line': float(line) if line is not None else None,
                'side': side,
                'book_id_vendor': book_id,
                'odds_american': int(odds_us) if odds_us else None,
                'odds_decimal': float(odds_dec) if odds_dec else None,
                'ingested_at_utc': datetime.now(timezone.utc).isoformat(),
                
                # New Join Keys
                'game_date': game_date,
                'home_team_raw': home_raw,
                'away_team_raw': away_raw,
                'home_team_norm': home_norm,
                'away_team_norm': away_norm,
                'match_key': match_key
            }
            
            # Add stable ID
            row['row_id'] = generate_row_id(row)
            
            normalized_rows.append(row)
            
    return normalized_rows

def normalize_batch(json_data, capture_ts=None):
    """
    Main entry point. Accepts a list of events/items.
    """
    if capture_ts is None:
        capture_ts = datetime.now(timezone.utc).isoformat()
        
    all_rows = []
    
    # If root is dict, maybe it has a 'data' key?
    items = json_data
    if isinstance(json_data, dict):
        items = json_data.get('data', [])
        
    if not isinstance(items, list):
        print("Warning: Input is not a list or does not contain 'data' list.")
        return []
        
    for item in items:
        all_rows.extend(normalize_fixture_row(item, capture_ts))
        
    return all_rows

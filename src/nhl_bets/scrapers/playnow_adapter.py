import re
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from nhl_bets.common.storage import save_raw_payload

logger = logging.getLogger(__name__)

class PlayNowAdapter:
    """
    Adapts PlayNow API responses into the unified fact_prop_odds schema.
    """
    
    def __init__(self):
        pass

    def slugify(self, text: str) -> str:
        return text.lower().replace(' at ', '-at-').replace(' ', '-').replace("'", "").replace(".", "")

    def normalize_market_type(self, market_name: str) -> Optional[str]:
        if 'Total Shots on Goal' in market_name or 'Total Shots On Goal' in market_name: 
            return 'SOG'
        elif 'Total Points' in market_name: 
            return 'POINTS'
        elif 'Total Assists' in market_name: 
            return 'ASSISTS'
        elif 'Total Blocks' in market_name or 'Total Blocked Shots' in market_name: 
            return 'BLOCKS'
        elif market_name == 'Player 1+ Goals': 
            return 'GOALS'
        return None

    def parse_event_details(self, data: Dict[str, Any], raw_path: str, raw_hash: str, capture_ts: datetime) -> List[Dict[str, Any]]:
        """
        Parses the detailed 'events-by-ids' response.
        """
        records = []
        events = data.get('data', {}).get('events', [])
        
        for event in events:
            event_id = str(event['id'])
            event_name = event['name']
            start_time = event['startTime']
            
            # PlayNow event_name format: "Away Team @ Home Team"
            home_team = None
            away_team = None
            if ' @ ' in event_name:
                away_team_raw, home_team_raw = event_name.split(' @ ')
                # Note: These are full names, we'll normalize them later or keep raw
                home_team = home_team_raw.strip()
                away_team = away_team_raw.strip()
            
            for market in event.get('markets', []):
                market_id = str(market['id'])
                market_name = market['name']
                market_type = self.normalize_market_type(market_name)
                
                if not market_type:
                    continue
                
                handicap = market.get('handicapValue')
                
                # Determine player name from market_name (e.g., "Connor McDavid Total Points")
                player_name = None
                if ' Total ' in market_name:
                    player_name = market_name.split(' Total ')[0].strip()
                elif market_name == 'Player 1+ Goals':
                    # Goal outcomes have the player name in the outcome name
                    pass
                else:
                    # Try regex
                    player_match = re.search(r'^(.*?)\s+Total\s+', market_name, re.IGNORECASE)
                    if player_match:
                        player_name = player_match.group(1).strip()

                for outcome in market.get('outcomes', []):
                    outcome_id = str(outcome['id'])
                    outcome_name = outcome['name']
                    
                    # For Goals, the player is the outcome_name
                    current_player = player_name
                    if market_type == 'GOALS' and market_name == 'Player 1+ Goals':
                        current_player = outcome_name
                        side = "OVER"
                        line = 0.5
                    elif outcome_name.lower() in ['over', 'under']:
                        side = outcome_name.upper()
                        line = handicap
                    else:
                        # Handle Maltese "3+" format
                        maltese_match = re.match(r"(\d+)\+", outcome_name)
                        if maltese_match:
                            k = int(maltese_match.group(1))
                            line = k - 0.5
                            side = "OVER"
                        else:
                            continue # Unknown format
                    
                    if not current_player or line is None:
                        continue
                        
                    price_decimal = None
                    price_american = None
                    if outcome.get('prices'):
                        price = outcome['prices'][0]
                        price_decimal = price.get('decimal')
                        # PlayNow doesn't always provide American odds in the same field, 
                        # but we can calculate it from decimal.
                        if price_decimal:
                            if price_decimal >= 2.0:
                                price_american = (price_decimal - 1) * 100
                            elif price_decimal > 1.0:
                                price_american = -100 / (price_decimal - 1)
                    
                    records.append({
                        "source_vendor": "PLAYNOW",
                        "capture_ts_utc": capture_ts,
                        "event_id_vendor": event_id,
                        "event_name_raw": event_name,
                        "event_start_ts_utc": start_time,
                        "home_team": home_team,
                        "away_team": away_team,
                        "player_id_vendor": None, # PlayNow doesn't give a stable player ID in this payload
                        "player_name_raw": current_player,
                        "market_type": market_type,
                        "line": float(line),
                        "side": side,
                        "book_id_vendor": "PLAYNOW",
                        "book_name_raw": "PlayNow",
                        "odds_american": int(round(price_american)) if price_american is not None else None,
                        "odds_decimal": float(price_decimal) if price_decimal is not None else None,
                        "is_live": False, # Assume pre-game unless otherwise indicated
                        "raw_payload_path": raw_path,
                        "raw_payload_hash": raw_hash
                    })
                    
        return records

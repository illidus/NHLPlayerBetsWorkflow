import requests
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential
from nhl_bets.common.storage import save_raw_payload

logger = logging.getLogger(__name__)

class UnabatedClient:
    URL = "https://content.unabated.com/markets/v2/league/6/propodds.json"
    
    # Mapping betTypeId to canonical market_type
    # Values inferred from structure and odds.
    BET_TYPE_MAP = {
        70: "POINTS",
        73: "ASSISTS",
        86: "SOG",
        88: "BLOCKS",
        129: "GOALS"
    }
    
    def __init__(self, timeout: int = 30):
        self.timeout = timeout

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_snapshot(self) -> Dict[str, Any]:
        """Fetches the latest prop odds snapshot from Unabated."""
        logger.info(f"Fetching Unabated snapshot from {self.URL}...")
        response = requests.get(self.URL, timeout=(10, self.timeout))
        response.raise_for_status()
        return response.json()

    def parse_snapshot(self, data: Dict[str, Any], raw_path: str, raw_hash: str, capture_ts: datetime) -> List[Dict[str, Any]]:
        """Parses the Unabated JSON into normalized records."""
        records = []
        
        people = data.get("people", {})
        market_sources = {str(ms["id"]): ms["name"] for ms in data.get("marketSources", [])}
        odds_dict = data.get("odds", {})
        teams_map = data.get("teams", {})
        
        # We focus on the pregame props (pt1)
        pregame_props = odds_dict.get("lg6:pt1:pregame", [])
        
        for prop in pregame_props:
            if prop.get("betSubType") is not None:
                continue # Skip Milestones/Alt lines for now
                
            bet_type_id = prop.get("betTypeId")
            market_type = self.BET_TYPE_MAP.get(bet_type_id)
            
            if not market_type:
                continue # Skip unknown/unsupported markets
                
            person_id = str(prop.get("personId"))
            person_data = people.get(person_id, {})
            player_name = f"{person_data.get('firstName', '')} {person_data.get('lastName', '')}".strip()
            
            event_id = str(prop.get("eventId"))
            event_start = prop.get("eventStart")
            event_name = prop.get("eventName")
            
            # Extract teams from eventTeams
            # Unabated eventTeams: {"1": {"id": 726, ...}, "0": {"id": 753, ...}}
            # sideId 1 is usually Home, sideId 0 is usually Away
            event_teams = prop.get("eventTeams", {})
            home_team_id = str(event_teams.get("1", {}).get("id", ""))
            away_team_id = str(event_teams.get("0", {}).get("id", ""))
            
            home_team = teams_map.get(home_team_id, {}).get("abbreviation")
            away_team = teams_map.get(away_team_id, {}).get("abbreviation")
            
            sides = prop.get("sides", {})
            if market_type == "GOALS" and len(sides) < 2:
                continue # Skip Anytime Goal Scorer (usually only 'Yes' side)
                
            for side_key, book_data in sides.items():
                # side_key usually looks like 'si1:pid45587' or 'si0:pid45587'
                # Unabated convention: si1 is typically OVER, si0 is typically UNDER for props
                side = "OVER" if side_key.startswith("si1") else "UNDER"
                
                for ms_key, price_data in book_data.items():
                    # ms_key looks like 'ms73'
                    book_id = ms_key.replace("ms", "")
                    book_name = market_sources.get(book_id, f"Unknown Book {book_id}")
                    
                    line = price_data.get("points")
                    price_american = price_data.get("americanPrice")
                    
                    if line is None or price_american is None:
                        continue
                        
                    # Calculate decimal odds
                    odds_decimal = None
                    if price_american > 0:
                        odds_decimal = (price_american / 100) + 1
                    elif price_american < 0:
                        odds_decimal = (100 / abs(price_american)) + 1
                    
                    records.append({
                        "source_vendor": "UNABATED",
                        "capture_ts_utc": capture_ts,
                        "event_id_vendor": event_id,
                        "event_name_raw": event_name,
                        "event_start_ts_utc": event_start,
                        "home_team": home_team,
                        "away_team": away_team,
                        "player_id_vendor": person_id,
                        "player_name_raw": player_name,
                        "market_type": market_type,
                        "line": float(line),
                        "side": side,
                        "book_id_vendor": book_id,
                        "book_name_raw": book_name,
                        "odds_american": int(price_american),
                        "odds_decimal": odds_decimal,
                        "is_live": prop.get("live", False),
                        "raw_payload_path": raw_path,
                        "raw_payload_hash": raw_hash
                    })
                    
        return records

    def run_ingestion(self) -> List[Dict[str, Any]]:
        """Fetch, save, and parse Unabated odds."""
        snapshot = self.fetch_snapshot()
        rel_path, sha_hash, capture_ts = save_raw_payload("UNABATED", snapshot, "json")
        
        logger.info(f"Saved Unabated snapshot to {rel_path} (hash: {sha_hash})")
        
        normalized_records = self.parse_snapshot(snapshot, rel_path, sha_hash, capture_ts)
        logger.info(f"Parsed {len(normalized_records)} records from Unabated snapshot.")
        
        return normalized_records

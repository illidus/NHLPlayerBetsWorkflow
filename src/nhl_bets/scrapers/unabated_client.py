import requests
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from nhl_bets.common.storage import save_raw_payload
from nhl_bets.common.vendor_utils import (
    MAX_RETRIES,
    VendorRequestError,
    get_timeout_tuple,
    should_force_vendor_failure,
)

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

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
        retry=retry_if_exception_type((VendorRequestError, requests.RequestException)),
    )
    def fetch_snapshot(self) -> Dict[str, Any]:
        """Fetches the latest prop odds snapshot from Unabated."""
        # Add cache-busting timestamp to the URL
        timestamp = int(datetime.now(timezone.utc).timestamp())
        url = f"{self.URL}?t={timestamp}"
        
        logger.info(f"Fetching Unabated snapshot from {url}...")
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Accept": "application/json"
        }
        
        if should_force_vendor_failure("UNABATED"):
            raise VendorRequestError("Forced Unabated failure via env var.")
        try:
            response = requests.get(url, headers=headers, timeout=get_timeout_tuple(self.timeout))
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            raise VendorRequestError(f"Unabated request failed: {exc}") from exc

    def parse_snapshot(self, data: Dict[str, Any], raw_path: str, raw_hash: str, capture_ts: datetime) -> Dict[str, Any]:
        """Parses the Unabated JSON into normalized records and metadata."""
        records = []
        events_map = {}
        players_map = {}
        
        people = data.get("people", {})
        market_sources = {str(ms["id"]): ms["name"] for ms in data.get("marketSources", [])}
        # Include statusId 1 (Active) and 3 (which includes Bet365/Heritage/Pinnacle-3838)
        active_market_sources = {str(ms["id"]) for ms in data.get("marketSources", []) if ms.get("statusId") in [1, 3]}
        odds_dict = data.get("odds", {})
        teams_map = data.get("teams", {})
        
        # We focus on the pregame props (pt1)
        pregame_props = odds_dict.get("lg6:pt1:pregame", [])
        
        for prop in pregame_props:
            # We focus on the pregame props (pt1)
            # Strictly skip any non-standard sub-types (Milestones, Alt lines etc)
            if prop.get("betSubType") is not None:
                continue 
                
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
            
            # Metadata collection
            if event_id not in events_map:
                events_map[event_id] = {
                    "vendor_event_id": event_id,
                    "event_start_time_utc": event_start,
                    "home_team": home_team,
                    "away_team": away_team,
                    "league": "NHL",
                    "capture_ts_utc": capture_ts
                }
            
            if person_id not in players_map:
                players_map[person_id] = {
                    "vendor_person_id": person_id,
                    "player_name": player_name,
                    "team_abbr": None, # Unabated doesn't always have a single stable team for a person in this view
                    "capture_ts_utc": capture_ts
                }

            sides = prop.get("sides", {})
            if market_type == "GOALS" and len(sides) < 2:
                continue # Skip Anytime Goal Scorer (usually only 'Yes' side)
                
            # Local collection for this prop to pick latest marketLineId per book/side/line
            prop_best_records = {}

            for side_key, book_data in sides.items():
                # side_key usually looks like 'si1:pid45587' or 'si0:pid45587'
                # VERIFIED BY SCREENSHOT: 
                # si1 is typically UNDER, si0 is typically OVER for prop markets
                side = "UNDER" if side_key.startswith("si1") else "OVER"
                
                for ms_key, price_data in book_data.items():
                    book_id = ms_key.replace("ms", "")
                    
                    # Skip inactive books
                    if book_id not in active_market_sources:
                        continue

                    # Skip blurred lines (Unabated requires subscription for some books)
                    if price_data.get("isBlurred", False):
                        continue

                    book_name = market_sources.get(book_id, f"Unknown Book {book_id}")
                    
                    line = price_data.get("points")
                    price_american = price_data.get("americanPrice")
                    market_line_id = price_data.get("marketLineId", 0)
                    
                    if line is None or price_american is None:
                        continue
                        
                    def add_record(l, p, sid, outcome_key, ml_id):
                        l_float = float(l)
                        # Key by book, side, and line to find the best version
                        best_key = (book_id, sid, l_float)
                        
                        if best_key in prop_best_records:
                            if ml_id <= prop_best_records[best_key]["market_line_id"]:
                                return # Keep existing better record
                        
                        # Calculate decimal odds
                        o_dec = None
                        if p > 0: o_dec = (p / 100) + 1
                        elif p < 0: o_dec = (100 / abs(p)) + 1
                        
                        prop_best_records[best_key] = {
                            "market_line_id": ml_id,
                            "data": {
                                "source_vendor": "UNABATED",
                                "capture_ts_utc": capture_ts,
                                "event_id_vendor": event_id,
                                "event_id_vendor_raw": event_id,
                                "vendor_event_id": event_id,
                                "event_name_raw": event_name,
                                "event_start_time_utc": event_start,
                                "home_team": home_team,
                                "away_team": away_team,
                                "player_id_vendor": person_id,
                                "vendor_person_id": person_id,
                                "player_name_raw": player_name,
                                "market_type": market_type,
                                "line": l_float,
                                "side": sid,
                                "book_id_vendor": book_id,
                                "book_name_raw": book_name,
                                "odds_american": int(p),
                                "odds_decimal": o_dec,
                                "odds_quoted_raw": str(p),
                                "odds_quoted_format": "american",
                                "odds_american_derived": False,
                                "odds_decimal_derived": True,
                                "is_live": prop.get("live", False),
                                "raw_payload_path": raw_path,
                                "raw_payload_hash": raw_hash,
                                "vendor_market_source_id": book_id,
                                "vendor_bet_type_id": bet_type_id,
                                "vendor_outcome_key": outcome_key,
                                "vendor_price_raw": str(p),
                                "vendor_price_format": "american"
                            }
                        }

                    add_record(line, price_american, side, side_key, market_line_id)
                    
                    # Check for alternate lines
                    alt_lines = price_data.get("alternateLines")
                    if alt_lines and isinstance(alt_lines, list):
                        for alt in alt_lines:
                            if not alt: continue
                            alt_line = alt.get("points")
                            alt_price = alt.get("americanPrice")
                            alt_ml_id = alt.get("marketLineId", 0) # usually 0 for alts
                            if alt_line is not None and alt_price is not None:
                                # We use a lower priority for alts if they clash with main lines
                                # but usually alts don't have IDs anyway.
                                add_record(alt_line, alt_price, side, f"{side_key}_alt_{alt_line}", alt_ml_id)

            # Extract the actual records from the deduplicated map
            for best_rec in prop_best_records.values():
                records.append(best_rec["data"])
                    
        return {
            "odds": records,
            "events": list(events_map.values()),
            "players": list(players_map.values())
        }

    def run_ingestion(self) -> List[Dict[str, Any]]:
        """Fetch, save, and parse Unabated odds."""
        snapshot = self.fetch_snapshot()
        rel_path, sha_hash, capture_ts = save_raw_payload("UNABATED", snapshot, "json")
        
        logger.info(f"Saved Unabated snapshot to {rel_path} (hash: {sha_hash})")
        
        parsed_data = self.parse_snapshot(snapshot, rel_path, sha_hash, capture_ts)
        normalized_records = parsed_data["odds"]
        logger.info(f"Parsed {len(normalized_records)} records from Unabated snapshot.")
        
        return normalized_records

import json
import logging
import requests
import pandas as pd
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from src.nhl_bets.ingestion.storage import RawStorage

logger = logging.getLogger(__name__)

class UnabatedIngestor:
    """
    Phase 11: Unabated Ingestion Implementation.
    Fetches snapshot, persists raw, and normalizes to fact_prop_odds schema.
    """
    
    URL = "https://content.unabated.com/markets/v2/league/6/propodds.json"
    
    # Mapping betTypeId to canonical market_type
    BET_TYPE_MAP = {
        70: "POINTS",
        73: "ASSISTS",
        86: "SOG",
        88: "BLOCKS",
        129: "GOALS"
    }

    def __init__(self, timeout: int = 30):
        self.timeout = timeout

    def fetch_snapshot(self) -> Dict[str, Any]:
        """Fetches the latest prop odds snapshot from Unabated."""
        logger.info(f"Fetching Unabated snapshot from {self.URL}...")
        try:
            response = requests.get(self.URL, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to fetch Unabated snapshot: {e}")
            raise

    def parse_snapshot(self, data: Dict[str, Any], raw_path: str, raw_hash: str, capture_ts: datetime) -> pd.DataFrame:
        """Parses the Unabated JSON into a normalized DataFrame."""
        records = []
        
        people = data.get("people", {})
        market_sources = {str(ms["id"]): ms["name"] for ms in data.get("marketSources", [])}
        odds_dict = data.get("odds", {})
        
        # We focus on the pregame props (pt1)
        pregame_props = odds_dict.get("lg6:pt1:pregame", [])
        
        for prop in pregame_props:
            bet_type_id = prop.get("betTypeId")
            market_type = self.BET_TYPE_MAP.get(bet_type_id)
            
            if not market_type:
                continue 
                
            person_id = str(prop.get("personId"))
            person_data = people.get(person_id, {})
            player_name = f"{person_data.get('firstName', '')} {person_data.get('lastName', '')}".strip()
            
            event_id = str(prop.get("eventId"))
            # eventStart is often ISO string or None? Unabated usually ISO.
            event_start_raw = prop.get("eventStart")
            event_start_ts = None
            if event_start_raw:
                try:
                    event_start_ts = pd.to_datetime(event_start_raw).tz_convert("UTC")
                except:
                    pass
            
            sides = prop.get("sides", {})
            if market_type == "GOALS" and len(sides) < 2:
                # Filter out ATGS if strict logic requires lines (Phase 11 focuses on O/U lines)
                # But keep if it has useful O/U structure.
                pass

            for side_key, book_data in sides.items():
                # side_key usually looks like 'si1:pid45587'
                # Unabated convention: si1=OVER, si0=UNDER usually.
                # Logic: si1 is typically associated with the 'Over' outcome for prop markets.
                # Verify logic: TheLines scraper uses similar map.
                
                side = "OVER" if "si1" in side_key else "UNDER"
                if "si0" in side_key: side = "UNDER" # Explicit check
                
                for ms_key, price_data in book_data.items():
                    book_id = ms_key.replace("ms", "")
                    book_name = market_sources.get(book_id, f"Book {book_id}")
                    
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
                        "event_start_ts_utc": event_start_ts,
                        "player_id_vendor": person_id,
                        "player_name_raw": player_name,
                        "market_type": market_type,
                        "line": float(line),
                        "side": side,
                        "book_id_vendor": book_id,
                        "book_name_raw": book_name,
                        "odds_american": int(price_american),
                        "odds_decimal": float(odds_decimal) if odds_decimal else None,
                        "is_live": prop.get("live", False),
                        "raw_payload_path": raw_path,
                        "raw_payload_hash": raw_hash
                    })
        
        df = pd.DataFrame(records)
        return df

    def run(self, save_only: bool = False) -> pd.DataFrame:
        """Full execution flow."""
        snapshot = self.fetch_snapshot()
        
        path, raw_hash, ts = RawStorage.save_payload(
            vendor="UNABATED", 
            payload=snapshot, 
            file_suffix="propodds.json"
        )
        logger.info(f"Unabated snapshot saved to {path}")
        
        if save_only:
            return pd.DataFrame()
            
        df = self.parse_snapshot(snapshot, path, raw_hash, ts)
        logger.info(f"Unabated ingestion produced {len(df)} rows.")
        return df

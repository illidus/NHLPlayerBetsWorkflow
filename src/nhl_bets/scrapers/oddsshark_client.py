import requests
import logging
from bs4 import BeautifulSoup
from datetime import datetime, timezone, date
from typing import List, Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from nhl_bets.common.storage import save_raw_payload
from nhl_bets.common.vendor_utils import (
    MAX_RETRIES,
    VendorRequestError,
    get_timeout_tuple,
    should_force_vendor_failure,
)
from nhl_bets.analysis.normalize import TEAM_NAME_TO_ABBR


def _normalize_team_abbr(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = value.strip()
    if len(cleaned) == 3 and cleaned.isalpha():
        return cleaned.upper()
    mapped = TEAM_NAME_TO_ABBR.get(cleaned)
    if mapped:
        return mapped
    return cleaned.upper() if cleaned.isalpha() and len(cleaned) <= 4 else None


def build_synthetic_event_id(game_date: date, away_team: Optional[str], home_team: Optional[str]) -> Optional[str]:
    away = _normalize_team_abbr(away_team)
    home = _normalize_team_abbr(home_team)
    if not game_date or not away or not home:
        return None
    return f"ODDSSHARK_{game_date:%Y%m%d}_{away}_{home}"

logger = logging.getLogger(__name__)

class OddsSharkClient:
    URL = "https://www.oddsshark.com/nhl/odds/player-props"
    
    def __init__(self, timeout: int = 30):
        self.timeout = timeout

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
        retry=retry_if_exception_type((VendorRequestError, requests.RequestException)),
    )
    def fetch_snapshot(self) -> str:
        """Fetches the latest prop odds HTML from OddsShark."""
        logger.info(f"Fetching OddsShark HTML from {self.URL}...")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        if should_force_vendor_failure("ODDSSHARK"):
            raise VendorRequestError("Forced OddsShark failure via env var.")
        try:
            response = requests.get(self.URL, headers=headers, timeout=get_timeout_tuple(self.timeout))
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            raise VendorRequestError(f"OddsShark request failed: {exc}") from exc

    def parse_snapshot(self, html: str, raw_path: str, raw_hash: str, capture_ts: datetime) -> List[Dict[str, Any]]:
        """Parses the OddsShark HTML into normalized records."""
        records = []
        soup = BeautifulSoup(html, 'html.parser')
        
        # 1. Identify markets from tabs
        # The tab buttons usually define what the containers below them are.
        tab_buttons = soup.select(".tab-group.foil .button--tab-primary")
        market_types = []
        for btn in tab_buttons:
            text = btn.get_text(strip=True).upper()
            if "POWER PLAY" in text: market_types.append(None) # Skip PPP for now
            elif "GOALS" == text: market_types.append("GOALS") # Exact match or careful contains
            elif "ASSISTS" == text: market_types.append("ASSISTS")
            elif "POINTS" == text: market_types.append("POINTS")
            elif "SHOTS ON GOAL" in text: market_types.append("SOG")
            elif "BLOCKS" in text or "BLOCKED SHOTS" in text: market_types.append("BLOCKS")
            elif "GOALS" in text: market_types.append("GOALS")
            elif "ASSISTS" in text: market_types.append("ASSISTS")
            elif "POINTS" in text: market_types.append("POINTS")
            else: market_types.append(None)
            
        # 2. Iterate over containers (each container corresponds to a tab)
        containers = soup.select(".player-props--container.tab")
        
        # Sportsbooks are in the header of each container
        # Note: They might be different per container but usually the same.
        
        for idx, container in enumerate(containers):
            if idx >= len(market_types) or market_types[idx] is None:
                continue # Skip unsupported markets
                
            market_type = market_types[idx]
            
            # Find book names in header
            header_items = container.select(".player-props--header .player-props--item img")
            book_names = [img.get('alt', '').strip() for img in header_items]
            # Some headers might not have images or be different. 
            # Looking at the sample, books start from the 3rd column (index 2).
            # Index 0: Player, Index 1: Best Odds, Index 2+: Specific Books
            
            rows = container.select(".player-props--row")
            
            # Find game info preceding the rows
            # Game info looks like: <div class="props-game-info"> <span class="props-teams">ANA @ WSH -</span> ...
            
            for row in rows:
                event_id_raw = row.get('data-event', 'unknown')
                
                # Find the game info by looking at the previous sibling that is not a row
                game_info = row.find_previous(class_="props-game-info")
                event_name = "unknown"
                home_team = None
                away_team = None
                if game_info:
                    teams_el = game_info.select_one(".props-teams")
                    if teams_el:
                        event_name = teams_el.get_text(strip=True).replace(" -", "")
                        if " @ " in event_name:
                            away_team, home_team = event_name.split(" @ ")

                event_date = capture_ts.date()
                event_id = build_synthetic_event_id(event_date, away_team, home_team) or event_id_raw
                
                player_name_el = row.select_one(".player-name")
                if not player_name_el:
                    continue
                player_name = player_name_el.get_text(strip=True)
                
                # Each book-row corresponds to a column in the header
                book_cells = row.select(".book-row")
                # Skip the "Best Odds" cell (usually the first book-row)
                
                for cell_idx, cell in enumerate(book_cells):
                    if cell_idx == 0: continue # Skip Best Odds column
                    
                    # Header index for book name would be cell_idx - 1 if we skip Player/Best Odds
                    # Wait, book_cells[0] is Best Odds. book_cells[1] is the first book.
                    # header_items[0] is usually the first book.
                    header_idx = cell_idx - 1
                    if header_idx >= len(book_names):
                        book_name = f"Unknown Book {header_idx}"
                    else:
                        book_name = book_names[header_idx]
                    
                    # Each cell might have Over and Under
                    odds_divs = cell.select(".player-props-odds > div")
                    for div in odds_divs:
                        info_el = div.select_one(".odds-info")
                        detail_el = div.select_one(".odds-detail")
                        
                        if not info_el or not detail_el:
                            continue
                            
                        info_text = info_el.get_text(strip=True) # e.g. "O 0.5" or "U 0.5"
                        detail_text = detail_el.get_text(strip=True) # e.g. "+130"
                        
                        parts = info_text.split()
                        if len(parts) < 2:
                            continue
                            
                        side_raw = parts[0]
                        line_raw = parts[1]
                        
                        side = "OVER" if side_raw.upper() == "O" else "UNDER" if side_raw.upper() == "U" else "UNKNOWN"
                        
                        try:
                            line = float(line_raw)
                            price_american = int(detail_text.replace("+", ""))
                            
                            # Calculate decimal odds
                            odds_decimal = None
                            if price_american > 0:
                                odds_decimal = (price_american / 100) + 1
                            elif price_american < 0:
                                odds_decimal = (100 / abs(price_american)) + 1
                                
                            records.append({
                                "source_vendor": "ODDSSHARK",
                                "capture_ts_utc": capture_ts,
                                "event_id_vendor": event_id,
                                "event_id_vendor_raw": event_id_raw,
                                "event_name_raw": event_name,
                                "event_start_ts_utc": None, # HTML doesn't easily give UTC timestamp per row
                                "home_team": home_team,
                                "away_team": away_team,
                                "player_id_vendor": None,
                                "player_name_raw": player_name,
                                "market_type": market_type,
                                "line": line,
                                "side": side,
                                "book_id_vendor": book_name.lower().replace(" ", "_"),
                                "book_name_raw": book_name,
                                "odds_american": price_american,
                                "odds_decimal": odds_decimal,
                                "odds_quoted_raw": detail_text,
                                "odds_quoted_format": "american",
                                "odds_american_derived": False,
                                "odds_decimal_derived": True,
                                "is_live": False,
                                "raw_payload_path": raw_path,
                                "raw_payload_hash": raw_hash
                            })
                        except (ValueError, TypeError):
                            continue
                            
        return records

    def run_ingestion(self) -> List[Dict[str, Any]]:
        """Fetch, save, and parse OddsShark odds."""
        html = self.fetch_snapshot()
        rel_path, sha_hash, capture_ts = save_raw_payload("ODDSSHARK", html, "html")
        
        logger.info(f"Saved OddsShark snapshot to {rel_path} (hash: {sha_hash})")
        
        normalized_records = self.parse_snapshot(html, rel_path, sha_hash, capture_ts)
        logger.info(f"Parsed {len(normalized_records)} records from OddsShark snapshot.")
        
        return normalized_records

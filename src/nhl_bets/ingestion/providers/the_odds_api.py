import os
import json
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
from src.nhl_bets.ingestion.providers.base import BaseOddsProvider

class TheOddsApiProvider(BaseOddsProvider):
    """
    Concrete implementation for The-Odds-Api (https://the-odds-api.com).
    Supports Sportsbook (event-based) and DFS (regional) pathways.
    """
    
    BASE_URL = "https://api.the-odds-api.com/v4/sports"
    
    MARKET_MAP = {
        "player_points": "POINTS",
        "player_assists": "ASSISTS",
        "player_goals": "GOALS",
        "player_shots_on_goal": "SOG",
        "player_blocked_shots": "BLOCKS",
        "player_power_play_points": "PPP"
    }

    def __init__(self, api_key: str = None, mock_mode: bool = False, mode: str = "sportsbook", regions: str = "us", limit_events: int = None):
        super().__init__("THE_ODDS_API")
        self.api_key = api_key or os.getenv("THE_ODDS_API_KEY")
        self.mock_mode = mock_mode
        self.mode = mode # sportsbook or dfs
        self.regions = regions
        self.limit_events = limit_events
        self.diagnostics = []

    def get_quota_remaining(self, headers: Dict) -> Optional[int]:
        """
        Extracts 'x-requests-remaining' from headers.
        """
        try:
            val = headers.get("x-requests-remaining")
            return int(val) if val is not None else None
        except:
            return None

    def fetch_data(self, start_date: datetime, end_date: datetime, league: str) -> List[Tuple[Dict, Any]]:
        """
        Fetches odds using the selected mode (Sportsbook vs DFS).
        """
        if self.mock_mode:
            return self._fetch_mock_data(start_date, league)
        
        if not self.api_key:
            raise ValueError("API Key required for TheOddsApiProvider (set THE_ODDS_API_KEY)")

        sport = "icehockey_nhl"
        markets = ",".join(self.MARKET_MAP.keys())
        
        if self.mode == "dfs" or self.regions == "us_dfs":
            return self._fetch_dfs_pathway(sport, markets)
        else:
            return self._fetch_sportsbook_pathway(sport, markets)

    def _fetch_dfs_pathway(self, sport: str, markets: str) -> List[Tuple[Dict, Any]]:
        """
        B) DFS Props Path: Regional bulk fetch.
        """
        url = f"{self.BASE_URL}/{sport}/odds"
        params = {
            "apiKey": self.api_key,
            "regions": self.regions,
            "markets": markets,
            "oddsFormat": "decimal",
        }
        
        print(f"[DFS] Fetching from {url}...")
        resp = requests.get(url, params=params)
        self._log_and_diagnose(url, params, resp)
        
        if resp.status_code != 200:
            return []

        payload = resp.json()
        requested_asof = datetime.now(timezone.utc)
        request_uid = self.compute_request_uid(self.provider_name, url, params, requested_asof)

        meta = {
            "file_suffix": f"odds_dfs_{requested_asof.strftime('%Y%m%d_%H%M%S')}.json",
            "requested_asof": requested_asof,
            "request_uid": request_uid,
            "is_dfs": True
        }
        return [(meta, payload)]

    def _fetch_sportsbook_pathway(self, sport: str, markets: str) -> List[Tuple[Dict, Any]]:
        """
        A) Sportsbook Props Path: Event-based fetch.
        """
        # 1. Fetch Events
        events_url = f"{self.BASE_URL}/{sport}/events"
        events_params = {"apiKey": self.api_key}
        print(f"[Sportsbook] Fetching events from {events_url}...")
        events_resp = requests.get(events_url, params=events_params)
        self._log_and_diagnose(events_url, events_params, events_resp)
        
        if events_resp.status_code != 200:
            return []
            
        events = events_resp.json()
        if self.limit_events:
            events = events[:self.limit_events]
            
        batches = []
        requested_asof = datetime.now(timezone.utc)

        # 2. Fetch Odds per Event
        for event in events:
            event_id = event.get('id')
            url = f"{self.BASE_URL}/{sport}/events/{event_id}/odds"
            params = {
                "apiKey": self.api_key,
                "regions": self.regions,
                "markets": markets,
                "oddsFormat": "decimal",
            }
            
            print(f"[Sportsbook] Fetching event {event_id} from {url}...")
            resp = requests.get(url, params=params)
            self._log_and_diagnose(url, params, resp)
            
            if resp.status_code == 422:
                print(f"Warning: 422 for event {event_id}. Retrying markets one-by-one...")
                self._diagnose_markets_one_by_one(url, params)
                continue
                
            if resp.status_code != 200:
                continue

            payload = resp.json()
            request_uid = self.compute_request_uid(self.provider_name, url, params, requested_asof)
            
            meta = {
                "file_suffix": f"odds_event_{event_id}_{requested_asof.strftime('%Y%m%d_%H%M%S')}.json",
                "requested_asof": requested_asof,
                "request_uid": request_uid,
                "is_dfs": False
            }
            batches.append((meta, payload))
            
        return batches

    def _log_and_diagnose(self, url: str, params: Dict, resp: requests.Response):
        safe_params = self._redact_params(params)
            
        self.log_request(url, safe_params, resp.status_code, dict(resp.headers), cost_est=0)
        
        if resp.status_code != 200:
            diag_entry = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "url": url,
                "params": safe_params,
                "status_code": resp.status_code,
                "body": resp.text
            }
            self.diagnostics.append(diag_entry)
            
            # Print body as requested
            try:
                print(f"ERROR {resp.status_code}: {json.dumps(resp.json(), indent=2)}")
            except:
                print(f"ERROR {resp.status_code}: {resp.text}")

    def _diagnose_markets_one_by_one(self, base_url: str, base_params: Dict):
        markets = base_params.get("markets", "").split(",")
        for m in markets:
            params = base_params.copy()
            params["markets"] = m
            resp = requests.get(base_url, params=params)
            self._log_and_diagnose(base_url, params, resp)
            if resp.status_code == 200:
                print(f"  [DIAG] Market '{m}' is SUPPORTED.")
            else:
                print(f"  [DIAG] Market '{m}' FAILED with {resp.status_code}.")

    def _redact_params(self, params: Dict) -> Dict:
        safe_params = params.copy()
        if "apiKey" in safe_params:
            safe_params["apiKey"] = "REDACTED"
        return safe_params

    def _api_key_fingerprint(self, api_key: Optional[str]) -> str:
        if not api_key:
            return "MISSING"
        return f"{api_key[:4]}...{api_key[-4:]}"

    def _ensure_api_key_in_params(self, params: Dict):
        if "apiKey" not in params or not params.get("apiKey"):
            raise ValueError("Diagnostics requires apiKey in request params; missing or empty apiKey.")

    def write_diagnostics(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.diagnostics, f, indent=2)
        abs_path = os.path.abspath(path)
        print(f"Diagnostics written to: {abs_path}")
        if not os.path.exists(path):
            raise RuntimeError(f"Diagnostics expected at {abs_path} but file was not written.")

    def diagnose_vendor(self, sport: str = "icehockey_nhl"):
        """
        C) Diagnostics improvements
        """
        print(f"--- Running Vendor Diagnostics for {sport} ---")

        api_key_present = bool(self.api_key)
        api_key_len = len(self.api_key) if self.api_key else 0
        api_key_fingerprint = self._api_key_fingerprint(self.api_key)
        print(f"api_key_present={api_key_present} api_key_len={api_key_len} api_key_fingerprint={api_key_fingerprint}")

        run_ts = datetime.now(timezone.utc)
        run_ts_str = run_ts.strftime("%Y%m%d_%H%M%S")
        run_dir = Path("outputs/phase12_odds_api") / run_ts_str
        run_dir.mkdir(parents=True, exist_ok=True)
        latest_path = Path("outputs/phase12_odds_api/LATEST.txt")
        latest_path.parent.mkdir(parents=True, exist_ok=True)
        latest_path.write_text(run_ts_str)

        diag_path = run_dir / "vendor_diagnostics.json"
        print(f"Diagnostics will be written to: {diag_path.resolve()}")

        self.diagnostics = [
            {
                "type": "meta",
                "run_ts": run_ts.isoformat(),
                "provider": self.provider_name,
                "api_key_present": api_key_present,
                "api_key_len": api_key_len,
                "api_key_fingerprint": api_key_fingerprint,
            }
        ]

        # Single-request diagnostics: /sports only
        url = "https://api.the-odds-api.com/v4/sports"
        params = {"apiKey": self.api_key}
        self._ensure_api_key_in_params(params)
        print("Testing /sports...")
        resp = requests.get(url, params=params)

        safe_params = self._redact_params(params)
        diag_entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "url": url,
            "params": safe_params,
            "status_code": resp.status_code,
            "headers": dict(resp.headers),
        }
        if resp.status_code != 200:
            diag_entry["body"] = resp.text
        self.diagnostics.append(diag_entry)
        self.log_request(url, safe_params, resp.status_code, dict(resp.headers), cost_est=0)

        invalid_key = False
        try:
            body_json = resp.json()
            if isinstance(body_json, dict) and body_json.get("error_code") == "INVALID_KEY":
                invalid_key = True
        except Exception:
            pass

        if invalid_key:
            print("Diagnostics stopped: INVALID_KEY detected in response body. Check THE_ODDS_API_KEY.")
            self.write_diagnostics(str(diag_path))
            return

        self.write_diagnostics(str(diag_path))

    def _fetch_mock_data(self, date_obj: datetime, league: str) -> List[Tuple[Dict, Any]]:
        """
        Returns a hardcoded sample payload resembling The-Odds-Api response.
        """
        mock_payload = [
            {
                "id": "3c515159345995543c97107787265917",
                "sport_key": "icehockey_nhl",
                "commence_time": (date_obj + timedelta(hours=19)).isoformat() + "Z",
                "home_team": "Edmonton Oilers",
                "away_team": "Toronto Maple Leafs",
                "bookmakers": [
                    {
                        "key": "draftkings",
                        "title": "DraftKings",
                        "markets": [
                            {
                                "key": "player_points",
                                "outcomes": [
                                    {"name": "Connor McDavid", "description": "Over", "price": 1.80, "point": 1.5},
                                    {"name": "Connor McDavid", "description": "Under", "price": 2.05, "point": 1.5}
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
        
        meta = {
            "file_suffix": f"odds_history_{date_obj.strftime('%Y%m%d')}.json",
            "requested_asof": date_obj.replace(tzinfo=timezone.utc),
            "capture_ts": date_obj.replace(hour=12, minute=0, second=0, microsecond=0, tzinfo=timezone.utc),
            "request_uid": f"mock_{date_obj.strftime('%Y%m%d')}",
            "is_dfs": False
        }
        return [(meta, mock_payload)]

    def normalize(self, payload: Any, capture_ts: datetime, raw_path: str, raw_hash: str, requested_asof: Optional[datetime], ingested_at: datetime) -> List[Dict]:
        """
        Normalizes The-Odds-API JSON. 
        Supports both list of events (general odds) and single event (event-odds).
        """
        rows = []
        
        # If payload is a dict, wrap in list (event-odds endpoint returns a single dict)
        if isinstance(payload, dict):
            events = [payload]
        elif isinstance(payload, list):
            events = payload
        else:
            return []

        for event in events:
            event_id = event.get("id")
            sport_key = event.get("sport_key")
            start_ts_str = event.get("commence_time")
            home = event.get("home_team")
            away = event.get("away_team")
            
            # Parse start_ts
            try:
                start_ts = datetime.fromisoformat(start_ts_str.replace("Z", "+00:00"))
            except:
                start_ts = None

            if "nhl" not in str(sport_key).lower():
                continue

            for book in event.get("bookmakers", []):
                book_key = book.get("key")
                book_title = book.get("title")
                
                book_update_str = book.get("last_update")
                book_ts = capture_ts
                if book_update_str:
                    try:
                        book_ts = datetime.fromisoformat(book_update_str.replace("Z", "+00:00"))
                    except:
                        pass
                
                for market in book.get("markets", []):
                    market_key = market.get("key")
                    normalized_market = self.MARKET_MAP.get(market_key)
                    
                    if not normalized_market:
                        continue 
                        
                    for outcome in market.get("outcomes", []):
                        player_name = outcome.get("name")
                        side = outcome.get("description")
                        line = outcome.get("point")
                        decimal_odds = outcome.get("price")
                        
                        if not player_name or not side or line is None:
                            continue

                        rows.append({
                            "source_vendor": self.provider_name,
                            "capture_ts_utc": book_ts,
                            "requested_asof_ts_utc": requested_asof,
                            "ingested_at_utc": ingested_at,
                            "event_id_vendor": event_id,
                            "event_start_ts_utc": start_ts,
                            "player_id_vendor": None, 
                            "player_name_raw": player_name,
                            "market_type": normalized_market,
                            "line": float(line),
                            "side": side.upper() if side else side,
                            "book_id_vendor": book_key,
                            "book_name_raw": book_title,
                            "odds_american": self.decimal_to_american(decimal_odds),
                            "odds_decimal": decimal_odds,
                            "is_live": False, 
                            "raw_payload_path": raw_path,
                            "raw_payload_hash": raw_hash,
                            
                            "join_conf_event": 1.0 if event_id and start_ts else 0.0,
                            "join_conf_player": 0.6 if player_name else 0.0,
                            "join_conf_market": 1.0,
                            "is_dfs": self.mode == "dfs" or self.regions == "us_dfs"
                        })
        return rows

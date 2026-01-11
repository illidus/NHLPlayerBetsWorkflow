from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone
import json
import pandas as pd
from src.nhl_bets.ingestion.storage import RawStorage

class BaseOddsProvider(ABC):
    """
    Abstract Base Class for Odds Ingestion Providers (Phase 12).
    Enforces pattern: Fetch -> Save Raw -> Normalize -> Map Dims -> Return DataFrame.
    """
    
    VALID_MARKETS = {"GOALS", "ASSISTS", "POINTS", "SOG", "BLOCKS"}
    VALID_SIDES = {"OVER", "UNDER"}

    def __init__(self, provider_name: str):
        self.provider_name = provider_name
        self.request_log = []

    def ingest_date_range(self, start_date: datetime, end_date: datetime, league: str = "NHL") -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Orchestrates ingestion for a date range.
        1. Fetch raw data from API/Source.
        2. Save raw data to immutable storage (Phase 11).
        3. Normalize to canonical schema.
        4. Gate for ROI Grade.
        5. Return (roi_grade_df, unresolved_df).
        """
        roi_rows = []
        unresolved_rows = []
        
        # 1. Fetch
        # Providers might implement this as yielding daily batches or one big batch.
        # We'll assume a generator or list of (metadata, payload) tuples.
        # Pass requested_asof implicitly via loop if needed, but fetch_data handles the range.
        raw_batches = self.fetch_data(start_date, end_date, league)
        
        ingested_at = datetime.now(timezone.utc)
        
        for batch_meta, payload in raw_batches:
            # 2. Save Raw
            # batch_meta should contain at least a file_suffix helper
            file_suffix = batch_meta.get('file_suffix', 'data.json')
            requested_asof = batch_meta.get('requested_asof', None) # Should be passed from fetch_data
            forced_capture_ts = batch_meta.get('capture_ts', None)
            request_uid = batch_meta.get('request_uid', None)
            
            file_path, file_hash, capture_ts = RawStorage.save_payload(
                vendor=self.provider_name,
                payload=payload,
                file_suffix=file_suffix,
                capture_ts=forced_capture_ts,
                request_uid=request_uid
            )
            
            # 3. Normalize
            canonical_rows = self.normalize(
                payload=payload, 
                capture_ts=capture_ts, 
                raw_path=file_path, 
                raw_hash=file_hash,
                requested_asof=requested_asof,
                ingested_at=ingested_at
            )
            
            # 4. Gate
            for row in canonical_rows:
                is_valid, reasons = self.is_roi_grade(row, league)
                if is_valid:
                    # Ensure confidence defaults if missing
                    row.setdefault('join_conf_event', 0.0)
                    row.setdefault('join_conf_player', 0.0)
                    row.setdefault('join_conf_market', 1.0) # Market is normalized, so high conf
                    roi_rows.append(row)
                else:
                    # Prepare for staging
                    unresolved_rows.append({
                        "source_vendor": self.provider_name,
                        "capture_ts_utc": row.get("capture_ts_utc"),
                        "ingested_at_utc": ingested_at,
                        "event_id_vendor": row.get("event_id_vendor"),
                        "player_name_raw": row.get("player_name_raw"),
                        "market_type": row.get("market_type"),
                        "line": row.get("line"),
                        "side": row.get("side"),
                        "book_id_vendor": row.get("book_id_vendor"),
                        "odds_american": row.get("odds_american"),
                        "raw_payload_path": file_path,
                        "raw_payload_hash": file_hash,
                        "failure_reasons": json.dumps(reasons),
                        "raw_row_json": json.dumps(row, default=str)
                    })
            
        roi_df = pd.DataFrame(roi_rows)
        unresolved_df = pd.DataFrame(unresolved_rows)
        
        return roi_df, unresolved_df

    def is_roi_grade(self, row: Dict, target_league: str) -> Tuple[bool, List[str]]:
        """
        Checks if a normalized row meets ROI-grade standards.
        Returns (is_valid, list_of_reasons).
        """
        reasons = []
        
        # League check (implicit in logic usually, but strict check here if row has league)
        # Assuming row might not have league column, but we ingested for target_league.
        
        # Event Identity
        if not row.get("event_id_vendor"):
            reasons.append("MISSING_EVENT_ID")
        if not row.get("event_start_ts_utc"):
            reasons.append("MISSING_EVENT_START")
            
        # Market Validation
        mkt = row.get("market_type")
        if mkt not in self.VALID_MARKETS:
            reasons.append(f"INVALID_MARKET_{mkt}")
            
        # Side/Line
        side = row.get("side")
        if side not in self.VALID_SIDES:
            reasons.append(f"INVALID_SIDE_{side}")
        if row.get("line") is None or not isinstance(row.get("line"), (int, float)):
             reasons.append("INVALID_LINE")
             
        # Odds
        odds = row.get("odds_american")
        if not isinstance(odds, int) or odds == 0:
            reasons.append("INVALID_ODDS_TYPE")
        elif odds < -10000 or odds > 10000:
            reasons.append("EXTREME_ODDS_VALUE")
            
        # Book
        if not row.get("book_id_vendor"):
            reasons.append("MISSING_BOOK_ID")
            
        # Timestamps
        if not row.get("capture_ts_utc"):
            reasons.append("MISSING_CAPTURE_TS")
            
        return (len(reasons) == 0, reasons)

    def log_request(self, endpoint: str, params: Dict, status: int, headers: Dict, cost_est: float):
        """Logs API request details for audit."""
        self.request_log.append({
            "ts": datetime.now(timezone.utc),
            "endpoint": endpoint,
            "params": str(params), # Redact key in impl
            "status": status,
            "cost_est": cost_est,
            "quota_remaining": self.get_quota_remaining(headers)
        })

    @abstractmethod
    def get_quota_remaining(self, headers: Dict) -> Optional[int]:
        pass

    @abstractmethod
    def fetch_data(self, start_date: datetime, end_date: datetime, league: str) -> List[Tuple[Dict, Any]]:
        """
        Fetches data from source.
        Returns list of (metadata_dict, payload).
        metadata_dict must include 'requested_asof' (datetime).
        """
        pass

    @abstractmethod
    def normalize(self, payload: Any, capture_ts: datetime, raw_path: str, raw_hash: str, requested_asof: Optional[datetime], ingested_at: datetime) -> List[Dict]:
        """
        Parses raw payload into flat dictionaries matching fact_prop_odds schema.
        Must populate new timestamp fields.
        """
        pass
    
    @staticmethod
    def compute_request_uid(vendor: str, endpoint: str, params: Dict, requested_asof: Optional[datetime]) -> str:
        """
        Computes a deterministic hash for a request to enable resume safety.
        """
        import hashlib
        # Filter out volatile params like apiKey if they were passed
        safe_params = params.copy()
        if 'apiKey' in safe_params:
            safe_params['apiKey'] = 'REDACTED'
            
        base_str = f"{vendor}_{endpoint}_{json.dumps(safe_params, sort_keys=True)}_{requested_asof.isoformat() if requested_asof else ''}"
        return hashlib.md5(base_str.encode()).hexdigest()

    @staticmethod
    def american_to_decimal(american: int) -> float:
        if american > 0:
            return 1 + (american / 100.0)
        else:
            return 1 + (100.0 / abs(american))
            
    @staticmethod
    def decimal_to_american(decimal: float) -> int:
        if decimal < 1.01: 
            return -10000 # Edge case protection
        if decimal >= 2.0:
            return int((decimal - 1) * 100)
        else:
            return int(-100 / (decimal - 1))

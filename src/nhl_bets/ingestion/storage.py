import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple, Union

class RawStorage:
    """
    Phase 11: Immutable Raw Storage for Odds Ingestion.
    Handles writing raw payloads to outputs/odds/raw/ with deterministic paths and hashing.
    """
    
    BASE_DIR = Path("outputs/odds/raw")
    
    @staticmethod
    def _compute_sha256(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    @staticmethod
    def _get_timestamp_path_components(ts: datetime) -> Tuple[str, str, str]:
        """Returns (YYYY, MM, DD) strings."""
        return (
            ts.strftime("%Y"),
            ts.strftime("%m"),
            ts.strftime("%d")
        )

    @classmethod
    def save_payload(
        cls, 
        vendor: str, 
        payload: Union[str, bytes, dict, list], 
        file_suffix: str,
        capture_ts: datetime = None,
        request_uid: str = None
    ) -> Tuple[str, str, datetime]:
        """
        Saves a raw payload to the filesystem.
        
        Args:
            vendor: Name of the vendor (e.g., 'UNABATED', 'PLAYNOW').
            payload: The content to save (str, bytes, or dict). Dicts are JSON dumped.
            file_suffix: Suffix for the filename (e.g., 'propodds.json', 'props.html').
            capture_ts: Timestamp of capture. Defaults to now(UTC).
            request_uid: Optional unique identifier for the request to prevent duplicates.
            
        Returns:
            Tuple of (relative_file_path, sha256_hash, capture_ts_utc)
        """
        if capture_ts is None:
            capture_ts = datetime.now(timezone.utc)
        else:
            # Ensure UTC
            if capture_ts.tzinfo is None:
                capture_ts = capture_ts.replace(tzinfo=timezone.utc)
            else:
                capture_ts = capture_ts.astimezone(timezone.utc)

        # Prepare content
        if isinstance(payload, (dict, list)):
            content_bytes = json.dumps(payload, sort_keys=True).encode('utf-8')
            mode = 'wb'
        elif isinstance(payload, str):
            content_bytes = payload.encode('utf-8')
            mode = 'wb'
        elif isinstance(payload, bytes):
            content_bytes = payload
            mode = 'wb'
        else:
            raise ValueError(f"Unsupported payload type: {type(payload)}")

        content_hash = cls._compute_sha256(content_bytes)

        # Construct Path: outputs/odds/raw/<vendor>/YYYY/MM/DD/
        year, month, day = cls._get_timestamp_path_components(capture_ts)
        ts_str = capture_ts.strftime("%H%M%S")
        
        # Clean vendor name
        vendor_clean = vendor.upper().replace(" ", "_")
        
        dir_path = cls.BASE_DIR / vendor_clean / year / month / day
        dir_path.mkdir(parents=True, exist_ok=True)
        
        if request_uid:
            filename = f"{ts_str}_{request_uid}_{file_suffix}"
        else:
            filename = f"{ts_str}_{file_suffix}"
            
        file_path = dir_path / filename
        
        # Check if already exists (Resume safety)
        if request_uid:
            # Check for any file with this request_uid in the same day folder
            existing = list(dir_path.glob(f"*_{request_uid}_*"))
            if existing:
                return str(existing[0]), content_hash, capture_ts

        # Write
        with open(file_path, mode) as f:
            f.write(content_bytes)
            
        return str(file_path), content_hash, capture_ts

    @classmethod
    def get_base_dir(cls) -> Path:
        return cls.BASE_DIR

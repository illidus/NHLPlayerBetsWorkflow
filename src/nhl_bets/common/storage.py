import hashlib
import json
import os
import logging
from datetime import datetime, timezone
from typing import Tuple, Any, Optional

logger = logging.getLogger(__name__)

STORAGE_ROOT = "outputs/odds/raw"

def save_raw_payload(vendor: str, payload: Any, extension: str = "json", suffix: Optional[str] = None) -> Tuple[str, str, datetime]:
    """
    Saves a raw payload to the local filesystem in a date-partitioned structure.
    Returns (relative_path, sha256_hash, capture_ts).
    """
    now = datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    time_prefix = now.strftime("%H%M%S")
    
    # Create filename
    filename = f"{time_prefix}_{vendor.lower()}"
    if suffix:
        filename += f"_{suffix}"
    filename += f".{extension}"
    
    dir_path = os.path.join(STORAGE_ROOT, vendor.upper(), date_path)
    os.makedirs(dir_path, exist_ok=True)
    
    full_path = os.path.join(dir_path, filename)
    rel_path = os.path.relpath(full_path, start=os.getcwd())
    
    # Convert payload to string for hashing/saving
    if extension == "json":
        content = json.dumps(payload, indent=2)
    else:
        content = str(payload)
        
    # Calculate SHA256
    sha_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    # Save file
    with open(full_path, "w", encoding="utf-8") as f:
        f.write(content)
        
    # Save hash file for integrity verification
    with open(f"{full_path}.sha256", "w") as f:
        f.write(sha_hash)
        
    return rel_path, sha_hash, now

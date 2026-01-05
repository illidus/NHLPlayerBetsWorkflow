import os
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple, Union, Any

def save_raw_payload(
    vendor: str, 
    payload: Union[str, dict, bytes], 
    extension: str,
    base_dir: str = "outputs/odds/raw"
) -> Tuple[str, str, datetime]:
    """
    Saves a raw payload to an immutable location and returns metadata.
    
    Args:
        vendor: Name of the vendor (e.g., 'UNABATED')
        payload: The verbatim response (str, dict, or bytes)
        extension: File extension (e.g., 'json', 'html')
        base_dir: Root directory for raw storage
        
    Returns:
        Tuple of (relative_file_path, sha256_hash, capture_ts_utc)
    """
    capture_ts = datetime.now(timezone.utc)
    
    # Prepare content and hash
    if isinstance(payload, dict):
        content = json.dumps(payload, sort_keys=True)
        encoded_content = content.encode('utf-8')
    elif isinstance(payload, str):
        content = payload
        encoded_content = content.encode('utf-8')
    else: # bytes
        encoded_content = payload
        content = None # Not needed for write if bytes
        
    sha256_hash = hashlib.sha256(encoded_content).hexdigest()
    
    # Path construction: YYYY/MM/DD
    date_path = capture_ts.strftime("%Y/%m/%d")
    filename = f"{capture_ts.strftime('%H%M%S')}_{vendor.lower()}.{extension}"
    
    full_dir = Path(base_dir) / vendor.upper() / date_path
    full_dir.mkdir(parents=True, exist_ok=True)
    
    file_path = full_dir / filename
    
    # Write file
    if isinstance(payload, bytes):
        with open(file_path, "wb") as f:
            f.write(payload)
    else:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
            
    # Also save a sidecar hash file for manual verification
    hash_file = file_path.with_suffix(file_path.suffix + ".sha256")
    with open(hash_file, "w") as f:
        f.write(sha256_hash)
        
    # Return relative path for DB storage
    rel_path = os.path.relpath(file_path, start=os.getcwd())
    
    return rel_path, sha256_hash, capture_ts

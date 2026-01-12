import json
import pytest
from datetime import datetime, timezone
from src.nhl_bets.ingestion.storage import RawStorage

def test_storage_save_payload(tmp_path, monkeypatch):
    # Monkeypatch BASE_DIR to use tmp_path
    monkeypatch.setattr(RawStorage, "BASE_DIR", tmp_path / "raw")
    
    vendor = "TEST_VENDOR"
    payload = {"foo": "bar", "baz": 123}
    suffix = "test_data.json"
    
    # Execute
    path, content_hash, ts = RawStorage.save_payload(vendor, payload, suffix)
    
    # Verify
    assert "TEST_VENDOR" in path
    assert suffix in path
    
    # Read back
    with open(path, 'r') as f:
        content = f.read()
        assert '"foo": "bar"' in content
        
    # Verify Hash
    import hashlib
    expected = hashlib.sha256(json.dumps(payload, sort_keys=True).encode('utf-8')).hexdigest()
    assert content_hash == expected

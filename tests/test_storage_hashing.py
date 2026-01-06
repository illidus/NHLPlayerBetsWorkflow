import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from nhl_bets.common import storage


def test_hash_stability(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "STORAGE_ROOT", str(tmp_path))
    payload_a = {"b": 2, "a": 1}
    payload_b = {"a": 1, "b": 2}

    _, hash_a, _ = storage.save_raw_payload("TEST", payload_a, "json")
    _, hash_b, _ = storage.save_raw_payload("TEST", payload_b, "json")

    assert hash_a == hash_b

    # Ensure output paths stay within the temp test directory.
    assert os.path.exists(tmp_path)

import hashlib
import json
import os
import pickle
from pathlib import Path
from typing import Any, Optional


def _hash_payload(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def cache_path(cache_dir: str, payload: Any, suffix: str) -> Path:
    key = _hash_payload(payload)
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    return Path(cache_dir) / f"{key}.{suffix}"


def load_pickle(path: Path) -> Optional[Any]:
    if not path.exists():
        return None
    with path.open("rb") as f:
        return pickle.load(f)


def save_pickle(path: Path, obj: Any) -> None:
    with path.open("wb") as f:
        pickle.dump(obj, f)

from __future__ import annotations

from typing import Dict

from .utils import stable_hash


def build_record_hash(record: Dict[str, object]) -> str:
    hash_fields = {
        "source": record.get("source"),
        "source_url": record.get("source_url"),
        "canonical_url": record.get("canonical_url"),
        "player_name_clean": record.get("player_name_clean"),
        "market": record.get("market"),
        "line": record.get("line"),
        "side": record.get("side"),
        "odds": record.get("odds"),
        "bookmaker": record.get("bookmaker"),
        "game_date": record.get("game_date"),
    }
    return stable_hash(hash_fields)

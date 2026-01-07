from __future__ import annotations

import re
from typing import Optional

from .parsers import MARKET_MAP

WHITESPACE_RE = re.compile(r"\s+")


def clean_player_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    cleaned = WHITESPACE_RE.sub(" ", name.strip())
    return cleaned.title()


def normalize_market(market_raw: Optional[str]) -> Optional[str]:
    if not market_raw:
        return None
    return MARKET_MAP.get(market_raw.lower())


def normalize_side(side: Optional[str]) -> Optional[str]:
    if not side:
        return None
    normalized = side.upper()
    if normalized in {"OVER", "UNDER"}:
        return normalized
    return None


def normalize_bookmaker(bookmaker: Optional[str]) -> Optional[str]:
    if not bookmaker:
        return None
    return bookmaker.strip().lower()

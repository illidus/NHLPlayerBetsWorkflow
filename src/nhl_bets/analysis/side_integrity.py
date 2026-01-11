from __future__ import annotations

from typing import Dict, Tuple, Optional

import pandas as pd

from nhl_bets.analysis.normalize import normalize_name


def normalize_player(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return normalize_name(str(value))


def normalize_market(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip().upper()


def normalize_book(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip().lower()


def normalize_side(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    raw = str(value).strip().upper()
    if raw in {"OVER", "O", "YES"}:
        return "OVER"
    if raw in {"UNDER", "U", "NO"}:
        return "UNDER"
    return raw


def build_odds_side_lookup(odds_df: pd.DataFrame) -> Dict[Tuple[str, str, float, str, int], str]:
    """
    Build a lookup for odds-side validation keyed by:
    (player_key, market_key, line_key, book_key, odds_american).
    """
    if odds_df.empty:
        return {}

    df = odds_df.copy()
    df["player_key"] = df["player_name_raw"].apply(normalize_player)
    df["market_key"] = df["market_type"].apply(normalize_market)
    df["line_key"] = pd.to_numeric(df["line"], errors="coerce").round(3)
    df["book_key"] = df["book_name_raw"].apply(normalize_book)
    df["side_key"] = df["side"].apply(normalize_side)

    lookup: Dict[Tuple[str, str, float, str, int], str] = {}
    grouped = df.groupby(
        ["player_key", "market_key", "line_key", "book_key", "odds_american"],
        dropna=False,
    )["side_key"]
    for key, sides in grouped:
        uniq = {s for s in sides if s}
        if len(uniq) == 1:
            lookup[key] = uniq.pop()
        elif len(uniq) > 1:
            lookup[key] = "AMBIGUOUS"
    return lookup


def resolve_odds_side(
    lookup: Dict[Tuple[str, str, float, str, int], str],
    player: object,
    market: object,
    line: object,
    book: object,
    odds_american: object,
) -> Tuple[str, str]:
    """
    Resolve odds side for a bet row. Returns (side, reason).
    """
    try:
        line_key = round(float(line), 3)
    except (TypeError, ValueError):
        line_key = None
    try:
        odds_key = int(odds_american)
    except (TypeError, ValueError):
        odds_key = None

    if line_key is None or odds_key is None:
        return "", "MISSING_KEY_FIELDS"

    key = (
        normalize_player(player),
        normalize_market(market),
        line_key,
        normalize_book(book),
        odds_key,
    )
    if key not in lookup:
        return "", "NO_MATCH"
    side = lookup[key]
    if side == "AMBIGUOUS":
        return side, "AMBIGUOUS_MATCH"
    return side, "OK"

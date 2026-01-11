from __future__ import annotations

import logging
import re
import json
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple
from pathlib import Path

from . import config

logger = logging.getLogger(__name__)

# Load Players
PLAYERS_PATH = config.DATA_DIR / "nhl_players.json"
NHL_PLAYERS = set()
if PLAYERS_PATH.exists():
    try:
        with open(PLAYERS_PATH, "r") as f:
            NHL_PLAYERS = set(json.load(f))
    except Exception as e:
        logger.warning(f"Failed to load NHL players: {e}")

# Normalize names for fuzzy matching check
NHL_PLAYERS_NORMALIZED = {p.lower().replace(".", "").replace(" ", "") for p in NHL_PLAYERS}

BOOKMAKER_PATTERN = re.compile(r"\b(" + "|".join(config.KNOWN_BOOKMAKERS) + r")\b", re.IGNORECASE)

MARKET_MAP = {
    "shots on goal": "SOG",
    "shots": "SOG",
    "points": "POINTS",
    "assists": "ASSISTS",
    "goals": "GOALS",
    "blocked shots": "BLOCKED_SHOTS",
    "power play points": "POWERPLAY_POINTS",
}


def is_nhl_page(url: str, title: str) -> bool:
    """
    Checks URL and Title for NHL relevance.
    """
    content = (str(url) + " " + str(title)).lower()
    if "nhl" in content:
        return True
    for team in config.NHL_TEAMS:
        if team in content:
            return True
    return False

def is_nhl_block(text: str) -> bool:
    """
    Validates if the text block is relevant to NHL and safe from cross-sport contamination.
    """
    text_lower = text.lower()
    
    # Negative Gate
    for kw in config.NEGATIVE_KEYWORDS:
        if kw in text_lower:
            return False
            
    # Positive Gate
    for team in config.NHL_TEAMS:
        if team in text_lower:
            return True
            
    # Player Check
    for p in NHL_PLAYERS:
         if p.lower() in text_lower:
             return True

    return False

def classify_entity(extracted_name: str) -> str:
    """
    Determines if the extracted name is a Player or a Game/Team entity.
    """
    cleaned = extracted_name.lower().replace(".", "").replace(" ", "")
    
    if cleaned in NHL_PLAYERS_NORMALIZED:
        return "PLAYER"
    
    extracted_lower = extracted_name.lower()
    for team in config.NHL_TEAMS:
        if team in extracted_lower:
            return "GAME"
            
    if " vs " in extracted_lower or " at " in extracted_lower:
        return "GAME"

    return "UNKNOWN"


@dataclass
class ParsedCandidate:
    player_name_raw: Optional[str]
    market_raw: str
    line: Optional[float]
    side: Optional[str]
    odds: Optional[int]
    odds_format: Optional[str]
    bookmaker: Optional[str]
    snippet: str
    confidence: float
    parser: str
    status_code: str = "CANDIDATE_READY"
    rejection_reason: Optional[str] = None
    derived_game_date: Optional[str] = None
    entity_type: str = "PLAYER"

    def to_record(self) -> Dict[str, object]:
        rec = {
            "player_name_raw": self.player_name_raw,
            "market_raw": self.market_raw,
            "market": MARKET_MAP.get(self.market_raw.lower()),
            "line": self.line,
            "side": self.side,
            "odds": self.odds,
            "odds_format": self.odds_format,
            "bookmaker": self.bookmaker,
            "snippet": self.snippet,
            "confidence": self.confidence,
            "parser": self.parser,
            "status_code": self.status_code,
            "rejection_reason": self.rejection_reason,
            "derived_game_date": self.derived_game_date,
            "entity_type": self.entity_type,
        }
        if self.entity_type == "GAME":
            rec["matchup_text_raw"] = self.player_name_raw 
            rec["player_name_raw"] = None
        
        return rec


class Parser:
    name = "base"

    def parse(self, text: str) -> Iterable[ParsedCandidate]:
        raise NotImplementedError


def _extract_bookmaker(snippet: str) -> Optional[str]:
    match = BOOKMAKER_PATTERN.search(snippet.lower())
    if match:
        return match.group(1).lower()
    return None


def _confidence(base: float, odds: Optional[int], line: Optional[float]) -> float:
    score = base
    if odds is not None:
        score += 0.2
    if line is not None:
        score += 0.2
    return min(score, 0.95)


def _determine_status(odds: Optional[int], bookmaker: Optional[str], entity_type: str) -> Tuple[str, Optional[str]]:
    if entity_type == "UNKNOWN":
        return "REJECT_ENTITY", "Entity not recognized as NHL Player or Game"
        
    if odds is None or odds == 0:
        return "MISSING_ODDS", "No odds found in snippet (or 0)"

    if bookmaker is None:
        return "MISSING_BOOK", "No bookmaker attributed"
        
    return "CANDIDATE_READY", None


class OverUnderParser(Parser):
    name = "over_under"
    pattern = re.compile(
        r"(?P<player>[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)+)\s+(?P<side>over|under)\s+"
        r"(?P<line>\d+(?:\.\d+)?)\s+(?P<market>shots on goal|shots|points|assists|goals|blocked shots|power play points)"
        r"(?:\s*\(?(?P<odds>[+-]?\d+)\)?)?",
        re.IGNORECASE,
    )

    def parse(self, text: str) -> Iterable[ParsedCandidate]:
        for match in self.pattern.finditer(text):
            start, end = match.span()
            lookahead = text[end : end + 30]
            snippet = match.group(0) + lookahead
            odds_str = match.group("odds")
            line = match.group("line")
            
            odds = int(odds_str) if odds_str else None
            if odds == 0: odds = None
            
            bookmaker = _extract_bookmaker(snippet)
            name_raw = match.group("player").strip()
            entity_type = classify_entity(name_raw)
            
            status_code, rejection_reason = _determine_status(odds, bookmaker, entity_type)

            candidate = ParsedCandidate(
                player_name_raw=name_raw,
                market_raw=match.group("market").strip(),
                line=float(line) if line else None,
                side=match.group("side").upper() if match.group("side") else None,
                odds=odds,
                odds_format="american" if odds else None,
                bookmaker=bookmaker,
                snippet=snippet.strip(),
                confidence=_confidence(0.5, odds, float(line) if line else None),
                parser=self.name,
                status_code=status_code,
                rejection_reason=rejection_reason,
                entity_type=entity_type
            )
            yield candidate


class InlineOddsParser(Parser):
    name = "inline_odds"
    pattern = re.compile(
        r"(?P<player>[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)+)\s+(?P<market>shots on goal|shots|points|assists|goals)"
        r"\s+(?P<side>over|under)\s+(?P<line>\d+(?:\.\d+)?)\s*@?\s*\(?(?P<odds>[+-]?\d+)\)?",
        re.IGNORECASE,
    )

    def parse(self, text: str) -> Iterable[ParsedCandidate]:
        for match in self.pattern.finditer(text):
            start, end = match.span()
            lookahead = text[end : end + 30]
            snippet = match.group(0) + lookahead
            odds = int(match.group("odds"))
            if odds == 0: odds = None
            
            line = float(match.group("line"))
            bookmaker = _extract_bookmaker(snippet)
            name_raw = match.group("player").strip()
            entity_type = classify_entity(name_raw)
            
            status_code, rejection_reason = _determine_status(odds, bookmaker, entity_type)

            candidate = ParsedCandidate(
                player_name_raw=name_raw,
                market_raw=match.group("market").strip(),
                line=line,
                side=match.group("side").upper(),
                odds=odds,
                odds_format="american",
                bookmaker=bookmaker,
                snippet=snippet.strip(),
                confidence=_confidence(0.6, odds, line),
                parser=self.name,
                status_code=status_code,
                rejection_reason=rejection_reason,
                entity_type=entity_type
            )
            yield candidate


class ParentheticalParser(Parser):
    name = "parenthetical"
    pattern = re.compile(
        r"(?P<player>[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)+)\s+\((?P<market>shots on goal|shots|points|assists|goals)"
        r"\s+(?P<side>over|under)\s+(?P<line>\d+(?:\.\d+)?)\s*\)\s*@?\s*\(?(?P<odds>[+-]?\d+)\)?",
        re.IGNORECASE,
    )

    def parse(self, text: str) -> Iterable[ParsedCandidate]:
        for match in self.pattern.finditer(text):
            start, end = match.span()
            lookahead = text[end : end + 30]
            snippet = match.group(0) + lookahead
            odds = int(match.group("odds"))
            if odds == 0: odds = None
            
            line = float(match.group("line"))
            bookmaker = _extract_bookmaker(snippet)
            name_raw = match.group("player").strip()
            entity_type = classify_entity(name_raw)
            
            status_code, rejection_reason = _determine_status(odds, bookmaker, entity_type)
            
            candidate = ParsedCandidate(
                player_name_raw=name_raw,
                market_raw=match.group("market").strip(),
                line=line,
                side=match.group("side").upper(),
                odds=odds,
                odds_format="american",
                bookmaker=bookmaker,
                snippet=snippet.strip(),
                confidence=_confidence(0.55, odds, line),
                parser=self.name,
                status_code=status_code,
                rejection_reason=rejection_reason,
                entity_type=entity_type
            )
            yield candidate


class ParserRegistry:
    def __init__(self) -> None:
        self.parsers: List[Parser] = []

    def register(self, parser: Parser) -> None:
        self.parsers.append(parser)

    def parse(self, text: str) -> List[ParsedCandidate]:
        candidates: List[ParsedCandidate] = []
        for parser in self.parsers:
            try:
                candidates.extend(parser.parse(text))
            except Exception:
                logger.exception("Parser %s failed", parser.name)
        return candidates


def build_registry() -> ParserRegistry:
    registry = ParserRegistry()
    registry.register(OverUnderParser())
    registry.register(InlineOddsParser())
    registry.register(ParentheticalParser())
    return registry
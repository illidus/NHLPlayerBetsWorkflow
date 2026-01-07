from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

from . import config

logger = logging.getLogger(__name__)

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


@dataclass
class ParsedCandidate:
    player_name_raw: str
    market_raw: str
    line: Optional[float]
    side: Optional[str]
    odds: Optional[int]
    odds_format: Optional[str]
    bookmaker: Optional[str]
    snippet: str
    confidence: float
    parser: str

    def to_record(self) -> Dict[str, object]:
        return {
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
        }


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


class OverUnderParser(Parser):
    name = "over_under"
    pattern = re.compile(
        r"(?P<player>[A-Z][a-z]+\s+[A-Z][a-z]+)\s+(?P<side>over|under)\s+"
        r"(?P<line>\d+\.?\d*)\s+(?P<market>shots on goal|shots|points|assists|goals|blocked shots|power play points)"
        r"(?:\s*\(?(?P<odds>[+-]?\d+)\)?)?",
        re.IGNORECASE,
    )

    def parse(self, text: str) -> Iterable[ParsedCandidate]:
        for match in self.pattern.finditer(text):
            snippet = match.group(0)
            odds = match.group("odds")
            line = match.group("line")
            candidate = ParsedCandidate(
                player_name_raw=match.group("player").strip(),
                market_raw=match.group("market").strip(),
                line=float(line) if line else None,
                side=match.group("side").upper() if match.group("side") else None,
                odds=int(odds) if odds else None,
                odds_format="american" if odds else None,
                bookmaker=_extract_bookmaker(snippet),
                snippet=snippet,
                confidence=_confidence(0.5, int(odds) if odds else None, float(line) if line else None),
                parser=self.name,
            )
            yield candidate


class InlineOddsParser(Parser):
    name = "inline_odds"
    pattern = re.compile(
        r"(?P<player>[A-Z][a-z]+\s+[A-Z][a-z]+)\s+(?P<market>shots on goal|shots|points|assists|goals)"
        r"\s+(?P<side>over|under)\s+(?P<line>\d+\.?\d*)\s*@?\s*(?P<odds>[+-]?\d+)",
        re.IGNORECASE,
    )

    def parse(self, text: str) -> Iterable[ParsedCandidate]:
        for match in self.pattern.finditer(text):
            snippet = match.group(0)
            odds = int(match.group("odds"))
            line = float(match.group("line"))
            candidate = ParsedCandidate(
                player_name_raw=match.group("player").strip(),
                market_raw=match.group("market").strip(),
                line=line,
                side=match.group("side").upper(),
                odds=odds,
                odds_format="american",
                bookmaker=_extract_bookmaker(snippet),
                snippet=snippet,
                confidence=_confidence(0.6, odds, line),
                parser=self.name,
            )
            yield candidate


class ParentheticalParser(Parser):
    name = "parenthetical"
    pattern = re.compile(
        r"(?P<player>[A-Z][a-z]+\s+[A-Z][a-z]+)\s+\((?P<market>shots on goal|shots|points|assists|goals)"
        r"\s+(?P<side>over|under)\s+(?P<line>\d+\.?\d*)\s*\)\s*(?P<odds>[+-]?\d+)",
        re.IGNORECASE,
    )

    def parse(self, text: str) -> Iterable[ParsedCandidate]:
        for match in self.pattern.finditer(text):
            snippet = match.group(0)
            odds = int(match.group("odds"))
            line = float(match.group("line"))
            candidate = ParsedCandidate(
                player_name_raw=match.group("player").strip(),
                market_raw=match.group("market").strip(),
                line=line,
                side=match.group("side").upper(),
                odds=odds,
                odds_format="american",
                bookmaker=_extract_bookmaker(snippet),
                snippet=snippet,
                confidence=_confidence(0.55, odds, line),
                parser=self.name,
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

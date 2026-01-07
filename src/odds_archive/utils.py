from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urlparse, urlunparse, urlencode, parse_qsl
from urllib.robotparser import RobotFileParser

from . import config

UTM_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_id",
    "utm_reader",
    "utm_name",
    "utm_referrer",
    "utm_source_platform",
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
}


@dataclass
class RobotsCache:
    cache: Dict[str, RobotFileParser]

    def get(self, base_url: str) -> RobotFileParser:
        if base_url in self.cache:
            return self.cache[base_url]
        parser = RobotFileParser()
        parser.set_url(f"{base_url}/robots.txt")
        try:
            parser.read()
        except Exception:
            parser = RobotFileParser()
            parser.parse([])
        self.cache[base_url] = parser
        return parser


def setup_logger(name: str, log_file: Optional[str] = None) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
    return logger


def ensure_dirs() -> None:
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    config.RUN_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    config.ODDS_ARCHIVE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def stable_hash(payload: Dict[str, Any]) -> str:
    normalized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    query_params = [
        (k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=False) if k not in UTM_PARAMS
    ]
    normalized_query = urlencode(sorted(query_params))
    normalized = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        query=normalized_query,
        fragment="",
    )
    return urlunparse(normalized)


def chunked(iterable: Iterable[Any], size: int) -> Iterable[list[Any]]:
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def sleep_with_rate_limit(last_ts: Optional[float]) -> float:
    now = time.time()
    if last_ts is None:
        return now
    elapsed = now - last_ts
    if elapsed < config.RATE_LIMIT_SECONDS:
        time.sleep(config.RATE_LIMIT_SECONDS - elapsed)
    return time.time()

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

from . import config
from .utils import RobotsCache, sha256_text

logger = logging.getLogger(__name__)


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        # Handle some common formats or just let date_parser handle it
        return date_parser.parse(value)
    except (ValueError, TypeError):
        return None


def _extract_json_ld_datetime(soup: BeautifulSoup, key: str) -> Optional[datetime]:
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                # Simple case
                if data.get(key):
                    parsed = _parse_datetime(data.get(key))
                    if parsed:
                        return parsed
                # Graph case
                if "@graph" in data:
                    for item in data["@graph"]:
                        if item.get(key):
                            parsed = _parse_datetime(item.get(key))
                            if parsed:
                                return parsed
            elif isinstance(data, list):
                for item in data:
                    if item.get(key):
                        parsed = _parse_datetime(item.get(key))
                        if parsed:
                            return parsed
        except (json.JSONDecodeError, TypeError):
            continue
    return None


def _extract_meta_datetime(soup: BeautifulSoup, keys: list[str]) -> Optional[datetime]:
    for key in keys:
        meta = soup.find("meta", attrs={"property": key}) or soup.find("meta", attrs={"name": key})
        if meta and meta.get("content"):
            parsed = _parse_datetime(meta.get("content"))
            if parsed:
                return parsed
    return None


def _extract_title(soup: BeautifulSoup) -> Optional[str]:
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    meta = soup.find("meta", attrs={"property": "og:title"})
    if meta and meta.get("content"):
        return meta.get("content").strip()
    return None


def _extract_canonical(soup: BeautifulSoup) -> Optional[str]:
    link = soup.find("link", rel="canonical")
    if link and link.get("href"):
        return link.get("href").strip()
    meta = soup.find("meta", attrs={"property": "og:url"})
    if meta and meta.get("content"):
        return meta.get("content").strip()
    return None


def _extract_snippet(soup: BeautifulSoup, limit: int = 800) -> Optional[str]:
    body = soup.get_text(" ", strip=True)
    if not body:
        return None
    return body[:limit]


def _extract_text(soup: BeautifulSoup) -> str:
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    # Use newline as separator to preserve blocks/lines
    return soup.get_text("\n", strip=True)


def _infer_parse_hint(text: str) -> Optional[str]:
    lowered = text.lower()
    if "odds" in lowered and "over" in lowered:
        return "odds mention with over/under"
    if "prop" in lowered:
        return "prop mention"
    return None


def fetch_page(url: str, robots_cache: RobotsCache) -> dict:
    parsed = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    parser = robots_cache.get(base_url)
    if not parser.can_fetch(config.USER_AGENT, url):
        raise PermissionError("Blocked by robots.txt")

    response = requests.get(
        url,
        timeout=config.REQUEST_TIMEOUT,
        headers={"User-Agent": config.USER_AGENT},
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")

    extracted_text = _extract_text(soup)
    content_hash = sha256_text(extracted_text)

    publish_ts = _extract_meta_datetime(
        soup,
        [
            "article:published_time",
            "pubdate",
            "publishdate",
            "date",
            "timestamp",
            "dc.date",
            "datePublished",
        ],
    )
    if not publish_ts:
        publish_ts = _extract_json_ld_datetime(soup, "datePublished")

    updated_ts = _extract_meta_datetime(
        soup,
        ["article:modified_time", "lastmod", "dateModified", "dc.date.modified"],
    )
    if not updated_ts:
        updated_ts = _extract_json_ld_datetime(soup, "dateModified")

    return {
        "http_status": response.status_code,
        "canonical_url": _extract_canonical(soup),
        "title": _extract_title(soup),
        "publish_ts": publish_ts,
        "updated_ts": updated_ts,
        "extracted_text": extracted_text,
        "html_snippet": _extract_snippet(soup),
        "content_hash": content_hash,
        "parse_hint": _infer_parse_hint(extracted_text),
    }

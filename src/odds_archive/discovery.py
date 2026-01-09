from __future__ import annotations

import logging
from typing import Iterable, List, Optional
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup

from . import config
from .utils import normalize_url

logger = logging.getLogger(__name__)


def _filter_url(url: str) -> bool:
    if not url.startswith("http"):
        return False
    lowered = url.lower()
    if config.ALLOWED_DOMAINS:
        domain = urlparse(url).netloc.lower()
        if not any(domain.endswith(allowed.lower()) for allowed in config.ALLOWED_DOMAINS):
            return False
    
    # Keyword filtering to focus on NHL props
    keywords = ["nhl", "prop", "pick", "best-bet", "betting", "odds"]
    if not any(kw in lowered for kw in keywords):
        return False
        
    # Exclude some obvious non-article types
    exclude = ["/video/", "/tag/", "/category/", "/author/", "sitemap"]
    if any(ex in lowered for ex in exclude):
        return False

    return True


def discover_from_sitemap(url: str) -> List[str]:
    logger.info("Fetching sitemap %s", url)
    urls: List[str] = []
    try:
        response = requests.get(url, timeout=config.REQUEST_TIMEOUT, headers={"User-Agent": config.USER_AGENT})
        response.raise_for_status()
        content = response.text
    except Exception as e:
        logger.warning("Failed to fetch sitemap %s: %s", url, e)
        return urls

    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        logger.warning("Failed to parse sitemap XML from %s", url)
        return urls

    namespace = ""
    if root.tag.startswith("{"):
        namespace = root.tag.split("}")[0] + "}"

    if root.tag.endswith("sitemapindex"):
        for sitemap in root.findall(f"{namespace}sitemap"):
            loc = sitemap.find(f"{namespace}loc")
            if loc is not None and loc.text:
                urls.extend(discover_from_sitemap(loc.text.strip()))
    else:
        for url_node in root.findall(f"{namespace}url"):
            loc = url_node.find(f"{namespace}loc")
            if loc is not None and loc.text:
                candidate = normalize_url(loc.text.strip())
                if _filter_url(candidate):
                    urls.append(candidate)

    return urls


def discover_from_index(url: str) -> List[str]:
    logger.info("Fetching index %s", url)
    response = requests.get(url, timeout=config.REQUEST_TIMEOUT, headers={"User-Agent": config.USER_AGENT})
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")
    urls: List[str] = []
    for anchor in soup.select("a[href]"):
        href = anchor.get("href")
        if not href:
            continue
        candidate = normalize_url(urljoin(url, href))
        if _filter_url(candidate):
            urls.append(candidate)
    return urls


def discover_urls(
    sitemaps: Optional[Iterable[str]] = None,
    index_urls: Optional[Iterable[str]] = None,
    manual_urls: Optional[Iterable[str]] = None,
) -> List[str]:
    discovered: List[str] = []
    for sitemap in sitemaps or []:
        discovered.extend(discover_from_sitemap(sitemap))
    for index_url in index_urls or []:
        discovered.extend(discover_from_index(index_url))
    for manual in manual_urls or []:
        normalized = normalize_url(manual)
        if _filter_url(normalized):
            discovered.append(normalized)
    return sorted(set(discovered))

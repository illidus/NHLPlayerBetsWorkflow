from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data" / "odds_archive"
RUN_LOGS_DIR = DATA_DIR / "run_logs"

URL_LAKE_PATH = DATA_DIR / "url_lake.parquet"
PAGES_PATH = DATA_DIR / "pages.parquet"
RAW_PROPS_PATH = DATA_DIR / "props_odds_raw.jsonl"
PROPS_PARQUET_PATH = DATA_DIR / "props_odds.parquet"

ODDS_ARCHIVE_DB_PATH = Path(
    os.getenv("ODDS_ARCHIVE_DB_PATH", REPO_ROOT / "data" / "db" / "nhl_backtest.duckdb")
)

USER_AGENT = os.getenv(
    "ODDS_ARCHIVE_USER_AGENT",
    "NHLPlayerBetsWorkflowOddsArchive/1.0 (+https://example.com)",
)
REQUEST_TIMEOUT = float(os.getenv("ODDS_ARCHIVE_REQUEST_TIMEOUT", "20"))
RATE_LIMIT_SECONDS = float(os.getenv("ODDS_ARCHIVE_RATE_LIMIT_SECONDS", "1.0"))
MAX_URLS_PER_RUN = int(os.getenv("ODDS_ARCHIVE_MAX_URLS", "500"))

ALLOWED_DOMAINS = [d.strip() for d in os.getenv("ODDS_ARCHIVE_ALLOWED_DOMAINS", "").split(",") if d.strip()]

DISCOVERY_SITEMAPS = [
    u.strip()
    for u in os.getenv("ODDS_ARCHIVE_SITEMAPS", "").split(",")
    if u.strip()
]
DISCOVERY_INDEX_URLS = [
    u.strip()
    for u in os.getenv("ODDS_ARCHIVE_INDEX_URLS", "").split(",")
    if u.strip()
]
DISCOVERY_MANUAL_URLS = [
    u.strip()
    for u in os.getenv("ODDS_ARCHIVE_MANUAL_URLS", "").split(",")
    if u.strip()
]

KNOWN_BOOKMAKERS = [
    "draftkings",
    "fanduel",
    "betmgm",
    "caesars",
    "pointsbet",
    "betrivers",
    "bet365",
    "fanatics",
    "circa",
    "barstool",
]

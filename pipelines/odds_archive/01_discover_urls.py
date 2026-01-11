from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from odds_archive import config, discovery, io
from odds_archive.utils import ensure_dirs, normalize_url, setup_logger, utcnow


def build_records(urls: list[str], method: str) -> pd.DataFrame:
    now = utcnow()
    records = []
    for url in urls:
        normalized = normalize_url(url)
        source = normalized.split("//", 1)[-1].split("/", 1)[0]
        records.append(
            {
                "url": normalized,
                "source": source,
                "discovery_method": method,
                "discovered_ts": now,
                "status": "new",
                "http_status": None,
                "canonical_url": None,
                "content_hash": None,
                "last_crawl_ts": None,
                "error": None,
            }
        )
    return pd.DataFrame(records)


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover NHL prop article URLs")
    parser.add_argument("--sitemap", action="append", default=[])
    parser.add_argument("--index", dest="index_url", action="append", default=[])
    parser.add_argument("--manual", action="append", default=[])
    args = parser.parse_args()

    ensure_dirs()
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_path = config.RUN_LOGS_DIR / f"discover_{run_ts}.log"
    logger = setup_logger("odds_archive.discover", str(log_path))

    sitemaps = config.DISCOVERY_SITEMAPS + args.sitemap
    index_urls = config.DISCOVERY_INDEX_URLS + args.index_url
    manual_urls = config.DISCOVERY_MANUAL_URLS + args.manual

    logger.info("Starting discovery with %s sitemaps, %s index urls", len(sitemaps), len(index_urls))

    sitemap_urls = discovery.discover_urls(sitemaps, [], [])
    index_urls_found = discovery.discover_urls([], index_urls, [])
    manual_urls_found = discovery.discover_urls([], [], manual_urls)

    discovered_urls = sorted(set(sitemap_urls + index_urls_found + manual_urls_found))
    logger.info("Discovered %s urls", len(discovered_urls))

    url_lake = io.load_url_lake()
    new_records = pd.concat(
        [
            build_records(sitemap_urls, "sitemap"),
            build_records(index_urls_found, "index"),
            build_records(manual_urls_found, "manual"),
        ],
        ignore_index=True,
    )
    combined = pd.concat([url_lake, new_records], ignore_index=True)
    combined = combined.drop_duplicates(subset=["url"], keep="first")
    io.save_url_lake(combined)
    io.write_duckdb_table("fact_odds_archive_url_lake", combined)

    summary = {
        "run_ts": run_ts,
        "discovered_count": len(discovered_urls),
        "url_lake_rows": len(combined),
    }
    summary_path = config.RUN_LOGS_DIR / f"discover_{run_ts}_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info("Discovery complete")


if __name__ == "__main__":
    main()

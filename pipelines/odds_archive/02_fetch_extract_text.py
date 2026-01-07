from __future__ import annotations

import argparse
import json
from datetime import datetime

import pandas as pd
import requests

from odds_archive import config, fetch_extract, io
from odds_archive.utils import RobotsCache, ensure_dirs, setup_logger, utcnow, sleep_with_rate_limit


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and extract text from discovered URLs")
    parser.add_argument("--limit", type=int, default=config.MAX_URLS_PER_RUN)
    args = parser.parse_args()

    ensure_dirs()
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_path = config.RUN_LOGS_DIR / f"fetch_{run_ts}.log"
    logger = setup_logger("odds_archive.fetch", str(log_path))

    url_lake = io.load_url_lake()
    if url_lake.empty:
        logger.info("No URLs to fetch")
        return

    candidates = url_lake[url_lake["status"].isin(["new", "error"])].copy()
    candidates = candidates.head(args.limit)

    robots_cache = RobotsCache(cache={})
    pages_records = []
    errors = 0
    last_ts = None

    for _, row in candidates.iterrows():
        last_ts = sleep_with_rate_limit(last_ts)
        url = row["url"]
        logger.info("Fetching %s", url)
        crawl_ts = utcnow()
        try:
            result = fetch_extract.fetch_page(url, robots_cache)
            pages_records.append(
                {
                    "url": url,
                    "canonical_url": result.get("canonical_url"),
                    "source": row["source"],
                    "title": result.get("title"),
                    "publish_ts": result.get("publish_ts"),
                    "updated_ts": result.get("updated_ts"),
                    "crawl_ts": crawl_ts,
                    "extracted_text": result.get("extracted_text"),
                    "html_snippet": result.get("html_snippet"),
                    "content_hash": result.get("content_hash"),
                    "parse_hint": result.get("parse_hint"),
                }
            )
            url_lake.loc[url_lake["url"] == url, ["status", "http_status", "canonical_url", "content_hash", "last_crawl_ts", "error"]] = [
                "fetched",
                result.get("http_status"),
                result.get("canonical_url"),
                result.get("content_hash"),
                crawl_ts,
                None,
            ]
        except PermissionError as exc:
            errors += 1
            url_lake.loc[url_lake["url"] == url, ["status", "error", "last_crawl_ts"]] = [
                "error",
                str(exc),
                crawl_ts,
            ]
        except requests.HTTPError as exc:
            errors += 1
            status_code = exc.response.status_code if exc.response is not None else None
            url_lake.loc[url_lake["url"] == url, ["status", "error", "last_crawl_ts", "http_status"]] = [
                "error",
                str(exc),
                crawl_ts,
                status_code,
            ]
        except Exception as exc:  # noqa: BLE001
            errors += 1
            url_lake.loc[url_lake["url"] == url, ["status", "error", "last_crawl_ts"]] = [
                "error",
                str(exc),
                crawl_ts,
            ]

    if pages_records:
        pages_df = pd.DataFrame(pages_records)
        combined_pages = pd.concat([io.load_pages(), pages_df], ignore_index=True)
        combined_pages = combined_pages.drop_duplicates(subset=["url", "content_hash"], keep="last")
        io.save_pages(combined_pages)
        io.write_duckdb_table("fact_odds_archive_pages", combined_pages)

    io.save_url_lake(url_lake)
    io.write_duckdb_table("fact_odds_archive_url_lake", url_lake)

    summary = {
        "run_ts": run_ts,
        "attempted": len(candidates),
        "fetched": len(pages_records),
        "errors": errors,
    }
    summary_path = config.RUN_LOGS_DIR / f"fetch_{run_ts}_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info("Fetch complete")


if __name__ == "__main__":
    main()

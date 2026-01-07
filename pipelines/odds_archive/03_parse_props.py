from __future__ import annotations

import argparse
import json
from datetime import datetime

import pandas as pd

from odds_archive import config, dedupe, io, parsers
from odds_archive.schema import RAW_PROPS_COLUMNS
from odds_archive.utils import ensure_dirs, setup_logger, utcnow


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse player props from extracted text")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    ensure_dirs()
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_path = config.RUN_LOGS_DIR / f"parse_{run_ts}.log"
    logger = setup_logger("odds_archive.parse", str(log_path))

    pages = io.load_pages()
    url_lake = io.load_url_lake()
    if pages.empty:
        logger.info("No pages to parse")
        return

    fetch_urls = url_lake[url_lake["status"] == "fetched"]["url"].tolist()
    pages = pages[pages["url"].isin(fetch_urls)]
    if args.limit:
        pages = pages.head(args.limit)

    registry = parsers.build_registry()
    existing_hashes = io.load_jsonl_hashes(config.RAW_PROPS_PATH)
    raw_records = []

    for _, page in pages.iterrows():
        text = page["extracted_text"] or ""
        candidates = registry.parse(text)
        logger.info("Parsed %s candidates from %s", len(candidates), page["url"])
        for candidate in candidates:
            record = {
                "source": page["source"],
                "source_url": page["url"],
                "canonical_url": page["canonical_url"],
                "publish_ts": page["publish_ts"],
                "updated_ts": page["updated_ts"],
                "crawl_ts": page["crawl_ts"],
                "game_date": None,
                "away_team": None,
                "home_team": None,
                "matchup_text_raw": None,
                "player_name_raw": candidate.player_name_raw,
                "player_name_clean": None,
                "player_team": None,
                "market_raw": candidate.market_raw,
                "market": candidate.to_record().get("market"),
                "line": candidate.line,
                "side": candidate.side,
                "odds": candidate.odds,
                "odds_format": candidate.odds_format,
                "bookmaker": candidate.bookmaker,
                "confidence": candidate.confidence,
                "parser": candidate.parser,
                "snippet": candidate.snippet,
                "content_hash": page["content_hash"],
            }
            record["record_hash"] = dedupe.build_record_hash(record)
            if record["record_hash"] not in existing_hashes:
                raw_records.append(record)

        url_lake.loc[url_lake["url"] == page["url"], "status"] = "parsed"

    if raw_records:
        raw_df = pd.DataFrame(raw_records).reindex(columns=RAW_PROPS_COLUMNS)
        io.append_jsonl(raw_df.to_dict(orient="records"), config.RAW_PROPS_PATH)

    io.save_url_lake(url_lake)
    io.write_duckdb_table("fact_odds_archive_url_lake", url_lake)

    summary = {
        "run_ts": run_ts,
        "pages_parsed": len(pages),
        "records": len(raw_records),
    }
    summary_path = config.RUN_LOGS_DIR / f"parse_{run_ts}_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info("Parse complete")


if __name__ == "__main__":
    main()

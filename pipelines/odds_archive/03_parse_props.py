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
    
    # Metrics
    total_blocks = 0
    accepted_blocks = 0
    rejected_blocks = 0
    rejected_by_reason = {} 
    # Reasons: PAGE_NON_NHL, BLOCK_NON_NHL, NO_CANDIDATE (implicit)

    rejected_pages = 0

    for _, page in pages.iterrows():
        text = page["extracted_text"] or ""
        blocks = text.split("\n")
        page_blocks_count = len(blocks)
        total_blocks += page_blocks_count

        # Page Scope Sport Check
        if not parsers.is_nhl_page(page["url"], page["title"]):
            rejected_pages += 1
            rejected_blocks += page_blocks_count
            rejected_by_reason["PAGE_NON_NHL"] = rejected_by_reason.get("PAGE_NON_NHL", 0) + page_blocks_count
            continue

        # Block Processing
        for block in blocks:
            block = block.strip()
            if not block:
                # Empty blocks don't count as rejected content really, but for denominator consistency?
                # I'll just skip and not count as "rejected sport"
                continue
            
            # Block Scope Sport Check (Double check for mixed pages if page passed?)
            # Prompt: "Apply sport classification at PAGE scope OR ensure blocks inherit page sport."
            # If I trust Page Scope, I assume all blocks are NHL-ish.
            # But let's be safe and check block too if Page passed (e.g. sidebar links).
            if not parsers.is_nhl_block(block):
                rejected_blocks += 1
                rejected_by_reason["BLOCK_NON_NHL"] = rejected_by_reason.get("BLOCK_NON_NHL", 0) + 1
                continue
                
            accepted_blocks += 1
            candidates = registry.parse(block)
            
            for candidate in candidates:
                # Check status code
                if candidate.status_code != "CANDIDATE_READY":
                     rejected_by_reason[candidate.status_code] = rejected_by_reason.get(candidate.status_code, 0) + 1

                record = {
                    "source": page["source"],
                    "source_url": page["url"],
                    "canonical_url": page["canonical_url"],
                    "publish_ts": page["publish_ts"],
                    "updated_ts": page["updated_ts"],
                    "crawl_ts": page["crawl_ts"],
                    "game_date": candidate.derived_game_date,
                    "away_team": None,
                    "home_team": None,
                    "matchup_text_raw": None, # Will be populated if GAME
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
                    "status_code": candidate.status_code,
                    "rejection_reason": candidate.rejection_reason,
                    "entity_type": candidate.entity_type,
                }
                
                # Handle GAME entity map
                if candidate.entity_type == "GAME":
                    record["matchup_text_raw"] = candidate.player_name_raw
                    record["player_name_raw"] = None

                record["record_hash"] = dedupe.build_record_hash(record)
                if record["record_hash"] not in existing_hashes:
                    raw_records.append(record)

        url_lake.loc[url_lake["url"] == page["url"], "status"] = "parsed"

    if raw_records:
        raw_df = pd.DataFrame(raw_records).reindex(columns=RAW_PROPS_COLUMNS)
        io.append_jsonl(raw_df.to_dict(orient="records"), config.RAW_PROPS_PATH)

    io.save_url_lake(url_lake)

    summary = {
        "run_ts": run_ts,
        "pages_parsed": len(pages),
        "total_blocks": total_blocks,
        "accepted_blocks": accepted_blocks,
        "rejected_blocks": rejected_blocks,
        "rejected_by_reason": rejected_by_reason,
        "records_generated": len(raw_records),
    }
    summary_path = config.RUN_LOGS_DIR / f"parse_{run_ts}_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info("Parse complete")


if __name__ == "__main__":
    main()
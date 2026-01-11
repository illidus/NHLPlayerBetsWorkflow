from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from odds_archive import config, dedupe, io, normalize
from odds_archive.schema import NORMALIZED_PROPS_COLUMNS, RAW_PROPS_COLUMNS
from odds_archive.utils import ensure_dirs, setup_logger


def _read_raw(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=RAW_PROPS_COLUMNS)
    return pd.read_json(path, lines=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize and dedupe raw prop records")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    ensure_dirs()
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_path = config.RUN_LOGS_DIR / f"normalize_{run_ts}.log"
    logger = setup_logger("odds_archive.normalize", str(log_path))

    raw_df = _read_raw(config.RAW_PROPS_PATH)
    if raw_df.empty:
        logger.info("No raw props to normalize")
        return

    if args.limit:
        raw_df = raw_df.head(args.limit)

    raw_df["player_name_clean"] = raw_df["player_name_clean"].fillna("").apply(normalize.clean_player_name)
    raw_df["market"] = raw_df["market"].fillna("").apply(normalize.normalize_market)
    raw_df["side"] = raw_df["side"].fillna("").apply(normalize.normalize_side)
    raw_df["bookmaker"] = raw_df["bookmaker"].fillna("").apply(normalize.normalize_bookmaker)

    normalized_records = []
    for _, record in raw_df.iterrows():
        record_dict = record.to_dict()
        record_dict["player_name_clean"] = normalize.clean_player_name(record_dict.get("player_name_raw"))
        record_dict["market"] = normalize.normalize_market(record_dict.get("market_raw")) or record_dict.get("market")
        record_dict["side"] = normalize.normalize_side(record_dict.get("side"))
        record_dict["bookmaker"] = normalize.normalize_bookmaker(record_dict.get("bookmaker"))
        record_dict["record_hash"] = dedupe.build_record_hash(record_dict)
        # Preserve new fields
        record_dict["status_code"] = record_dict.get("status_code", "CANDIDATE_READY")
        record_dict["rejection_reason"] = record_dict.get("rejection_reason")
        
        normalized_records.append(record_dict)

    normalized_df = pd.DataFrame(normalized_records)
    normalized_df = normalized_df.drop_duplicates(subset=["record_hash"], keep="last")
    normalized_df = normalized_df.reindex(columns=NORMALIZED_PROPS_COLUMNS)

    # --- Tier 2 Transformation ---
    tier2_records = []
    for _, row in normalized_df.iterrows():
         props = {
             "player_name": row["player_name_clean"] or row["player_name_raw"],
             "market": row["market"],
             "line": float(row["line"]) if pd.notna(row["line"]) else None,
             "side": row["side"],
             "odds": int(row["odds"]) if pd.notna(row["odds"]) else None,
             "bookmaker": row["bookmaker"]
         }
         
         meta = {
             "url": row["source_url"],
             "source": row["source"],
             "publish_ts": row["publish_ts"],
             "crawl_ts": row["crawl_ts"]
         }

         tier2_records.append({
             "mention_id": row["record_hash"],
             "raw_text_snippet": row["snippet"],
             "extracted_props": json.dumps(props, default=str),
             "derived_game_date": row["game_date"],
             "confidence_score": row["confidence"],
             "status_code": row["status_code"],
             "rejection_reason": row["rejection_reason"],
             "metadata": json.dumps(meta, default=str),
             "ingest_ts_utc": datetime.utcnow()
         })
    
    tier2_df = pd.DataFrame(tier2_records)

    # --- Routing ---
    # Write to raw_editorial_mentions (Tier 2)
    io.write_duckdb_table("raw_editorial_mentions", tier2_df)
    
    # Save parquet backup
    io.write_parquet(tier2_df, config.DATA_DIR / "editorial_mentions.parquet")

    # STOP writing to fact_odds_archive_props (Legacy)
    # io.write_duckdb_table("fact_odds_archive_props", normalized_df) 
    # io.write_parquet(normalized_df, config.PROPS_PARQUET_PATH)

    summary = {
        "run_ts": run_ts,
        "raw_records": len(raw_df),
        "normalized_records": len(normalized_df),
        "tier2_records": len(tier2_df),
    }
    summary_path = config.RUN_LOGS_DIR / f"normalize_{run_ts}_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info("Normalize complete")


if __name__ == "__main__":
    main()

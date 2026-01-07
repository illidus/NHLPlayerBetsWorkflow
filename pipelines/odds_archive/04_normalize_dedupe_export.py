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
        normalized_records.append(record_dict)

    normalized_df = pd.DataFrame(normalized_records)
    normalized_df = normalized_df.drop_duplicates(subset=["record_hash"], keep="last")
    normalized_df = normalized_df.reindex(columns=NORMALIZED_PROPS_COLUMNS)

    io.write_parquet(normalized_df, config.PROPS_PARQUET_PATH)
    io.write_duckdb_table("fact_odds_archive_props", normalized_df)

    summary = {
        "run_ts": run_ts,
        "raw_records": len(raw_df),
        "normalized_records": len(normalized_df),
    }
    summary_path = config.RUN_LOGS_DIR / f"normalize_{run_ts}_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info("Normalize complete")


if __name__ == "__main__":
    main()

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import duckdb
import pandas as pd

# Ensure project root is in path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
src_dir = os.path.join(project_root, "src")
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from nhl_bets.scrapers.unabated_client import UnabatedClient
from nhl_bets.common.db_init import initialize_phase11_tables, insert_odds_records, get_db_connection

DB_PATH = "data/db/nhl_backtest.duckdb"
RAW_ROOT = os.path.join("outputs", "odds", "raw", "UNABATED")


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def _date_range(start: datetime, end: datetime):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def _has_snapshot_for_date(date_obj: datetime) -> bool:
    dir_path = os.path.join(RAW_ROOT, date_obj.strftime("%Y"), date_obj.strftime("%m"), date_obj.strftime("%d"))
    if not os.path.isdir(dir_path):
        return False
    for name in os.listdir(dir_path):
        if name.endswith(".json") and not name.endswith(".sha256"):
            return True
    return False


def _save_raw_payload_for_date(payload: dict, target_date: datetime):
    date_path = target_date.strftime("%Y/%m/%d")
    time_prefix = datetime.now(timezone.utc).strftime("%H%M%S")
    filename = f"{time_prefix}_unabated.json"
    dir_path = os.path.join(RAW_ROOT, date_path)
    os.makedirs(dir_path, exist_ok=True)

    full_path = os.path.join(dir_path, filename)
    rel_path = os.path.relpath(full_path, start=os.getcwd())

    content = json.dumps(payload, indent=2)
    sha_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

    with open(full_path, "w", encoding="utf-8") as handle:
        handle.write(content)
    with open(f"{full_path}.sha256", "w") as handle:
        handle.write(sha_hash)

    capture_ts = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
    return rel_path, sha_hash, capture_ts


def _is_payload_ingested(con: duckdb.DuckDBPyConnection, payload_hash: str) -> bool:
    res = con.execute("SELECT count(*) FROM raw_odds_payloads WHERE payload_hash = ?", [payload_hash]).fetchone()
    return res[0] > 0


def _register_payload(con: duckdb.DuckDBPyConnection, capture_ts: datetime, rel_path: str, payload_hash: str):
    con.execute(
        "INSERT INTO raw_odds_payloads (payload_hash, source_vendor, capture_ts_utc, file_path) VALUES (?, ?, ?, ?)",
        [payload_hash, "UNABATED", capture_ts, rel_path],
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Bounded backfill for Unabated snapshots.")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD).")
    parser.add_argument("--max-requests", type=int, default=10, help="Maximum number of snapshot requests.")
    parser.add_argument("--max-elapsed-seconds", type=int, default=300, help="Maximum elapsed seconds.")
    parser.add_argument("--dry-run", action="store_true", help="Print plan only, do not fetch.")

    args = parser.parse_args()

    start_date = _parse_date(args.start_date)
    end_date = _parse_date(args.end_date)
    if end_date < start_date:
        raise ValueError("end-date must be on or after start-date")

    plan = []
    for date_obj in _date_range(start_date, end_date):
        if _has_snapshot_for_date(date_obj):
            plan.append((date_obj.date().isoformat(), "skip"))
        else:
            plan.append((date_obj.date().isoformat(), "fetch"))

    if args.dry_run:
        print("Backfill plan:")
        for date_str, action in plan:
            print(f"- {date_str}: {action}")
        return 0

    client = UnabatedClient()
    con = get_db_connection(DB_PATH)
    try:
        initialize_phase11_tables(con)
        request_count = 0
        start_time = time.time()

        for date_str, action in plan:
            if action != "fetch":
                continue
            if request_count >= args.max_requests:
                print("Max requests reached; stopping.")
                break
            if (time.time() - start_time) >= args.max_elapsed_seconds:
                print("Max elapsed time reached; stopping.")
                break

            snapshot = client.fetch_snapshot()
            rel_path, sha_hash, capture_ts = _save_raw_payload_for_date(snapshot, _parse_date(date_str))

            if _is_payload_ingested(con, sha_hash):
                print(f"{date_str}: payload already ingested, skipping.")
                continue

            records = client.parse_snapshot(snapshot, rel_path, sha_hash, capture_ts)
            if records:
                df = pd.DataFrame(records)
                insert_odds_records(con, df)
                _register_payload(con, capture_ts, rel_path, sha_hash)
                print(f"{date_str}: inserted {len(records)} records.")
            else:
                print(f"{date_str}: no records parsed.")

            request_count += 1
    finally:
        con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

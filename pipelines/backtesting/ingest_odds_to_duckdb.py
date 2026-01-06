import os
import sys
import json
import logging
import duckdb
import pandas as pd
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

# Ensure project root is in path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
src_dir = os.path.join(project_root, "src")
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from nhl_bets.scrapers.unabated_client import UnabatedClient
from nhl_bets.scrapers.oddsshark_client import OddsSharkClient
from nhl_bets.scrapers.playnow_api_client import PlayNowAPIClient
from nhl_bets.scrapers.playnow_adapter import PlayNowAdapter
from nhl_bets.common.db_init import initialize_phase11_tables, insert_odds_records
from nhl_bets.common.storage import save_raw_payload
from nhl_bets.common.vendor_utils import VendorRequestError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ingest_odds.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("ingest_odds_pipeline")

from nhl_bets.common.db_init import DEFAULT_DB_PATH

DB_PATH = DEFAULT_DB_PATH
STATUS_PATH_ENV = "INGEST_STATUS_PATH"
FAIL_FAST_ENV = "FAIL_FAST"

def _init_status():
    return {
        "start_ts_utc": datetime.now(timezone.utc).isoformat(),
        "end_ts_utc": None,
        "vendors": {}
    }

def _write_status(status: Dict[str, Any], path: Optional[str]):
    if not path:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(status, handle, indent=2)

def is_payload_ingested(con: duckdb.DuckDBPyConnection, payload_hash: str) -> bool:
    res = con.execute("SELECT count(*) FROM raw_odds_payloads WHERE payload_hash = ?", [payload_hash]).fetchone()
    return res[0] > 0

def register_payload(con: duckdb.DuckDBPyConnection, vendor: str, capture_ts: datetime, rel_path: str, payload_hash: str):
    con.execute("INSERT INTO raw_odds_payloads (payload_hash, source_vendor, capture_ts_utc, file_path) VALUES (?, ?, ?, ?)", 
                [payload_hash, vendor, capture_ts, rel_path])

def run_unabated_ingestion(con: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    logger.info("Starting UNABATED ingestion...")
    client = UnabatedClient()
    status = {"status": "PASS", "records": 0, "error_type": None, "error": None}
    try:
        snapshot = client.fetch_snapshot()
        rel_path, sha_hash, capture_ts = save_raw_payload("UNABATED", snapshot, "json")
        
        if is_payload_ingested(con, sha_hash):
            logger.info(f"UNABATED: Snapshot with hash {sha_hash} already ingested. Skipping.")
            return status
            
        records = client.parse_snapshot(snapshot, rel_path, sha_hash, capture_ts)
        if records:
            df = pd.DataFrame(records)
            insert_odds_records(con, df)
            register_payload(con, "UNABATED", capture_ts, rel_path, sha_hash)
            status["records"] = len(records)
            logger.info(f"UNABATED: Inserted {len(records)} records.")
    except VendorRequestError as e:
        status["status"] = "FAIL"
        status["error_type"] = "vendor"
        status["error"] = str(e)
        logger.error(f"UNABATED ingestion failed: {e}", exc_info=True)
    except Exception as e:
        status["status"] = "FAIL"
        status["error_type"] = "core"
        status["error"] = str(e)
        logger.error(f"UNABATED ingestion failed: {e}", exc_info=True)
        raise
    return status

def run_oddsshark_ingestion(con: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    logger.info("Starting ODDSSHARK ingestion...")
    client = OddsSharkClient()
    status = {"status": "PASS", "records": 0, "error_type": None, "error": None}
    try:
        html = client.fetch_snapshot()
        rel_path, sha_hash, capture_ts = save_raw_payload("ODDSSHARK", html, "html")
        
        if is_payload_ingested(con, sha_hash):
            logger.info(f"ODDSSHARK: Snapshot with hash {sha_hash} already ingested. Skipping.")
            return status
            
        records = client.parse_snapshot(html, rel_path, sha_hash, capture_ts)
        if records:
            df = pd.DataFrame(records)
            insert_odds_records(con, df)
            register_payload(con, "ODDSSHARK", capture_ts, rel_path, sha_hash)
            status["records"] = len(records)
            logger.info(f"ODDSSHARK: Inserted {len(records)} records.")
    except VendorRequestError as e:
        status["status"] = "FAIL"
        status["error_type"] = "vendor"
        status["error"] = str(e)
        logger.error(f"ODDSSHARK ingestion failed: {e}", exc_info=True)
    except Exception as e:
        status["status"] = "FAIL"
        status["error_type"] = "core"
        status["error"] = str(e)
        logger.error(f"ODDSSHARK ingestion failed: {e}", exc_info=True)
        raise
    return status

def run_playnow_ingestion(con: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    logger.info("Starting PLAYNOW ingestion...")
    client = PlayNowAPIClient()
    adapter = PlayNowAdapter()
    status = {"status": "PASS", "records": 0, "error_type": None, "error": None}
    try:
        # Fetch event list
        logger.info("Fetching PlayNow event list...")
        url_list, data_list = client.fetch_event_list(event_sorts="MTCH,TNMT")
        
        # Save raw event list (we don't track this hash for props ingestion deduplication, 
        # as the detailed props are what matters)
        save_raw_payload("PLAYNOW", data_list, "json", suffix="event_list")
        
        events = data_list.get('data', {}).get('events', [])
        event_ids = [e['id'] for e in events if e.get('marketCount', 0) > 5]
        
        if not event_ids:
            logger.warning("PLAYNOW: No events with props found.")
            return status

        # Fetch detailed props
        logger.info(f"Fetching PlayNow details for {len(event_ids)} events...")
        url_det, data_det = client.fetch_event_details(event_ids)
        
        # Save raw details
        rel_path, sha_hash, capture_ts = save_raw_payload("PLAYNOW", data_det, "json", suffix="details")
        
        if is_payload_ingested(con, sha_hash):
            logger.info(f"PLAYNOW: Snapshot with hash {sha_hash} already ingested. Skipping.")
            return status
            
        # Parse and insert
        records = adapter.parse_event_details(data_det, rel_path, sha_hash, capture_ts)
        if records:
            df = pd.DataFrame(records)
            insert_odds_records(con, df)
            register_payload(con, "PLAYNOW", capture_ts, rel_path, sha_hash)
            status["records"] = len(records)
            logger.info(f"PLAYNOW: Inserted {len(records)} records.")
            
    except VendorRequestError as e:
        status["status"] = "FAIL"
        status["error_type"] = "vendor"
        status["error"] = str(e)
        logger.error(f"PLAYNOW ingestion failed: {e}", exc_info=True)
    except Exception as e:
        status["status"] = "FAIL"
        status["error_type"] = "core"
        status["error"] = str(e)
        logger.error(f"PLAYNOW ingestion failed: {e}", exc_info=True)
        raise
    return status

def main():
    logger.info("Initializing Phase 11 Odds Ingestion Pipeline")
    
    status_payload = _init_status()
    status_path = os.environ.get(STATUS_PATH_ENV)
    fail_fast = os.environ.get(FAIL_FAST_ENV, "0") == "1"

    con = duckdb.connect(DB_PATH)
    try:
        # Connect and set pragmas
        con.execute("SET memory_limit = '8GB';")
        con.execute("SET threads = 8;")
        con.execute("SET temp_directory = './duckdb_temp/';")
        con.execute("SET TimeZone = 'UTC';")
        
        # Initialize schema
        initialize_phase11_tables(con)
        
        # Run vendors
        status_payload["vendors"]["UNABATED"] = run_unabated_ingestion(con)
        status_payload["vendors"]["PLAYNOW"] = run_playnow_ingestion(con)
        status_payload["vendors"]["ODDSSHARK"] = run_oddsshark_ingestion(con)

        vendor_failures = [
            name for name, info in status_payload["vendors"].items()
            if info.get("status") == "FAIL" and info.get("error_type") == "vendor"
        ]
        if vendor_failures and fail_fast:
            raise RuntimeError(f"Vendor failures (fail-fast enabled): {', '.join(vendor_failures)}")
        
        logger.info("Odds ingestion pipeline completed.")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        status_payload["end_ts_utc"] = datetime.now(timezone.utc).isoformat()
        _write_status(status_payload, status_path)
        con.close()

if __name__ == "__main__":
    main()

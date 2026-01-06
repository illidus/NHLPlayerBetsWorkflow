import os
import sys
import logging
import duckdb
import pandas as pd
from datetime import datetime, timezone
from typing import List, Dict, Any

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

DB_PATH = 'data/db/nhl_backtest.duckdb'

def is_payload_ingested(con: duckdb.DuckDBPyConnection, payload_hash: str) -> bool:
    res = con.execute("SELECT count(*) FROM raw_odds_payloads WHERE payload_hash = ?", [payload_hash]).fetchone()
    return res[0] > 0

def register_payload(con: duckdb.DuckDBPyConnection, vendor: str, capture_ts: datetime, rel_path: str, payload_hash: str):
    con.execute("INSERT INTO raw_odds_payloads (payload_hash, source_vendor, capture_ts_utc, file_path) VALUES (?, ?, ?, ?)", 
                [payload_hash, vendor, capture_ts, rel_path])

def run_unabated_ingestion(con: duckdb.DuckDBPyConnection):
    logger.info("Starting UNABATED ingestion...")
    client = UnabatedClient()
    try:
        snapshot = client.fetch_snapshot()
        rel_path, sha_hash, capture_ts = save_raw_payload("UNABATED", snapshot, "json")
        
        if is_payload_ingested(con, sha_hash):
            logger.info(f"UNABATED: Snapshot with hash {sha_hash} already ingested. Skipping.")
            return
            
        records = client.parse_snapshot(snapshot, rel_path, sha_hash, capture_ts)
        if records:
            df = pd.DataFrame(records)
            insert_odds_records(con, df)
            register_payload(con, "UNABATED", capture_ts, rel_path, sha_hash)
            logger.info(f"UNABATED: Inserted {len(records)} records.")
    except Exception as e:
        logger.error(f"UNABATED ingestion failed: {e}", exc_info=True)

def run_oddsshark_ingestion(con: duckdb.DuckDBPyConnection):
    logger.info("Starting ODDSSHARK ingestion...")
    client = OddsSharkClient()
    try:
        html = client.fetch_snapshot()
        rel_path, sha_hash, capture_ts = save_raw_payload("ODDSSHARK", html, "html")
        
        if is_payload_ingested(con, sha_hash):
            logger.info(f"ODDSSHARK: Snapshot with hash {sha_hash} already ingested. Skipping.")
            return
            
        records = client.parse_snapshot(html, rel_path, sha_hash, capture_ts)
        if records:
            df = pd.DataFrame(records)
            insert_odds_records(con, df)
            register_payload(con, "ODDSSHARK", capture_ts, rel_path, sha_hash)
            logger.info(f"ODDSSHARK: Inserted {len(records)} records.")
    except Exception as e:
        logger.error(f"ODDSSHARK ingestion failed: {e}", exc_info=True)

def run_playnow_ingestion(con: duckdb.DuckDBPyConnection):
    logger.info("Starting PLAYNOW ingestion...")
    client = PlayNowAPIClient()
    adapter = PlayNowAdapter()
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
            return

        # Fetch detailed props
        logger.info(f"Fetching PlayNow details for {len(event_ids)} events...")
        url_det, data_det = client.fetch_event_details(event_ids)
        
        # Save raw details
        rel_path, sha_hash, capture_ts = save_raw_payload("PLAYNOW", data_det, "json", suffix="details")
        
        if is_payload_ingested(con, sha_hash):
            logger.info(f"PLAYNOW: Snapshot with hash {sha_hash} already ingested. Skipping.")
            return
            
        # Parse and insert
        records = adapter.parse_event_details(data_det, rel_path, sha_hash, capture_ts)
        if records:
            df = pd.DataFrame(records)
            insert_odds_records(con, df)
            register_payload(con, "PLAYNOW", capture_ts, rel_path, sha_hash)
            logger.info(f"PLAYNOW: Inserted {len(records)} records.")
            
    except Exception as e:
        logger.error(f"PLAYNOW ingestion failed: {e}", exc_info=True)

def main():
    logger.info("Initializing Phase 11 Odds Ingestion Pipeline")
    
    con = duckdb.connect(DB_PATH)
    try:
        # Connect and set pragmas
        con.execute("SET memory_limit = '8GB';")
        con.execute("SET threads = 8;")
        con.execute("SET temp_directory = './duckdb_temp/';")
        
        # Initialize schema
        initialize_phase11_tables(con)
        
        # Run vendors
        run_unabated_ingestion(con)
        run_playnow_ingestion(con)
        run_oddsshark_ingestion(con)
        
        logger.info("Odds ingestion pipeline completed.")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
    finally:
        con.close()

if __name__ == "__main__":
    main()
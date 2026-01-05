import os
import sys
import time
import argparse
import requests
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://moneypuck.com/playerData"
SEASON_SUMMARY_BASE = f"{BASE_URL}/seasonSummary"
LOOKUP_URL = f"{BASE_URL}/playerBios/allPlayersLookup.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def download_file(url, target_path, force=False):
    """
    Download a file from a URL to target_path. 
    If not force, it compares Content-Length if available to skip.
    """
    target_file = Path(target_path)
    target_file.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # 1. Use HEAD request to check size/existence
        head = requests.head(url, headers=HEADERS, timeout=10)
        if head.status_code == 403:
            logger.warning(f"Upstream returned 403; downloader skipped; ingestion may proceed using existing local data. URL: {url}")
            return "skipped"
            
        if head.status_code != 200:
            # Try GET if HEAD is not supported properly
            resp = requests.get(url, headers=HEADERS, stream=True, timeout=10)
            if resp.status_code == 403:
                logger.warning(f"Upstream returned 403; downloader skipped; ingestion may proceed using existing local data. URL: {url}")
                return "skipped"
            if resp.status_code != 200:
                logger.error(f"File not found on server ({resp.status_code}): {url}")
                return "failed"
        else:
            remote_size = int(head.headers.get('Content-Length', 0))
            if not force and target_file.exists() and target_file.stat().st_size == remote_size:
                return "skipped"
            resp = requests.get(url, headers=HEADERS, stream=True, timeout=30)

        # 2. Download and save
        with open(target_file, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return "downloaded"
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        return "failed"

def main():
    parser = argparse.ArgumentParser(description="Download MoneyPuck NHL Season Summary data.")
    parser.add_argument("--start-season", type=int, default=2018)
    parser.add_argument("--end-season", type=int, default=2025)
    parser.add_argument("--season-type", type=str, default="regular")
    parser.add_argument("--force", action="store_true")
    
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent
    data_root = project_root / "data" / "raw" / "moneypuck"
    
    args = parser.parse_args()
    
    logger.info(f"Starting optimized download to {data_root}")
    
    # 1. Download Lookup
    status = download_file(LOOKUP_URL, data_root / "allPlayersLookup.csv", force=args.force)
    logger.info(f"Lookup file: {status}")
    
    # 2. Download Season Summaries (Faster than game-by-game for live bridge)
    # Note: ingest_moneypuck_to_duckdb.py currently expects individual game files.
    # However, the user asked for a "downloader". 
    # If I use season summaries, I need to update the ingestor too.
    # Let's stick to individual games if that's what the ingestor wants, 
    # OR provide both and update the ingestor.
    
    # For now, let's keep the game-by-game logic but make it more robust.
    # MoneyPuck's directory listing can be finicky.
    
    GAME_BY_GAME_BASE = f"{BASE_URL}/teamPlayerGameByGame"
    
    import re
    def get_csv_links(index_url):
        try:
            response = requests.get(index_url, headers=HEADERS, timeout=30)
            if response.status_code == 403:
                logger.warning(f"Upstream returned 403; downloader skipped; ingestion may proceed using existing local data. URL: {index_url}")
                return []
            response.raise_for_status()
            links = re.findall(r'href=["\\]?([^"\">]+\.csv)["\\]?', response.text)
            return list(set(links))
        except Exception as e:
            logger.error(f"Error fetching index {index_url}: {e}")
            return []

    groups = ["skaters", "goalies"]
    seasons = range(args.start_season, args.end_season + 1)
    
    overall_failed = False
    for season in seasons:
        for group in groups:
            rel_path = f"teamPlayerGameByGame/{season}/{args.season_type}/{group}"
            target_dir = data_root / rel_path
            index_url = f"{GAME_BY_GAME_BASE}/{season}/{args.season_type}/{group}/"
            
            logger.info(f"Syncing {season} {group}...")
            csv_files = get_csv_links(index_url)
            
            if not csv_files:
                continue
                
            stats = {"downloaded": 0, "skipped": 0, "failed": 0}
            for csv_file in csv_files:
                res = download_file(f"{index_url}{csv_file}", target_dir / csv_file, force=args.force)
                stats[res] += 1
            
            if stats['failed'] > 0:
                overall_failed = True
            
            if stats['downloaded'] > 0:
                logger.info(f"  Result: {stats['downloaded']} new, {stats['skipped']} skipped.")
            else:
                logger.info(f"  Result: Up to date.")
    
    if overall_failed and args.force:
        logger.error("Fresh download was requested (--force) but some files failed to download.")
        sys.exit(1)

if __name__ == "__main__":
    main()

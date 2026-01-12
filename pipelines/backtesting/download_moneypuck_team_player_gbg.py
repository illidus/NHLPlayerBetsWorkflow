import os
import sys
import time
import argparse
import requests
import json
import logging
from pathlib import Path
from datetime import datetime

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

# Config
REFRESH_MODE = os.getenv("MPUCK_REFRESH_MODE", "best_effort").lower() # best_effort, required
CACHE_POLICY = os.getenv("MPUCK_CACHE_POLICY", "prefer_cache").lower() # prefer_cache, force_refresh

def get_manifest_path(data_root):
    return data_root / "_manifest.json"

def save_manifest(data_root, stats):
    manifest_path = get_manifest_path(data_root)
    manifest = {
        "timestamp": datetime.now().isoformat(),
        "stats": stats,
        "status": "valid"
    }
    try:
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        logger.info(f"Updated cache manifest at {manifest_path}")
    except Exception as e:
        logger.warning(f"Failed to write manifest: {e}")

def validate_cache(target_path):
    """Returns True if the file exists and has content."""
    p = Path(target_path)
    return p.exists() and p.stat().st_size > 0

def download_file(url, target_path, force=False):
    """
    Download a file from a URL to target_path. 
    Returns: "downloaded", "skipped", "failed", "served_from_cache"
    """
    target_file = Path(target_path)
    target_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Check cache first if not forcing
    if not force and validate_cache(target_file):
        # We still want to check for updates unless we are in a strict "offline" mode (not implemented),
        # but the prompt implies 'prefer_cache' means 'use cache if valid and don't aggressively refresh'?
        # Actually standard HTTP caching checks ETag/Content-Length.
        # However, to avoid 403s on HEAD requests, we might want to skip the network entirely 
        # if the policy implies it. But 'prefer_cache' usually implies 'conditional get'.
        # Given the 403 issue, let's try HEAD. If 403, we fall back to cache.
        pass

    try:
        # 1. Use HEAD request to check size/existence
        try:
            head = requests.head(url, headers=HEADERS, timeout=10)
        except requests.RequestException as e:
            logger.warning(f"Network error on HEAD {url}: {e}")
            head = None

        if head and head.status_code == 403:
            logger.warning(f"Upstream returned 403 (Forbidden) for {url}")
            if validate_cache(target_file):
                logger.info(f"  -> Using cached version for {target_file.name}")
                return "served_from_cache"
            else:
                if REFRESH_MODE == "required":
                    logger.error(f"  -> Cache missing and refresh required. Fatal.")
                    return "failed"
                else:
                    logger.warning(f"  -> Cache missing but best_effort mode. Skipping.")
                    return "skipped"
            
        if head and head.status_code != 200:
            # Try GET if HEAD is not supported properly or other error
            # But if it was 404, we shouldn't retry?
            # Sticking to original logic's robustness
            try:
                resp = requests.get(url, headers=HEADERS, stream=True, timeout=10)
            except requests.RequestException:
                resp = None

            if resp and resp.status_code == 403:
                logger.warning(f"Upstream returned 403 (Forbidden) on GET {url}")
                if validate_cache(target_file):
                    return "served_from_cache"
                return "failed" if REFRESH_MODE == "required" else "skipped"

            if not resp or resp.status_code != 200:
                code = resp.status_code if resp else "ConnectionError"
                logger.error(f"File not found or error ({code}): {url}")
                return "failed"
        else:
            # HEAD 200 OK
            if head:
                remote_size = int(head.headers.get('Content-Length', 0))
                if not force and validate_cache(target_file) and target_file.stat().st_size == remote_size:
                    return "skipped" # Up to date
            
            # Perform Download
            resp = requests.get(url, headers=HEADERS, stream=True, timeout=30)

        if resp.status_code == 403:
             # Just in case GET fails after HEAD succeeded (unlikely but possible)
             if validate_cache(target_file): return "served_from_cache"
             return "failed"

        # 2. Download and save
        with open(target_file, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return "downloaded"
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        if validate_cache(target_file):
            logger.info("Falling back to cache on exception.")
            return "served_from_cache"
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
    
    # Merge env var policy with arg
    force = args.force or (CACHE_POLICY == "force_refresh")
    
    logger.info(f"Starting download to {data_root}")
    logger.info(f"Mode: {REFRESH_MODE}, Policy: {CACHE_POLICY}, Force: {force}")
    
    global_stats = {"downloaded": 0, "skipped": 0, "failed": 0, "served_from_cache": 0}

    # 1. Download Lookup
    status = download_file(LOOKUP_URL, data_root / "allPlayersLookup.csv", force=force)
    global_stats[status] += 1
    logger.info(f"Lookup file: {status}")
    
    GAME_BY_GAME_BASE = f"{BASE_URL}/teamPlayerGameByGame"
    
    import re
    def get_csv_links(index_url):
        try:
            response = requests.get(index_url, headers=HEADERS, timeout=30)
            if response.status_code == 403:
                logger.warning(f"Upstream 403 on index: {index_url}")
                return "403"
            response.raise_for_status()
            links = re.findall(r'href=["\\]?([^"">]+\.csv)["\\]?', response.text)
            return list(set(links))
        except Exception as e:
            logger.error(f"Error fetching index {index_url}: {e}")
            return None

    groups = ["skaters", "goalies"]
    seasons = range(args.start_season, args.end_season + 1)
    
    for season in seasons:
        for group in groups:
            rel_path = f"teamPlayerGameByGame/{season}/{args.season_type}/{group}"
            target_dir = data_root / rel_path
            index_url = f"{GAME_BY_GAME_BASE}/{season}/{args.season_type}/{group}/"
            
            logger.info(f"Syncing {season} {group}...")
            csv_files = get_csv_links(index_url)
            
            # Handle Index Failures
            if csv_files == "403":
                # If we can't get the index, we can't know what new files there are.
                # However, we might rely on what we already have in the folder?
                # The user requirement says "warn and continue using cache".
                # For `teamPlayerGameByGame`, the cache is the directory contents.
                # We can't discover NEW files, but we should acknowledge the existing ones.
                if target_dir.exists():
                     cached_files = [f.name for f in target_dir.glob("*.csv")]
                     count = len(cached_files)
                     logger.warning(f"  -> Index blocked. Retaining {count} cached files.")
                     global_stats["served_from_cache"] += count
                else:
                    if REFRESH_MODE == "required":
                        logger.error("  -> Index blocked and no local cache. Fatal.")
                        global_stats["failed"] += 1
                    else:
                        logger.warning("  -> Index blocked and no cache. Skipping.")
                        global_stats["skipped"] += 1
                continue

            if not csv_files:
                logger.warning(f"  -> No files found or error (non-403) for {index_url}")
                continue
                
            stats = {"downloaded": 0, "skipped": 0, "failed": 0, "served_from_cache": 0}
            for csv_file in csv_files:
                res = download_file(f"{index_url}{csv_file}", target_dir / csv_file, force=force)
                stats[res] += 1
                global_stats[res] += 1
            
            logger.info(f"  Result: {stats['downloaded']} new, {stats['served_from_cache']} cached, {stats['skipped']} skipped.")
    
    # Final Report
    logger.info("-" * 40)
    logger.info(f"Download Summary: {global_stats}")
    
    save_manifest(data_root, global_stats)

    if global_stats['failed'] > 0:
        if REFRESH_MODE == "required":
            logger.error("Failures occurred in required mode.")
            sys.exit(1)
        else:
            logger.warning("Some files failed to download, but proceeding in best_effort mode.")

if __name__ == "__main__":
    main()
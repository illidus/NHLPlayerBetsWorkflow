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
# Updated default base URL to the working one
DEFAULT_BASE_URL = os.getenv("MONEYPUCK_BASE_URL", "https://moneypuck.com/moneypuck/playerData")
FALLBACK_BASE_URL = "https://moneypuck.com/playerData"

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

def _attempt_request(url, method="HEAD", stream=False, timeout=10):
    """Helper to perform request and return response or None on network error."""
    try:
        if method == "HEAD":
            return requests.head(url, headers=HEADERS, timeout=timeout)
        elif method == "GET":
            return requests.get(url, headers=HEADERS, stream=stream, timeout=timeout)
    except requests.RequestException as e:
        logger.warning(f"Network error on {method} {url}: {e}")
        return None
    return None

def resolve_url_candidates(rel_path):
    """Yields (url, description) tuples for candidates."""
    # Ensure rel_path doesn't start with /
    clean_rel = rel_path.lstrip('/')
    yield f"{DEFAULT_BASE_URL}/{clean_rel}", "primary"
    if DEFAULT_BASE_URL != FALLBACK_BASE_URL:
        yield f"{FALLBACK_BASE_URL}/{clean_rel}", "fallback"

def download_file_with_fallback(rel_path, target_path, force=False):
    """
    Download a file from a relative path, trying primary then fallback base URLs.
    Returns: "downloaded", "skipped", "failed", "served_from_cache"
    """
    target_file = Path(target_path)
    target_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Check cache first if not forcing
    if not force and validate_cache(target_file):
        pass # Will verify with HEAD or just skip if we wanted strict cache adherence

    last_error = None
    
    for url, desc in resolve_url_candidates(rel_path):
        # 1. Check existence/size with HEAD
        head = _attempt_request(url, "HEAD")
        
        if head and head.status_code == 403:
            logger.warning(f"Upstream 403 ({desc}): {url}")
            last_error = "403"
            continue # Try next candidate
            
        if head and head.status_code == 200:
            remote_size = int(head.headers.get('Content-Length', 0))
            if not force and validate_cache(target_file) and target_file.stat().st_size == remote_size:
                return "skipped" # Up to date, no need to download
            
            # Found a valid source, try downloading
            resp = _attempt_request(url, "GET", stream=True, timeout=30)
            if resp and resp.status_code == 200:
                try:
                    with open(target_file, 'wb') as f:
                        for chunk in resp.iter_content(chunk_size=8192):
                            f.write(chunk)
                    logger.info(f"Downloaded from {desc}: {url}")
                    return "downloaded"
                except Exception as e:
                    logger.error(f"Write error from {url}: {e}")
                    return "failed"
            else:
                logger.warning(f"GET failed after HEAD 200 ({desc}): {url}")
                continue
        
        # If HEAD failed (404 etc), try GET directly just in case (some servers block HEAD)
        if not head or head.status_code != 200:
             resp = _attempt_request(url, "GET", stream=True, timeout=10)
             if resp and resp.status_code == 403:
                 logger.warning(f"Upstream 403 on GET ({desc}): {url}")
                 last_error = "403"
                 continue
             
             if resp and resp.status_code == 200:
                 # Success
                 try:
                    with open(target_file, 'wb') as f:
                        for chunk in resp.iter_content(chunk_size=8192):
                            f.write(chunk)
                    logger.info(f"Downloaded from {desc} (direct GET): {url}")
                    return "downloaded"
                 except Exception as e:
                     logger.error(f"Write error from {url}: {e}")
                     return "failed"
    
    # If we get here, all candidates failed
    if validate_cache(target_file):
        logger.info(f"All network attempts failed. Using cached version for {target_file.name}")
        return "served_from_cache"

    if last_error == "403":
        if REFRESH_MODE == "required":
            logger.error(f"Fatal: 403 received and no cache for {rel_path}")
            return "failed"
        else:
            logger.warning(f"Skipping {rel_path} (403, best_effort)")
            return "skipped"

    logger.error(f"Failed to download {rel_path} from any source.")
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
    logger.info(f"Base URL: {DEFAULT_BASE_URL}")
    logger.info(f"Mode: {REFRESH_MODE}, Policy: {CACHE_POLICY}, Force: {force}")
    
    global_stats = {"downloaded": 0, "skipped": 0, "failed": 0, "served_from_cache": 0}

    # 1. Download Lookup
    # Lookup is at /playerBios/allPlayersLookup.csv relative to player data root?
    # Old URL was BASE_URL + /playerBios/allPlayersLookup.csv
    status = download_file_with_fallback("playerBios/allPlayersLookup.csv", data_root / "allPlayersLookup.csv", force=force)
    global_stats[status] += 1
    logger.info(f"Lookup file: {status}")
    
    import re
    def get_csv_links_with_fallback(rel_path_to_index):
        """Returns list of CSV links from index page, trying fallback if needed."""
        for url, desc in resolve_url_candidates(rel_path_to_index):
            # Ensure index ends with /
            if not url.endswith('/'):
                url += '/'
                
            resp = _attempt_request(url, "GET", timeout=30)
            if resp and resp.status_code == 200:
                links = re.findall(r'href=["\\]?([^"">]+\.csv)["\\]?', resp.text)
                return list(set(links)), url # Return the success URL base for constructing children
            elif resp and resp.status_code == 403:
                logger.warning(f"Upstream 403 on index ({desc}): {url}")
            else:
                logger.warning(f"Failed to fetch index ({desc}): {url}")
        
        return "failed", None

    groups = ["skaters", "goalies"]
    seasons = range(args.start_season, args.end_season + 1)
    
    for season in seasons:
        for group in groups:
            # rel_path is teamPlayerGameByGame/2024/regular/skaters
            rel_dir = f"teamPlayerGameByGame/{season}/{args.season_type}/{group}"
            target_dir = data_root / rel_dir
            
            logger.info(f"Syncing {season} {group}...")
            csv_files, success_base_url = get_csv_links_with_fallback(rel_dir)
            
            # Handle Index Failures
            if csv_files == "failed":
                if target_dir.exists():
                     cached_files = [f.name for f in target_dir.glob("*.csv")]
                     count = len(cached_files)
                     logger.warning(f"  -> Index blocked/failed. Retaining {count} cached files.")
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
                logger.warning(f"  -> No files found in index.")
                continue
                
            stats = {"downloaded": 0, "skipped": 0, "failed": 0, "served_from_cache": 0}
            
            # success_base_url is like https://.../skaters/
            # but we need to pass a relative path to download_file_with_fallback?
            # Actually, we know the exact URL that worked for the index, 
            # so the files *should* be relative to that.
            # But download_file_with_fallback logic tries both bases again.
            # Ideally we should just use the base that worked?
            # But maybe the index worked on fallback but files are on primary? Unlikely.
            # Let's simple pass the relative path of the file.
            
            for csv_file in csv_files:
                file_rel_path = f"{rel_dir}/{csv_file}"
                res = download_file_with_fallback(file_rel_path, target_dir / csv_file, force=force)
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

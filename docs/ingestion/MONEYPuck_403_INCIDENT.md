# MoneyPuck HTTP 403 Incident Report

**Date:** 2026-01-12  
**Status:** Active / mitigated via caching  
**Component:** `pipelines/backtesting/download_moneypuck_team_player_gbg.py`

## 1. Incident Description
The ingestion pipeline has begun receiving `HTTP 403 Forbidden` responses from MoneyPuck.com endpoints. This blocks fresh data acquisition, potentially stalling feature updates if local cache is cold.

### Affected Endpoints
- Base: `https://moneypuck.com/playerData`
- Lookup: `https://moneypuck.com/playerData/playerBios/allPlayersLookup.csv`
- Index Pages: `https://moneypuck.com/playerData/teamPlayerGameByGame/.../`

### Evidence
Logs indicate:
```text
Upstream returned 403; downloader skipped; ingestion may proceed using existing local data. URL: ...
```
This response typically occurs immediately upon request, suggesting WAF (Web Application Firewall) or bot protection rules have been tightened upstream.

## 2. Root Cause Analysis
- **Suspected Cause:** Bot Protection / User-Agent Blocking.
- **Trigger:** The script uses a hardcoded Chrome User-Agent, but lack of genuine browser fingerprints (TLS fingerprinting, cookies, etc.) or IP reputation issues may have triggered a block.
- **Impact:** 
    - **Production (Existing Cache):** Low impact. The system fails over to the last successful download.
    - **New Environments:** Critical impact. Cannot bootstrap without a data seed.

## 3. Mitigation Strategy
We are implementing a **"Cache First, Network Best-Effort"** strategy.

### Implemented Changes
1.  **Strict Fallback Modes:** 
    - `MPUCK_REFRESH_MODE=best_effort` (Default): If 403 occurs, log warning and use local files.
    - `MPUCK_REFRESH_MODE=required`: Hard fail if 403 occurs AND local cache is missing.
2.  **Cache Manifest:** A `_manifest.json` file now tracks the validity of the local cache.
3.  **Bootstrap Script:** A new utility `scripts/bootstrap_moneypuck_cache.py` allows manually seeding the cache from a zip archive (e.g., provided by a developer with browser access).

## 4. Path Forward
If 403s persist:
1.  Developers should manually download the `moneypuck/` data directory from a browser/unblocked IP.
2.  Zip the directory.
3.  Run `python scripts/bootstrap_moneypuck_cache.py --from-zip path/to/data.zip`.
4.  Pipeline will recognize valid cache and proceed.

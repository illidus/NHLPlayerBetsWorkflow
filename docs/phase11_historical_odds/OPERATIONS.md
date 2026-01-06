# Phase 11 — Operational Guide
**Status:** Working Draft  
**Last updated:** 2026-01-05

## 1. Running Odds Ingestion
### 1.1 Manual Run
```powershell
# Ingest all vendors
python pipelines/odds/run_odds_ingestion.py --all

# Ingest specific vendor
python pipelines/odds/run_odds_ingestion.py --vendor UNABATED
```

### 1.2 Integration with Production
Set environment variable:
```powershell
$env:RUN_ODDS_INGESTION=1
python pipelines/production/run_production_pipeline.py
```

## 2. Scheduling Recommendation
- **Forward Capture:** Run every 4 hours during game days.
- **Historical Backfill:** One-time execution per available historical snapshot.

## 3. Failure Modes & Recovery
- **Network Timeout:** All requests use 30s timeout + 3 retries.
- **Parser Error:** Vendor-specific parsers are isolated. If one fails, the pipeline logs the error and continues with other vendors.
- **DuckDB Lock:** The pipeline will fail if DuckDB is held by another process (e.g., DBeaver or another script).

## 4. Safety & Hygiene (CRITICAL)
- **NO COMMIT GUARD:**
    - `outputs/odds/raw/**` must be in `.gitignore`. (VERIFIED: Added to .gitignore)
    - `data/db/*.duckdb` must be in `.gitignore`. (VERIFIED: Already in .gitignore)
- **Hashing:** Every raw file is accompanied by a `.sha256` sidecar file and stored in `raw_odds_payloads` table.
- **Idempotency:** Re-running the same raw file will NOT create duplicate rows in `fact_prop_odds`.

## 5. Reprocessing
To reprocess raw payloads:
1. TRUNCATE `fact_prop_odds`. (Caution: deletes all normalized data).
2. Run `pipelines/odds/run_odds_ingestion.py --reprocess`.

## 6. Phase 11 Remediation Runs
- 2026-01-05: `python pipelines/backtesting/ingest_odds_to_duckdb.py` (hash stability check) – completed successfully.
- 2026-01-05: `python pipelines/backtesting/ingest_odds_to_duckdb.py` (DB constraints/idempotency check) – completed successfully.
- 2026-01-05: `python -` (update_player_mappings + update_event_mappings via inline script) – completed successfully.
- 2026-01-05: `python src/nhl_bets/analysis/runner_duckdb.py` (EV report gate for mapping safety) – completed successfully.
- 2026-01-05: `python -` (update_player_mappings + update_event_mappings via inline script, post-join fixes) – completed successfully.
- 2026-01-05: `python src/nhl_bets/analysis/runner_duckdb.py` (EV report gate for join correctness) – completed successfully.
- 2026-01-05: `python -m pytest tests/test_storage_hashing.py tests/test_runner_join.py` – completed successfully (2 passed).

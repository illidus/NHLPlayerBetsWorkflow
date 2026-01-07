# Unabated Debug Inventory

**Generated at (UTC):** 2026-01-07T05:34:49.189315+00:00

## Key Modules
- `src/nhl_bets/scrapers/unabated_client.py` (snapshot parsing + mapping)
- `pipelines/backtesting/ingest_odds_to_duckdb.py` (ingestion + metadata upserts)
- `pipelines/odds/backfill_unabated_snapshots.py` (raw snapshot backfill)
- `scripts/analysis/debug_unabated_mapping.py` (raw?DB spot checks)
- `scripts/analysis/unabated_coverage_report.py` (coverage diagnostics)
- `scripts/analysis/unabated_ui_reconcile.py` (UI reconciliation, debug-only)

## Key Tables
- `fact_prop_odds` (canonical odds)
- `raw_odds_payloads` (payload registry)
- `dim_events_unabated` (event metadata)
- `dim_players_unabated` (player metadata)
- `dim_events_mapping` (vendor?canonical events)
- `dim_players_mapping` (vendor?canonical players)

## Raw Payload Storage
- `outputs/odds/raw/UNABATED/YYYY/MM/DD/HHMMSS_unabated.json`

## Relevant Env Flags
- `FORCE_VENDOR_FAILURE`, `FORCE_UNABATED_FAILURE` (simulate vendor fetch failure)
- `EV_ODDS_FRESHNESS_MINUTES`, `EV_EVENT_START_GRACE_MINUTES` (production eligibility)
- `EV_EXCLUDE_BOOK_TYPES`, `EV_EXCLUDE_MARKETS` (gated exclusions; default OFF)

## Notes
- Unabated side mapping uses `si0`?OVER and `si1`?UNDER.
- Any vendor/book exclusions are now gated behind explicit flags (default OFF).

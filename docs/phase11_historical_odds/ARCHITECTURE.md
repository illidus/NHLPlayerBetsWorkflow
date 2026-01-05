# Phase 11 — Historical Odds Ingestion Architecture
**Status:** Design (frozen for Phase 11)  
**Last updated:** 2026-01-05

## 1. Module Layout
New and modified components for Phase 11:

### 1.1 Ingestion Primitives
- `src/nhl_bets/common/storage.py`: (New) Hashing and immutable file storage helpers.
- `src/nhl_bets/common/db_init.py`: (New or update) Schema initializer for Phase 11 tables.

### 1.2 Vendor Scrapers / Parsers
- `src/nhl_bets/scrapers/unabated_client.py`: (New) Snapshot fetcher for Unabated.
- `src/nhl_bets/scrapers/oddsshark_client.py`: (New) HTML snapshot fetcher + parser for OddsShark.
- `src/nhl_bets/scrapers/playnow_adapter.py`: (New) Adapter to normalize existing PlayNow scraper outputs.

### 1.3 Pipelines
- `pipelines/odds/run_odds_ingestion.py`: (New) Top-level orchestrator for odds capture and normalization.
- `pipelines/production/run_production_pipeline.py`: (Update) Add opt-in hook for `RUN_ODDS_INGESTION=1`.

## 2. DuckDB Schema
Append-only tables with deterministic dedup.

### 2.1 Fact Tables
- `fact_prop_odds`: Unified multi-book player prop odds.
- `raw_odds_payloads`: Registry of ingested files + hashes to prevent duplicate file processing.

### 2.2 Dimension Tables (Mapping Layer)
- `dim_books`: Vendor book name → Canonical book ID.
- `dim_markets`: Vendor market label → Canonical `market_type`.
- `dim_players_mapping`: Vendor player ID/Name → Canonical `player_id`.
- `dim_events_mapping`: Vendor event ID → Canonical `game_id`.

## 3. Dedup Keys
### 3.1 `fact_prop_odds` Unique Key
`(source_vendor, capture_ts_utc, event_id_vendor, player_id_vendor, player_name_raw, market_type, line, side, book_id_vendor)`

### 3.2 Idempotency Strategy
1. Load new records into `stg_prop_odds` (temporary).
2. `INSERT INTO fact_prop_odds` selecting from `stg_prop_odds` where the key does not already exist in `fact_prop_odds`.

## 4. Join Strategy
- **Stage 1 (Raw):** Preserves all vendor-specific identifiers.
- **Stage 2 (Mapping):** Best-effort join to `dim_games` and `dim_players` using:
    - Team + Date (for games).
    - Normalized Name string matching (for players).
- **Stage 3 (Canonical):** Exported reports use mapped keys where available, falling back to raw labels for coverage auditing.

## 5. Out of Scope
- Modifying μ formulas or probability math.
- ROI-based model selection or gating.
- Automated settlement of bets (beyond basic join-to-outcome reporting).
- Live execution/betting execution (this is ingestion/analysis only).
- Historical odds reconstruction for dates where snapshots were not captured.

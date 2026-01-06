# Phase 11 — Historical NHL Player Prop Odds Ingestion (PlayNow + Unabated + OddsShark)
**Status:** Implementation spec (authoritative for Phase 11 work)  
**Last updated:** 2026-01-05  
**Phase policy:** Phase 10 canonical modeling logic is frozen. Phase 11 adds a data-ingestion and normalization layer only.

---

## 1. Purpose
Implement a production-grade historical and forward-capture odds ingestion layer for NHL player props that:

- Captures multi-book player prop odds for markets: **GOALS, ASSISTS, POINTS, SOG, BLOCKS** (extendable).
- Stores **immutable raw snapshots** (verbatim payloads) per vendor for full audit/reprocessing.
- Normalizes odds into an **append-only** DuckDB schema with **deterministic dedup**.
- Integrates cleanly with the existing Phase 10 modeling/backtesting system **without changing** any canonical model logic.
- Enables multi-book EV analysis **without changing EV math** (odds sources expand; probability sources do not).

This phase is **data ingestion + normalization + joinability + reporting plumbing** only.

---

## 2. Phase 10 Non-Negotiables (Binding)
This Phase 11 work MUST NOT modify:

- Probability distributions, μ formulas, probability math
- Feature definitions or rolling windows
- MARKET_POLICY rules
- Calibration methods or fitting logic
- Backtesting philosophy: **accuracy-only gating** (allowed: Log Loss, Brier, ECE, ROC AUC, Top-K)
- Canonical probability sources (no recompute / fallback / alternate sources)
- Canonical outputs, except for opt-in hooks and additional Phase 11 artifacts

**ROI** is permitted only as a clearly labeled **exploratory** report and MUST NOT be used for gating or model selection.

---

## 3. In-Scope Vendors
### 3.1 PlayNow (Primary “current book” feed)
- Purpose: capture the book you actively bet/compare against.
- Ingestion may be via their content-service API endpoints (preferred) or existing browser automation if still required.
- Raw payloads must be stored immutably.
- Normalize into the canonical schema with `source_vendor = PLAYNOW`.
- PlayNow ingestion reuses the existing API-first scraper and normalized markets foundation; Phase 11 only normalizes into the unified odds table. You can adjust if you deem it important however.

### 3.2 Unabated (Primary multi-book board)
- Purpose: multi-book player prop odds board for historical and forward capture.
- Implement **snapshot-first** ingestion.
- Endpoint (snapshot): `https://content.unabated.com/markets/v2/league/6/propodds.json`
- Normalize into canonical schema with `source_vendor = UNABATED`.

### 3.3 OddsShark (Secondary / redundancy)
- Purpose: supplemental multi-book board for cross-checks and gap filling.
- Endpoint: `https://www.oddsshark.com/nhl/odds/player-props`
- Store raw HTML snapshots. Parse into the same schema with `source_vendor = ODDSSHARK`.
- Treated as non-canonical; keep vendor identity explicit.

---

## 4. Storage & Repo Hygiene (Must Follow)
### 4.1 Raw payload storage (immutable; git-ignored)
Store verbatim payloads under:

- `outputs/odds/raw/PLAYNOW/YYYY/MM/DD/<capture_ts>_playnow_*.json`
- `outputs/odds/raw/UNABATED/YYYY/MM/DD/<capture_ts>_propodds.json`
- `outputs/odds/raw/ODDSSHARK/YYYY/MM/DD/<capture_ts>_player-props.html`

Each raw file MUST also be accompanied by:
- `sha256` hash (stored in DB and/or sidecar `.sha256` file)

### 4.2 DuckDB storage (git-ignored)
Use existing project convention for DuckDB storage (e.g., `data/db/*.duckdb`).
Never commit the DB.

---

## 5. Canonical Normalized Schema (Minimum)
Implement DuckDB tables (append-only):

### 5.1 fact_prop_odds (append-only)
Columns (minimum):
- `source_vendor` (PLAYNOW | UNABATED | ODDSSHARK)
- `capture_ts_utc` (TIMESTAMP)
- `event_id_vendor` (TEXT)
- `event_start_ts_utc` (TIMESTAMP, nullable)
- `player_id_vendor` (TEXT, nullable for HTML vendors)
- `player_name_raw` (TEXT)
- `market_type` (TEXT: GOALS, ASSISTS, POINTS, SOG, BLOCKS)
- `line` (DOUBLE)
- `side` (TEXT: OVER, UNDER)
- `book_id_vendor` (TEXT)
- `book_name_raw` (TEXT)
- `odds_american` (INTEGER, nullable)
- `odds_decimal` (DOUBLE, nullable)
- `is_live` (BOOLEAN, default false if unknown)
- `raw_payload_path` (TEXT)
- `raw_payload_hash` (TEXT)

### 5.2 Dimensional tables (minimum)
- `dim_books` (stable internal id + vendor ids + canonical name)
- `dim_markets` (vendor betTypeId/labels → canonical `market_type`)
- `dim_events` (vendor event ids → internal event key if available)
- `dim_players` (vendor player ids/names → internal player key if available)

**Note:** Player/event mapping may start as best-effort and improve over time, but the raw vendor ids/names must always be preserved.

---

## 6. Dedup & Idempotency Rules
A rerun of the same capture MUST produce **0 net new rows**.

Recommended dedup uniqueness key:
`(source_vendor, capture_ts_utc, event_id_vendor, player_id_vendor, player_name_raw, market_type, line, side, book_id_vendor)`

Implementation may use:
- DuckDB `MERGE`, or
- insert into staging + `INSERT ... SELECT DISTINCT` with anti-join against existing keys.

---

## 7. Pipelines & Entrypoints
### 7.1 New entrypoint
Create:
- `pipelines/odds/run_odds_ingestion.py`

It should:
- accept vendor selection flags (e.g., `--vendor UNABATED`, `--vendor PLAYNOW`, `--vendor ODDSSHARK`)
- write raw payloads + hashes
- normalize into DuckDB tables
- emit a join-coverage report

### 7.2 Safe production integration (opt-in)
Add an opt-in hook in the production pipeline, gated by env var:
- `RUN_ODDS_INGESTION=1`

Default behavior when unset must remain unchanged.

---

## 8. EV Reporting Parity (No EV math changes)
Extend EV reporting to consume odds from `fact_prop_odds` across vendors and books.

Requirements:
- Existing probability sources remain the same (Phase 10).
- EV math remains unchanged.
- Output must carry:
  - `source_vendor`
  - `book_name`
  - `capture_ts_utc`
  - vendor and book identifiers for traceability

---

## 9. Backtesting Data Capture Over Time
Phase 11 must ensure the system begins capturing data needed for future evaluation:
- odds snapshots (this phase)
- existing probability snapshots (already present)
- outcomes (existing or add a read-only join table)

Generate:
- `outputs/odds/reports/odds_join_coverage_<date>.md`
  - counts by vendor/book/market
  - mapped vs unmapped event/player totals
  - top unmapped causes

---

## 10. Exploratory “Mini ROI” (Allowed, Non-Gating Only)
Given small sample sizes, ROI is observational only.

Rules:
- Flat 1-unit stake per bet candidate
- No threshold tuning using ROI results
- Must be clearly labeled non-gating

Outputs:
- `outputs/backtesting/roi_exploratory_<date>.csv`
- `outputs/backtesting/roi_exploratory_<date>.md` (include caveats, sample size)

---

## 11. Acceptance Criteria (Phase 11 “Done”)
- Unabated snapshot ingestion runs end-to-end: raw + normalized + dedup verified.
- PlayNow ingestion runs end-to-end (API or existing method): raw + normalized + dedup verified.
- OddsShark ingestion runs end-to-end (if enabled): raw + normalized + dedup verified.
- Opt-in production hook works and is off by default.
- EV report includes multi-vendor multi-book odds without changing EV math.
- Join coverage report exists.
- Exploratory ROI report exists (optional) and is explicitly non-gating.

# Phase 11 — Long-Run TODO (Gemini CLI Execution Plan)
**Last updated:** 2026-01-05

This file is designed to let an agent work for hours with clear checkpoints, verification, and safe rollback.

---

## Global Rules
- **Never modify Phase 10 canonical logic** (probability math, distributions, mu formulas, rolling windows, MARKET_POLICY, calibration fitting).
- **No committing raw payloads or DuckDB files.**
- Work in small commits; after each subsection, run the associated verification tasks and record results.

---

## Section A — Repo reconnaissance (READ-ONLY)
### Tasks
1. Identify existing DB utilities, path conventions, and artifact directories.
2. Identify existing EV analysis entrypoints and odds ingestion / scraping code (if any).
3. Identify canonical identifiers used for events/players/markets in Phase 10.

### Verification
- Produce `docs/phase11_historical_odds/RECON_NOTES.md` containing:
  - entrypoints found
  - existing DB file locations and access helpers
  - any existing odds-related modules
  - existing internal IDs to align with

---

## Section B — Governance wiring (Docs become source of truth)
### Tasks
1. Create `docs/phase11_historical_odds/PHASE11_IMPLEMENTATION.md` (this spec).
2. Create `docs/phase11_historical_odds/ARCHITECTURE.md` (agent-written plan before code).
3. Create `docs/phase11_historical_odds/SCHEMA.md` (DuckDB schema, dedup key, data types).
4. Create `docs/phase11_historical_odds/OPERATIONS.md` (how to run, scheduling, failure modes).
5. Create `docs/phase11_historical_odds/ACCEPTANCE_CRITERIA.md` (merge gates, pass/fail).

### Verification
- Docs exist and are internally consistent (links + entrypoints + outputs).
- ARCHITECTURE.md explicitly states what is out of scope.

---

## Section C — Storage primitives (raw payload + hashing)
### Tasks
1. Implement a small library:
   - write raw payload to `outputs/odds/raw/<vendor>/YYYY/MM/DD/...`
   - compute sha256
   - return `(path, hash, capture_ts_utc)`
2. Ensure gitignore patterns prevent payload/DB commit.

### Verification
- Unit-style check: write a test payload and confirm:
  - file exists
  - hash matches
- Add a “no-commit guard” note in OPERATIONS.md.

---

## Section D — DuckDB schema + idempotent insert
### Tasks
1. Create schema migration/initializer for:
   - `fact_prop_odds`, `dim_books`, `dim_markets`, `dim_players`, `dim_events`
2. Implement staging insert + dedup/anti-join to guarantee idempotency.

### Verification
- Run initializer twice; confirm no errors.
- Insert same sample rows twice; confirm 0 net new rows on second pass.

---

## Section E — Vendor ingestion: UNABATED (snapshot-first, required)
### Tasks
1. Fetch snapshot JSON.
2. Persist raw JSON + hash.
3. Parse to intermediate vendor-neutral records.
4. Normalize into `fact_prop_odds` with `source_vendor=UNABATED`.
5. Populate/refresh `dim_books` and `dim_markets` as mappings are discovered.

### Verification
- One run produces:
  - a raw snapshot file
  - non-zero normalized rows
- Rerun immediately:
  - raw file written (new timestamp) is OK
  - dedup prevents duplicate rows for same capture_ts
- Produce a vendor coverage summary:
  - counts by book and market_type.

---

## Section F — Vendor ingestion: PLAYNOW (required)
### Tasks
1. Determine best ingestion method:
   - Prefer content-service API (JSON), fall back to existing browser automation only if required.
2. Persist raw payloads + hash.
3. Normalize into `fact_prop_odds` with `source_vendor=PLAYNOW`.
4. Ensure event_id_vendor and player_name_raw are always populated.

### Verification
- Same as Unabated:
  - raw payload exists
  - normalized rows exist
  - rerun for same capture_ts yields 0 net new rows

---

## Section G — Vendor ingestion: ODDSSHARK (optional but planned)
### Tasks
1. Fetch player props HTML.
2. Persist raw HTML + hash.
3. Parse DOM for player/market/line/book/odds.
4. Normalize into `fact_prop_odds` with `source_vendor=ODDSSHARK`.

### Verification
- Snapshot saved, normalized rows exist.
- Parser isolated in vendor module; failure cannot break other vendors.

---

## Section H — Joinability layer + coverage reporting
### Tasks
1. Implement best-effort mapping functions to internal event/player keys:
   - start with name matching + game date/time heuristics
   - store mappings in `dim_players` and `dim_events`
2. Emit `outputs/odds/reports/odds_join_coverage_<date>.md`.

### Verification
- Coverage report created with:
  - totals and mapped/unmapped counts
  - top unmapped reasons

---

## Section I — EV multi-book reporting (no EV math changes)
### Tasks
1. Extend EV analysis to read from `fact_prop_odds` for selected capture windows.
2. Output enriched report including:
   - vendor, book, capture timestamp
3. Ensure existing Phase 10 behavior remains when odds ingestion is disabled.

### Verification
- Existing EV report still builds for PlayNow-only mode.
- New report builds for Unabated multi-book (and OddsShark if enabled).

---

## Section J — Exploratory ROI report (non-gating)
### Tasks
1. Create a post-run report using realized outcomes for bets that can be settled.
2. Flat 1u stake; no optimization.
3. Output CSV + MD caveats.

### Verification
- Report generated with explicit “exploratory, non-gating” headers.
- No tests/CI gates depend on ROI values.

---

## Section K — End-to-end verification (merge gate)
### Tasks
1. Run golden validations to confirm Phase 10 remains stable.
2. Run odds ingestion for requested vendors.
3. Run EV reporting with multi-book odds.

### Verification
- `scripts/golden_run_validate.py` PASS
- odds ingestion PASS
- EV report PASS
- join coverage report PASS

# Phase 11 — Acceptance Criteria
**Status:** Frozen  
**Last updated:** 2026-01-05

## 1. Functional Requirements
- [ ] **Unabated Ingestion:** Can fetch, store, and normalize JSON snapshots.
- [ ] **PlayNow Adapter:** Can normalize existing PlayNow API outputs into the unified schema.
- [ ] **OddsShark Ingestion:** Can fetch, store, and parse HTML snapshots.
- [ ] **Idempotency:** Re-running the pipeline on the same file adds 0 rows.
- [ ] **Joinability:** At least 80% of major books match to canonical keys for active games.
- [ ] **EV Parity:** Multi-book EV reports include vendor/book/timestamp.

## 2. Technical Requirements
- [ ] **Storage Hygiene:** Raw payloads stored in `outputs/odds/raw/` and git-ignored.
- [ ] **Hashing:** Every raw file has a verifiable SHA256 hash.
- [ ] **Isolation:** A failure in one vendor parser does not crash the entire pipeline.
- [ ] **Performance:** DuckDB inserts use vectorized or bulk operations (e.g. `INSERT INTO ... SELECT`).

## 3. Governance
- [ ] **Zero Regression:** `scripts/golden_run_validate.py` passes.
- [ ] **Frozen Logic:** No changes to μ, distributions, or rolling windows.
- [ ] **Docs:** Architecture and Operations docs are complete and accurate.

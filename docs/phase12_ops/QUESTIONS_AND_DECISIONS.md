# Phase 12 Questions and Decisions
**Status:** Append-only log for overnight mode decisions and blocked items.

- 2026-01-06
  - Issue: Which entrypoint should represent the "production probability snapshot run" in the daily driver.
  - Default chosen: Use `pipelines/backtesting/build_probability_snapshots.py`.
  - Rationale: It is the existing snapshot-oriented entrypoint and is safe (skips if tables already exist) while avoiding re-running the full production pipeline.
  - Alternatives: `pipelines/production/run_production_pipeline.py`, `src/nhl_bets/projections/single_game_probs.py`.
  - Follow-up needed: Yes.

- 2026-01-06
  - Issue: Unabated backfill cannot request historical snapshots from the API.
  - Default chosen: Treat backfill as "fetch if missing per date" using current snapshots, skipping dates that already have raw files.
  - Rationale: Provides a bounded, resumable scaffold with idempotent inserts without claiming historical API support.
  - Alternatives: Require a historical snapshot source or local archive.
  - Follow-up needed: Yes.

- 2026-01-06
  - Issue: Forced vendor failure toggle naming for simulated failures.
  - Default chosen: Support `FORCE_VENDOR_FAILURE=UNABATED|ODDSSHARK|PLAYNOW|ALL` and per-vendor `FORCE_<VENDOR>_FAILURE=1`.
  - Rationale: Provides a single toggle for tests while keeping per-vendor override simple.
  - Alternatives: A single boolean per vendor only.
  - Follow-up needed: No.

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

- 2026-01-06
  - Issue: OddsShark HTML does not expose a reliable game date for synthetic event IDs.
  - Default chosen: Use the capture date (`capture_ts.date()`) when generating the synthetic ID.
  - Rationale: Maintains deterministic IDs while avoiding brittle HTML parsing for dates.
  - Alternatives: Parse dates from HTML or require a separate schedule source.
  - Follow-up needed: Yes.

- 2026-01-06
  - Issue: Conflict between Phase 12 default daily log creation and new quality gate requiring no artifacts when all flags are off.
  - Default chosen: If no steps are enabled, `run_daily.py` exits without logging or registry writes.
  - Rationale: Satisfies the no-side-effects quality gate while keeping explicit runs logged.
  - Alternatives: Keep logging even when idle and exempt the test.
  - Follow-up needed: Yes.

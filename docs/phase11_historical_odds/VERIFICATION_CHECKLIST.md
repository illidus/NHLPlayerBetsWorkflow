# Phase 11 — Verification Checklist (Pass/Fail)
**Last updated:** 2026-01-05

Use this checklist to decide whether Phase 11 is merge-ready.

---

## V0 — Safety & Hygiene
- [ ] Raw payload directories are git-ignored (`outputs/odds/raw/**`)
- [ ] DuckDB files are git-ignored (`data/db/*.duckdb`)
- [ ] No secrets/cookies/tokens committed

## V1 — Schema & Idempotency
- [ ] DuckDB schema initializer creates required tables
- [ ] Re-running schema initializer is safe (no destructive behavior)
- [ ] Inserting identical staged rows twice produces 0 net new rows
- [ ] Dedup key implemented and documented

## V2 — Vendor Ingestion
### Unabated (required)
- [ ] Snapshot fetch succeeds
- [ ] Raw JSON snapshot stored with sha256
- [ ] Normalized rows inserted into `fact_prop_odds` with `source_vendor=UNABATED`
- [ ] Book/market mappings captured in dims

### PlayNow (required)
- [ ] Ingestion succeeds via API preferred (or documented fallback)
- [ ] Raw payload stored with sha256
- [ ] Normalized rows inserted with `source_vendor=PLAYNOW`

### OddsShark (optional)
- [ ] Raw HTML snapshot stored with sha256
- [ ] Parser produces normalized rows with `source_vendor=ODDSSHARK`
- [ ] Parser failure is isolated (does not fail Unabated/PlayNow runs)

## V3 — Integration Safety
- [ ] Production pipeline unchanged when `RUN_ODDS_INGESTION` is unset
- [ ] `RUN_ODDS_INGESTION=1` triggers odds ingestion without breaking production run

## V4 — Reporting
- [ ] `outputs/odds/reports/odds_join_coverage_<date>.md` generated
- [ ] EV report includes `source_vendor`, `book_name`, `capture_ts_utc`
- [ ] EV math unchanged (odds source expanded only)

## V5 — Exploratory ROI (optional, non-gating)
- [ ] ROI report includes clear non-gating disclaimer
- [ ] Flat staking only; no optimization
- [ ] ROI is not used in any acceptance gate

## V6 — Final Gate
- [ ] `scripts/golden_run_validate.py` PASS (Default/Debug/Backtest as available)
- [ ] Phase 10 accuracy/calibration artifacts remain consistent

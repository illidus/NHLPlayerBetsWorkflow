# Phase 11: Historical Odds Ingestion

**Status:** EXPERIMENTAL
**Purpose:** Backtesting and coverage analysis. Not for live production betting.

## Overview
This module provides a pipeline to ingest historical odds from:
1.  **Offline Fixtures:** JSON dumps of past odds (for development/testing).
2.  **Live APIs:** (Future) Fetching historical windows from providers.

## Architecture
- **Source:** JSON fixtures or API.
- **Normalization:** `src/nhl_bets/odds_historical/normalize_phase11.py`
- **Storage:** DuckDB (Table: `fact_odds_historical_phase11`)
- **Pipeline:** `pipelines/phase11_historical_odds/run_phase11_historical_odds.py`

## Join Keys & Matching
To support downstream matching with game stats, the normalization process generates:
*   `game_date` (YYYY-MM-DD)
*   `home_team_norm` / `away_team_norm` (Standardized uppercase strings)
*   `match_key` (Format: `DATE|AWAY|HOME`)

## How to Run (Fixture Mode)
This is the canonical way to test ingestion without API keys.

```powershell
python pipelines/phase11_historical_odds/run_phase11_historical_odds.py --fixture examples/phase11/fixture_happy_path.json
```

## Governance Outputs
This pipeline is fully integrated with repo governance tooling.

*   **Run Manifest:** `outputs/runs/run_manifest_<timestamp>.json`
    *   Indexed by `scripts/index_historical_evals.py`.
    *   Contains record of input source and row counts.
*   **Eval Manifest:** `outputs/eval/eval_manifest_<timestamp>.json`
    *   Stubs a "coverage evaluation" to track data quality over time.
*   **Coverage Report:** `outputs/odds_archive_audit/phase11_coverage_<timestamp>.md`
    *   Detailed human-readable audit log (ignored by git).

## Coexistence with Phase 12
Phase 12 is the *production* live odds ingestion. Phase 11 is strictly for *historical backfill* and *offline analysis*. They may share schema concepts but run in separate pipelines to ensure production stability.
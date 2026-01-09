# Phase 12 Operations Manual

## 1. Ingestion Commands

### Mock Run (Test Logic)
Use this to verify plumbing without API costs.
```powershell
python pipelines/phase12_odds_api/run_provider_ingestion.py --real_sample_date 2023-01-01 --mock --db_path data/db/nhl_backtest.duckdb
```

### Real Run (Current Odds)
**Requires API Key.**
fetches current odds for NHL from The-Odds-API.
```powershell
$env:THE_ODDS_API_KEY="your_api_key_here"
python pipelines/phase12_odds_api/run_provider_ingestion.py --real_sample_date 2026-01-08
```

### Historical Backfill (Paid Only)
**Warning: High Cost.**
```powershell
$env:THE_ODDS_API_KEY="your_api_key_here"
python pipelines/phase12_odds_api/run_provider_ingestion.py --start_date 2023-01-01 --end_date 2023-01-31
```

## 2. Audit & Verification
After a run, check `outputs/phase12_odds_api/`:
- `run_summary.json`: High-level stats.
- `audit_quota_burn.md`: Cost tracking.
- `audit_join_confidence.md`: Data quality check.
- `ingestion_audit.md`: Append-only log.

## 3. Idempotency
The system is designed to be idempotent. Re-running the same date/range should not duplicate rows in `fact_prop_odds`, provided the source payload is identical (or cached).
For live API calls, re-running WILL fetch new data (new timestamps), resulting in new rows (snapshots). This is intended behavior for line movement tracking.

## 4. Viability Metrics
Before moving a provider from "Experimental" to "Production", evaluate the audit artifacts:

### Grade Definitions

#### A) Exploration-Grade
*Acceptable for mapping verification and coverage estimation.*
- `join_conf_event` >= 0.80
- `join_conf_player` >= 0.60 (Name-based matches allowed)
- `roi_count` / `total_rows` > 50%

#### B) ROI-Grade (Production)
*Minimum standard for backtesting projection models.*
- `join_conf_event` == 1.0 (Requires vendor event IDs)
- `join_conf_player` >= 0.95 (Requires stable vendor player IDs or highly unique name matching)
- `roi_count` / `total_rows` > 85%
- `market_coverage` includes GOALS, ASSISTS, POINTS, SOG.

| Metric | Exploration | ROI (Prod) | Description |
|---|---|---|---|
| `join_conf_event` | 0.80 | 1.0 | Event mapping confidence. |
| `join_conf_player` | 0.60 | 0.95 | Player mapping confidence. |
| `roi_count` / `total_rows` | 50% | 85% | Valid row percentage. |
| `market_coverage` | 3 markets | 4+ markets | Market variety. |

Failure to meet these thresholds indicates the provider may require better normalization or a higher-tier plan.

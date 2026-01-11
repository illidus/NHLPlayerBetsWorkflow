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

## 5. Player Identity Resolution (Phase 13)
The ingestion pipeline now automatically runs Phase 13 Identity Resolution.
- High-confidence matches (>= 0.90) are promoted to `fact_prop_odds`.
- Low-confidence matches (< 0.90) are routed to `stg_prop_odds_unresolved`.

### Verification
Check `outputs/phase12_odds_api/<run_ts>/audit_unresolved_reasons.md` (if generated) or check the DB:
```sql
SELECT failure_reasons, count(*) FROM stg_prop_odds_unresolved GROUP BY 1;
```

### Identity Proof Mode
To verify the quality of player resolution without commiting data or to just inspect stats:
```powershell
python pipelines/phase12_odds_api/run_provider_ingestion.py ... --prove_identity
```
Check `outputs/phase12_odds_api/<run_ts>/identity_proof.json`.

## 6. Manual Alias Resolution
To improve resolution rates, review the `stg_player_alias_review_queue`.

1. **Review Queue:**
   ```sql
   SELECT * FROM stg_player_alias_review_queue WHERE decision_status = 'PENDING';
   ```
2. **Create Aliases:**
   Insert confirmed mappings into `dim_player_alias`:
   ```sql
   INSERT INTO dim_player_alias (source_vendor, alias_text_norm, canonical_player_id, match_confidence, match_method)
   VALUES ('THE_ODDS_API', 'alexis lafreniere', 'player_id_123', 1.0, 'MANUAL');
   ```
3. **Mark Resolved:**
   Update the queue to prevent re-review:
   ```sql
   UPDATE stg_player_alias_review_queue SET decision_status = 'RESOLVED' WHERE alias_text_norm = 'alexis lafreniere';
   ```
4. **Re-run Ingestion:**
   Re-running the ingestion for the same date will now resolve these players correctly using the new aliases.

## 7. Roster Management (Phase 13 Strict Mode)
Phase 13 requires roster snapshots to perform safe fuzzy matching.
If `audit_player_resolution.md` shows high "Missing Roster Failures":

1. **Check Snapshots:**
   ```sql
   SELECT team_abbrev, snapshot_date, count(*) FROM dim_team_roster_snapshot GROUP BY 1,2;
   ```
2. **Populate Snapshots:**
   (Pending Phase 13.5 Roster Scraper)
   Manually insert for critical dates if needed, or run the roster ingestion job (TBD).
   Structure:
   ```json
   [{"player_id": "p1", "player_name_canonical": "Connor McDavid", "nhl_id": 8478402}, ...]
   ```

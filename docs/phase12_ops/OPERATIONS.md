# Operational Workflow (Phase 12)

This document describes the standardized daily workflow for the NHL Player Bets system.

## 1. Daily Execution
The system is driven by a single entrypoint that ensures safe ordering and error isolation.

```powershell
python pipelines/ops/run_daily.py --run-production --run-odds-ingestion --run-ev --run-diagnostics --fail-fast
```

### Steps:
1. **Production Projections:** Generates the probability snapshot (`SingleGamePropProbabilities.csv`).
2. **Odds Ingestion:** Scrapes PlayNow, Unabated, and OddsShark. Normalizes data into `fact_prop_odds`.
3. **EV Analysis:** Computes EV% by joining the latest odds with the probability snapshot.
4. **Diagnostics:** Updates longitudinal evidence tables and generates reports.

## 2. Automated Evidence Chain
Every run produces artifacts in `outputs/monitoring/` for rapid verification.

- **`daily_report_YYYY-MM-DD.md`:** Executive summary of step status, vendor health, and record counts.
- **`ev_freshness_coverage_latest.md`:** Audit of how many bets were excluded due to staleness (>90m) or games already starting.
- **`unabated_mapping_coverage_latest.md`:** Confirmation that Unabated odds are correctly tied to events and players.
- **`cross_book_coherence_latest.md`:** Detection of price outliers across different sportsbooks.
- **`top5_ev_walkthrough_latest.md`:** Forensic walkthrough of the highest EV bets, tracing math from Mu -> Distribution -> Odds -> EV.

## 3. Database Schema
Longitudinal data is stored in DuckDB for trend analysis:
- `fact_run_registry`: History of every script execution.
- `fact_odds_coverage_daily`: Volume by vendor/book/market.
- `fact_mapping_quality_daily`: Success rate of player/event joins.
- `fact_ev_summary_daily`: Aggregate EV stats to detect model drift or market shifts.

## 4. Maintenance & Debugging
- **Vendor Failures:** If a scraper fails, `run_daily.py` will log it but continue with other vendors (unless `--fail-fast` is used).
- **Stale Odds:** If `MultiBookBestBets.xlsx` is empty, check `ev_freshness_coverage_latest.md` to see if the odds were scraped too long after the probability snapshot.
- **Audit Failures:** If a bet looks "too good to be true," use `scripts/analysis/audit_model_prob.py` to generate a fresh walkthrough.
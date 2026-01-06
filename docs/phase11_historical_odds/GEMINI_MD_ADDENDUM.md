# GEMINI.md — Suggested Phase 11 Addendum (manual merge)
**Date:** 2026-01-05

Add the following to GEMINI.md (do not remove existing Phase 10 constraints).

## Phase 11 — Historical Odds Ingestion (In Progress)
**Spec:** docs/phase11_historical_odds/PHASE11_IMPLEMENTATION.md

### Scope
- Add odds ingestion for: PLAYNOW (primary current book), UNABATED (primary multi-book), ODDSSHARK (supplemental).
- Store immutable raw snapshots (git-ignored) and normalize into append-only DuckDB tables with deterministic dedup.
- Integrate via opt-in flag RUN_ODDS_INGESTION=1 so Phase 10 production runs remain unchanged by default.

### Evaluation
- Backtesting acceptance gates remain accuracy-only (Log Loss, Brier, ECE, ROC AUC, Top-K).
- Exploratory ROI may be produced as a labeled non-gating report when sample size is small.

### Artifacts (Git Hygiene)
- MUST NEVER COMMIT: outputs/odds/raw/**, any DuckDB files, cookies/tokens/secrets.

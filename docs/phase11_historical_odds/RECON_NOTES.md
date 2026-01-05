# Phase 11 — Repo Reconnaissance Notes
**Date:** 2026-01-05

## 1. Entrypoints Found
- **Main Production Pipeline:** `pipelines/production/run_production_pipeline.py`
- **Backtest EV Runner:** `pipelines/backtesting/run_ev_backtest.py`
- **PlayNow Scraper:** `src/nhl_bets/scrapers/scrape_playnow_api.py` (API-first)
- **Odds Ingestion (Historical/Legacy):** `pipelines/backtesting/ingest_odds_to_duckdb.py`

## 2. Existing DB File Locations & Access Helpers
- **DB Path:** `data/db/nhl_backtest.duckdb`
- **Connection Helper:** Most scripts use `duckdb.connect(DB_PATH)` directly. Some have local connection functions.
- **Pragmas:** `SET memory_limit = '8GB'; SET threads = 8; SET temp_directory = './duckdb_temp/';` (Required as per `GEMINI.md`).

## 3. Existing Odds-Related Modules
- `src/nhl_bets/scrapers/nhl_props_scraper.py`
- `src/nhl_bets/scrapers/playnow_api_client.py`
- `pipelines/backtesting/normalize_odds_schema.py`

## 4. Existing Internal IDs to Align With
- **Player ID:** `player_id` (BIGINT, standard NHL ID). Found in `dim_players`.
- **Game ID:** `game_id` (TEXT, e.g., `anaheim-ducks-at-washington-capitals` or numeric). Found in `dim_games`.
- **Market Type:** `market_type` (TEXT: `GOALS`, `ASSISTS`, `POINTS`, `SOG`, `BLOCKS`).
- **Sides:** `OVER`, `UNDER`.

## 5. Existing Tables (Reference)
- `fact_odds_props`: Existing historical odds table (to be supplemented/replaced by `fact_prop_odds`).
- `fact_skater_game_all`: Source for realized outcomes and player deployment.
- `dim_players`: Canonical player list.
- `dim_games`: Canonical game list.

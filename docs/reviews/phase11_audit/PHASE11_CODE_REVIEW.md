# Phase 11 Implementation Audit — Code Review

## Executive Summary
**Merge readiness:** PASS with conditions. Core Phase 10 artifacts are untouched, but Phase 11 ingestion/mapping lacks enforced uniqueness, deterministic hashing, and stable join keys. Vendor adapters are wired but fragile parsing and name-only joins create leakage risk. Address the listed blockers before relying on multi-book EV output. **Blocking to ship**: (1) enforce uniqueness/constraints for `fact_prop_odds` and mapping tables, (2) stabilize hashing for idempotent replays, (3) tighten joins to canonical IDs (or team+date) before producing EV.

## File-by-File Findings
- **pipelines/backtesting/ingest_odds_to_duckdb.py (L35-L142)**
  - Dedup relies solely on `raw_odds_payloads.payload_hash`; hashes are derived from `json.dumps` without key sorting, so equivalent payloads with different key order will bypass the duplicate check and reinsert (L48-L112). **Action:** hash with `sort_keys=True` and add a UNIQUE constraint.
  - Inserts use anti-join on `capture_ts_utc` and other fields (L101-L113 via db_init) rather than the documented unique key; a replayed payload with a new timestamp will duplicate even if `payload_hash` matches. **Action:** include `raw_payload_hash` in the key and constrain at the DB layer.
  - PlayNow event list is saved but not registered for dedup (L92-L95), so reruns will always refetch details even if unchanged. **Action:** log and register event-list payloads too.

- **src/nhl_bets/common/db_init.py (L12-L116)**
  - `fact_prop_odds`, `dim_books`, `dim_markets`, and mapping tables have no primary/unique constraints; the implemented anti-join key omits `raw_payload_hash` and `event_name_raw`, diverging from the documented unique key `(source_vendor, capture_ts_utc, event_id_vendor, player_id_vendor, player_name_raw, market_type, line, side, book_id_vendor)` (L101-L113). **Action:** declare PRIMARY KEY/UNIQUE indexes on the documented key and add `raw_payload_hash` to support replay detection.
  - No NOT NULL clauses on key fields; nulls will collapse distinct rows through `COALESCE` in the anti-join. **Action:** add NOT NULL to key columns (vendor, capture_ts_utc, event_id_vendor, market_type, line, side, book_id_vendor).

- **src/nhl_bets/common/storage.py (L33-L49)**
  - Hashing uses unsorted `json.dumps` output; dict order differences change hashes, undermining idempotency guarantees when vendors reorder fields.

- **src/nhl_bets/scrapers/unabated_client.py (L26-L123)**
  - Assumes fixed `BET_TYPE_MAP` and skips `betSubType` props entirely; if Unabated adds markets or reuses IDs, odds may be silently dropped.
  - Home/away derived from `eventTeams` without fallback; missing keys will yield `None` teams and degrade event mapping.

- **src/nhl_bets/scrapers/oddsshark_client.py (L17-L165)**
  - HTML parsing assumes tab/button order to infer `market_type`; DOM drift will silently map to `None` and skip markets. Default `event_id_vendor` of `"unknown"` (L75-L87) makes dedup/join unstable across slates.
  - Book identifiers are derived from `<img alt>` and lowercased (L101-L156) with no mapping table enforcement, so the same book may appear under multiple keys.

- **src/nhl_bets/scrapers/playnow_adapter.py (L20-L135)**
  - American odds are recalculated from decimal with rounding (L103-L132); discrepancies vs upstream quoted prices are possible and not tagged as derived. Only the first price is read when multiple price entries exist (L105-L108).
  - No vendor player IDs; uses player names parsed from market text (L64-L99), which risks duplicates for identical names.

- **src/nhl_bets/analysis/normalize.py (L115-L222)**
  - Player mapping uses name-only equality to `dim_players` (L123-L132) with no team/season disambiguation; common names can map to the wrong player_id and persist in `dim_players_mapping` with no uniqueness constraint.
  - Event mapping uses capture date ±1 day vs `dim_games.game_date` (L191-L219) and matches on swapped home/away, enabling cross-season collisions for reused event IDs; mapping is append-only without constraints.
  - `get_mapped_odds` joins by vendor player name, ignoring `vendor_player_id` even when present (L229-L235).

- **src/nhl_bets/analysis/runner_duckdb.py (L53-L156)**
  - Odds-to-probs join uses normalized name only (L53-L64); ignores canonical IDs and team, so two players with the same name will be merged. No capture-window filter—stale odds can join to fresh probabilities.
  - EV output is written twice with slightly different schemas (drops `ev_sort` first, then writes full frame) (L138-L155), risking user confusion over which file is canonical.
  - GOALS market is hard-excluded (L83-L85) without rationale; EV math otherwise matches Phase 10 (`EV = p * odds - 1`).

- **pipelines/production/run_production_pipeline.py (L27-L175)**
  - Phase 11 ingestion and multi-book EV are not wired into production or guarded by feature flags. This protects Phase 10 but leaves new functionality manual/undocumented in the main orchestrator.

## Governance Violations
- None detected for Phase 10 logic: projection, calibration, and EV math remain unchanged. Phase 11 additions do not alter existing Phase 10 backtests.

## Risks & Failure Modes
- **Idempotency drift:** Unsorted hashing plus absence of table constraints allows duplicate odds rows on replays or dict reorderings; dedup key omits payload hash entirely.
- **Mapping leakage:** Name-only player/event joins can bind odds to the wrong canonical keys; lack of constraints means bad mappings persist.
- **Vendor fragility:** OddsShark DOM or Unabated schema changes will silently drop markets. PlayNow odds reconstruction may diverge from source prices.
- **Join ambiguity:** Runner joins on names without date/team filters, risking EV based on mismatched games or duplicated players.

## Recommendations (Prioritized, minimal-impact)
1. **Enforce unique keys in DuckDB**: Add primary/unique constraints on `fact_prop_odds` using the documented key and include `raw_payload_hash` in the anti-join to guarantee idempotency. Add NOT NULL where feasible.
2. **Stabilize hashing & dedup**: Use `json.dumps(..., sort_keys=True)` (or vendor-provided checksum) before hashing; register all saved payloads (including PlayNow event lists) in `raw_odds_payloads` for replay skips.
3. **Harden mappings**: Add uniqueness on `dim_players_mapping`/`dim_events_mapping`, include team/season filters, and prefer `vendor_player_id`/`event_start_ts_utc` when available. Update `get_mapped_odds` to join on vendor IDs when present.
4. **Tighten odds/prob join**: Merge on `(canonical_player_id or norm_name + team)` and align capture windows to avoid stale odds; remove duplicate exports and document the canonical EV artifact.
5. **Vendor resilience**: Add schema assertions for Unabated bet types, guard OddsShark parsing with explicit selectors and fallbacks, and tag PlayNow odds as derived when decimal → American conversion is applied.

## Next Steps & Clean-up (Actionable)
- **DB constraints and backfill:** Add PRIMARY KEY/UNIQUE + NOT NULL constraints on `fact_prop_odds`, `dim_books`, `dim_markets`, `dim_players_mapping`, and `dim_events_mapping`. Backfill by de-duplicating on the target keys before applying constraints to avoid migration failures.
- **Stable hashing retrofit:** Swap hashing to `json.dumps(payload, sort_keys=True)` (or equivalent canonicalization) in `storage.py`, then re-hash existing payload registries to align with the new scheme. Document the hashing method inline to prevent regressions.
- **Mapping hygiene sweep:** Purge existing duplicate mappings using team/date disambiguation, then re-run normalization with uniqueness enforced so bad joins cannot persist. Capture any manual overrides in a checked-in mapping dictionary.
- **Runner alignment:** Update `runner_duckdb.py` to (a) prefer canonical IDs when present, (b) enforce a recency window for odds vs projections, and (c) emit a single EV artifact path. Rename or delete the duplicate write to avoid confusion.
- **Vendor regression tests:** Add lightweight schema/HTML contract tests for Unabated and OddsShark parsers plus a PlayNow derived-price tag check; wire them into CI to catch upstream drift before ingestion runs.

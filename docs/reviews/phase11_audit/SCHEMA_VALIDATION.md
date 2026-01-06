# Schema Validation — Phase 11 Odds Ingestion

## Intended Uniqueness / Dedup Key
- Documentation states `fact_prop_odds` key: `(source_vendor, capture_ts_utc, event_id_vendor, player_id_vendor, player_name_raw, market_type, line, side, book_id_vendor)` (docs/phase11_historical_odds/ARCHITECTURE.md §3.1).
- Implementation anti-join (src/nhl_bets/common/db_init.py L101-L113) uses the same columns but **omits `raw_payload_hash` and any constraint/PRIMARY KEY**, leaving dedup purely procedural. **Blocking action:** add PRIMARY KEY/UNIQUE on the documented columns plus `raw_payload_hash` to defend replay integrity.

## Idempotency Mechanisms
- Raw payload dedup: `pipelines/backtesting/ingest_odds_to_duckdb.py` checks `raw_odds_payloads.payload_hash` before parsing (L35-L112). Hash is computed via `json.dumps` without `sort_keys` (src/nhl_bets/common/storage.py L33-L40), so ordering differences break repeat detection.
- Insert dedup: rows are inserted only if an anti-join against `fact_prop_odds` finds no match on the procedural key above (src/nhl_bets/common/db_init.py L101-L114). No database constraints prevent concurrent/racing inserts. **Action:** enforce uniqueness in DDL so concurrent runs cannot double-insert.
- Mapping tables: `dim_players_mapping` and `dim_events_mapping` receive inserts via `normalize.update_player_mappings`/`update_event_mappings` with no uniqueness enforcement, so reruns can duplicate rows if the `LEFT JOIN` filters miss any null/spacing variation.

## Potential Duplicate Pathways
- **JSON order drift:** Vendor payloads with identical data but different key order produce new hashes and bypass `raw_odds_payloads` dedup, resulting in repeated inserts with new `capture_ts_utc`. **Action:** hash with `sort_keys=True` (or vendor-provided checksum) before dedup checks.
- **Capture-timestamp sensitivity:** The anti-join key includes `capture_ts_utc`; replaying the same payload with a new capture timestamp will insert a second copy even when `payload_hash` matches. **Action:** add `raw_payload_hash` to the unique key and enforce in DDL.
- **Missing constraints on dimensions:** No PRIMARY KEY on `dim_books`, `dim_markets`, `dim_players_mapping`, or `dim_events_mapping`; manual corrections or concurrent jobs can create duplicate mapping rows. **Action:** add uniqueness per vendor key.
- **OddsShark defaults:** Rows with `event_id_vendor='unknown'` (oddsshark_client.py L75-L87) will dedup only on `capture_ts_utc`, allowing repeated “unknown” rows per run.
- **PlayNow event list:** Event list payloads are saved but not registered in `raw_odds_payloads` (ingest_odds_to_duckdb.py L92-L95); reruns refetch details and can insert the same odds if hashing changes.

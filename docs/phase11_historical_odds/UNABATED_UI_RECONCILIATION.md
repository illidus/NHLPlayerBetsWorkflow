# Unabated UI Reconciliation

Purpose: deterministically trace a UI price to raw payload fields, normalized rows, and EV outputs without changing production filters.

## Mapping Rules (Unabated Props)
- **Row/column semantics (UI):** each row is one player + market + game; each column is a book's prices for that same prop.
- **Side mapping:** `si0:*` -> `OVER`, `si1:*` -> `UNDER`.
- **American odds:** read from `americanPrice` (raw), stored as `odds_american`.
- **Decimal odds:** derived from American odds (no double-conversion).
- **Book mapping:** `marketSourceId` from keys like `ms2`, resolved via `marketSources`.
- **Book type:** classify as `SPORTSBOOK` vs `DFS_FIXED_PAYOUT` based on book name keywords (diagnostic only).
- **Event/Player IDs:** `eventId` and `personId` stored as `vendor_event_id` and `vendor_person_id`.
- **Matchup:** `eventTeams` mapped to `home_team` and `away_team` using `teams`.
- **Player team:** `teamId` (when present) mapped via `teams` and stored in Unabated metadata and odds.

## Diagnostic Script
Script: `scripts/analysis/unabated_ui_reconcile.py`

Inputs are supplied via JSON:
- `examples/unabated_ui_examples.template.json` (fill out for new UI evidence)
- `examples/unabated_ui_examples.json` (sample inputs)

Run (uses the most recent raw snapshot by default):
```powershell
python scripts/analysis/unabated_ui_reconcile.py --examples examples/unabated_ui_examples.json
```

Optional:
```powershell
python scripts/analysis/unabated_ui_reconcile.py --examples examples/unabated_ui_examples.json --file outputs/odds/raw/UNABATED/YYYY/MM/DD/HHMMSS_unabated.json
```

## Report Output
Generated at:
`outputs/monitoring/unabated_ui_reconcile_<timestamp>.md`

The report includes:
- Inputs for each example
- Raw payload excerpts (eventId/personId/betTypeId/line/side/book/odds)
- Matching `fact_prop_odds` rows (LIMIT 20)
- Pass/fail checks for side, odds, and event time alignment

## Common Pitfalls
- **Side inversion:** `si0` is `OVER`, `si1` is `UNDER` (do not assume ordering).
- **Book ambiguity:** use `marketSourceId` when possible.
- **DFS platforms:** PrizePicks / Underdog / Pick6 are fixed-payout; do not treat them as sportsbook odds.
- **Completed games:** debug tool does not apply bettable-now filtering; this is expected.

## Checklist (UI -> Raw -> DB)
- Identify row: player + market + line + game (from UI header).
- Identify column: book logo/name and side (o/u).
- Locate raw entry by `eventId`, `personId`, `betTypeId`, and `points`.
- Confirm side mapping via `sideKey` (`si0`/`si1`) and price via `americanPrice`.
- Verify `vendor_event_id`, `vendor_person_id`, `event_start_time_utc`, `home_team`, `away_team`, and `player_team` in `fact_prop_odds`.
- Confirm `book_name_raw` and classify `book_type` (SPORTSBOOK vs DFS_FIXED_PAYOUT).

## Example Reconciliations (UI Ground Truth)
Source snapshot: `outputs/odds/raw/UNABATED/2026/01/07/051429_unabated.json`

### Example A: Sebastian Aho SOG 2.5 (Novig)
- UI row: Sebastian Aho, Shots on Goal, DAL @ CAR.
- UI column: Novig (o2.5 +108 / u2.5 -141).
- Raw payload:
  - `eventId`: 104502
  - `personId`: 44247
  - `betTypeId`: 86 (SOG)
  - `points`: 2.5
  - `sideKey`: `si0:pid44247` (OVER), `si1:pid44247` (UNDER)
  - `marketSourceId`: 89 (Novig)
  - `americanPrice`: +108 (OVER), -141 (UNDER)
- DB (`fact_prop_odds`):
  - `vendor_event_id`: 104502
  - `vendor_person_id`: 44247
  - `event_start_time_utc`: 2026-01-07T00:00:00
  - `home_team`: CAR, `away_team`: DAL
  - `player_team`: CAR
  - `book_name_raw`: Novig, `book_type`: SPORTSBOOK
  - `side`: OVER -> odds +108, UNDER -> odds -141

### Example B: Wyatt Johnston Assists 0.5 (Underdog Fantasy)
- UI row: Wyatt Johnston, Assists, DAL @ CAR.
- UI column: Underdog Fantasy (DFS fixed-payout).
- Raw payload:
  - `eventId`: 104502
  - `personId`: 242424
  - `betTypeId`: 73 (ASSISTS)
  - `points`: 0.5
  - `sideKey`: `si0:pid242424` (OVER), `si1:pid242424` (UNDER)
  - `marketSourceId`: 73 (Underdog Fantasy)
  - `americanPrice`: -189 (OVER), +151 (UNDER)
- DB (`fact_prop_odds`):
  - `vendor_event_id`: 104502
  - `vendor_person_id`: 242424
  - `event_start_time_utc`: 2026-01-07T00:00:00
  - `home_team`: CAR, `away_team`: DAL
  - `player_team`: DAL
  - `book_name_raw`: Underdog Fantasy, `book_type`: DFS_FIXED_PAYOUT
  - `side`: OVER -> odds -189, UNDER -> odds +151

### Example C: Thomas Harley Blocks 2.5 (DraftKings)
- UI row: Thomas Harley, Blocked Shots, DAL @ CAR.
- UI column: DraftKings (o2.5 +140 / u2.5 -182).
- Raw payload:
  - `eventId`: 104502
  - `personId`: 43984
  - `betTypeId`: 88 (BLOCKS)
  - `points`: 2.5
  - `sideKey`: `si0:pid43984` (OVER), `si1:pid43984` (UNDER)
  - `marketSourceId`: 1 (DraftKings)
  - `americanPrice`: +140 (OVER), -182 (UNDER)
- DB (`fact_prop_odds`):
  - `vendor_event_id`: 104502
  - `vendor_person_id`: 43984
  - `event_start_time_utc`: 2026-01-07T00:00:00
  - `home_team`: CAR, `away_team`: DAL
  - `player_team`: DAL
  - `book_name_raw`: DraftKings, `book_type`: SPORTSBOOK
  - `side`: OVER -> odds +140, UNDER -> odds -182

### Example D: Shayne Gostisbehere Points 0.5 (Underdog Fantasy)
- UI row: Shayne Gostisbehere, Points, DAL @ CAR.
- UI column: Underdog Fantasy (DFS fixed-payout).
- Raw payload:
  - `eventId`: 104502
  - `personId`: 44070
  - `betTypeId`: 70 (POINTS)
  - `points`: 0.5
  - `sideKey`: `si0:pid44070` (OVER), `si1:pid44070` (UNDER)
  - `marketSourceId`: 73 (Underdog Fantasy)
  - `americanPrice`: +117 (OVER), -143 (UNDER)
- DB (`fact_prop_odds`):
  - `vendor_event_id`: 104502
  - `vendor_person_id`: 44070
  - `event_start_time_utc`: 2026-01-07T00:00:00
  - `home_team`: CAR, `away_team`: DAL
  - `player_team`: CAR
  - `book_name_raw`: Underdog Fantasy, `book_type`: DFS_FIXED_PAYOUT
  - `side`: OVER -> odds +117, UNDER -> odds -143

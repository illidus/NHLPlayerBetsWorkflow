# Unabated UI Reconciliation

Purpose: deterministically trace a UI price to raw payload fields, normalized rows, and EV outputs without changing production filters.

## Mapping Rules (Unabated Props)
- **Side mapping:** `si0:*` -> `OVER`, `si1:*` -> `UNDER`.
- **American odds:** read from `americanPrice` (raw), stored as `odds_american`.
- **Decimal odds:** derived from American odds (no double-conversion).
- **Book mapping:** `marketSourceId` from keys like `ms2`, resolved via `marketSources`.
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
- **Completed games:** debug tool does not apply bettable-now filtering; this is expected.

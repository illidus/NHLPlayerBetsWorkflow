# Unabated Odds Mapping Notes

This document explains how raw Unabated prop odds are mapped to the unified `fact_prop_odds` table.

## Raw Fields and Mapping

### Market Type (`market_type`)
Mapped from `betTypeId` in the raw JSON:
- `70` -> `POINTS`
- `73` -> `ASSISTS`
- `86` -> `SOG`
- `88` -> `BLOCKS`
- `129` -> `GOALS`

### Side (`side`)
Determined by the `sideKey` in the `sides` dictionary:
- Keys starting with `si0` (e.g., `si0:pid45587`) are mapped to **`OVER`**.
- Keys starting with `si1` (e.g., `si1:pid45587`) are mapped to **`UNDER`**.
*Note: This convention is specific to prop markets in the Unabated API structure.*

### Sportsbook (`book_name_raw`)
Mapped from `marketSourceId`:
- Extracted from keys like `ms73` in the `sides[sideKey]` dictionary.
- Resolved against the `marketSources` lookup table in the JSON payload.
- Canonical IDs: `73` (Underdog Fantasy), `1` (DraftKings), `2` (FanDuel), etc.

### Odds (`odds_american`, `odds_decimal`)
- `odds_american` is taken directly from `americanPrice`.
- `odds_decimal` is derived from `americanPrice` using standard conversion formulas.

## Forensics and Observability Columns
To aid in debugging mapping issues, the following columns are populated in `fact_prop_odds` for Unabated data:

- `vendor_market_source_id`: The raw ID of the book (e.g., `"73"`).
- `vendor_bet_type_id`: The raw `betTypeId` (e.g., `70`).
- `vendor_outcome_key`: The raw side key (e.g., `"si1:pid45587"`).
- `vendor_price_raw`: The raw `americanPrice` as a string.
- `vendor_price_format`: Always `"american"` for Unabated.

## Verification
Use the debug script to verify mapping for a specific player or market:
```powershell
python scripts/analysis/debug_unabated_mapping.py --person-id 45587 --bet-type-id 70 --points 0.5
```

Known Gotchas:
- **Anytime Goal Scorer:** These markets (betTypeId 129) often only have a 'Yes' side. The parser currently filters out GOALS markets with fewer than 2 sides to avoid ambiguity until settlement logic is refined.
- **Milestones/Alt Lines:** The parser currently skips records where `betSubType` is present to focus on standard lines.
- **Player Team:** When `teamId` is available on the prop, it is mapped to a team abbreviation via `teams` and stored in Unabated metadata/odds.

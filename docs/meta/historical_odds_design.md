# Design Note: Historical Odds Ingestion Pipeline

## Objective
To enable ROI-based backtesting and market efficiency analysis by ingesting and normalizing historical betting odds for player props.

## 1. Folder Scaffold
```text
5_Odds/
├── raw/                # Original files from vendors (JSON, CSV, API dumps)
├── snapshots/          # Daily/Hourly snapshots in parquet/csv format
├── normalized/         # Cleaned, unified schema ready for DuckDB ingestion
└── dim_books.csv       # Sportsbook metadata (id, name, region, margin_calc_type)
```

## 2. Normalized Data Schema
The `fact_historical_odds` table in DuckDB should adhere to the following schema:

| Field | Type | Description |
|:---|:---|:---|
| `odds_id` | UUID | Primary key for the odds record |
| `event_id` | STRING | Link to the specific game (e.g., MoneyPuck gameId) |
| `player_id` | STRING | Link to the player (standardized NHL ID) |
| `market` | ENUM | Market type (GOALS, ASSISTS, POINTS, SOG, BLOCKS) |
| `line` | FLOAT | The prop line (e.g., 0.5, 2.5) |
| `over_odds` | FLOAT | Decimal odds for the Over |
| `under_odds` | FLOAT | Decimal odds for the Under |
| `book_id` | INT | Foreign key to `dim_books` |
| `timestamp` | DATETIME | When the odds were captured |
| `is_closing` | BOOLEAN | Flag indicating if this is the closing line |

## 3. Join Logic & Backtesting Integration
Historical odds will be joined to game outcomes and model projections using the following logic:

1. **Date + Player Join**: Match `player_id` and `event_id` (or `game_date` + `team`) between `fact_historical_odds` and `fact_skater_game_all`.
2. **Snapshot Selection**: For a given game, select the odds record closest to the projection generation time (or the closing line) to prevent look-ahead bias.
3. **Outcome Calculation**:
   - `hit = 1 if actual_value > line else 0`
   - `profit = (over_odds - 1) if hit == 1 else -1` (for Over bets)
4. **Efficiency Metrics**: Calculate `CLV` (Closing Line Value) by comparing model-selected odds against the final closing line.

## 4. Integration with DuckDB
A new pipeline `4_Backtesting/30_pipelines/ingest_historical_odds.py` will be created to:
- Read from `5_Odds/normalized/`.
- Perform entity resolution (matching vendor player names to NHL IDs).
- Load into a new `fact_historical_odds` table in `nhl_backtest.duckdb`.

## 5. Potential Risks
- **Name Matching**: Player name variations across books (e.g., "Mitch Marner" vs "Mitchell Marner").
- **Timezone Alignment**: Ensuring odds timestamps are correctly synchronized with game start times.
- **Settle Ambiguity**: Handling "Push" results on integer lines.

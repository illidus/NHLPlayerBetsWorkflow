# Schema Mapping Standard

This document defines how to map a Provider's raw API response to the `fact_prop_odds` internal schema.

## Target Schema (`fact_prop_odds`)

| Internal Column | Required? | Description |
|---|---|---|
| `source_vendor` | YES | Constant provider name (e.g., 'THE_ODDS_API') |
| `capture_ts_utc` | YES | UTC Timestamp when the odds were published/observed (Provider timestamp) |
| `requested_asof_ts_utc` | NO | The snapshot time requested (if historical fetch) |
| `ingested_at_utc` | YES | System timestamp when the raw payload was saved |
| `event_id_vendor` | YES | Provider's unique ID for the game |
| `event_start_ts_utc` | YES | Game start time |
| `player_id_vendor` | YES | Provider's unique ID for the player (or name hash if missing) |
| `player_name_raw` | YES | Player name as it appears in source |
| `market_type` | YES | Normalized market (GOALS, ASSISTS, POINTS, SOG, BLOCKS) |
| `line` | YES | The handicap/line (e.g., 2.5, 0.5) |
| `side` | YES | 'Over' or 'Under' |
| `book_id_vendor` | YES | Provider's ID for the sportsbook (e.g., 'draftkings') |
| `book_name_raw` | YES | Book name as it appears in source |
| `odds_american` | YES | American odds (integer). Convert decimal if needed. |
| `odds_decimal` | NO | Decimal odds |
| `join_conf_event` | NO | 0.0-1.0 confidence of map to dim_events |
| `join_conf_player` | NO | 0.0-1.0 confidence of map to dim_players |
| `join_conf_market` | NO | 0.0-1.0 confidence of map to dim_markets |

## Mapping Registry

### Provider: [PROVIDER_NAME]

#### Market Type Map
| Provider String | Internal `market_type` |
|---|---|
| `player_points` | `POINTS` |
| `player_assists` | `ASSISTS` |
| `player_goals` | `GOALS` |
| `player_shots_on_goal` | `SOG` |
| `player_blocked_shots` | `BLOCKS` |

#### Bookmaker Map (Optional / Dynamic)
| Provider Key | Internal `dim_books` ID |
|---|---|
| `draftkings` | `draftkings` |
| `fanduel` | `fanduel` |

#### Data Transformations
1. **Timestamps:** Convert `2023-01-01T12:00:00Z` to UTC datetime.
2. **Odds:** If only Decimal provided, `American = (Dec - 1) * 100` if >= 2.0 else `-100 / (Dec - 1)`.
3. **Player IDs:** If provider has no IDs, use `md5(player_name + team_abbr)`.

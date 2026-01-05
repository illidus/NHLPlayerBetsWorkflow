# Phase 11 — DuckDB Schema Definition
**Status:** Frozen  
**Last updated:** 2026-01-05

## 1. Fact Tables

### 1.1 fact_prop_odds
| Column | Type | Description |
| :--- | :--- | :--- |
| `source_vendor` | TEXT | PLAYNOW, UNABATED, ODDSSHARK |
| `capture_ts_utc` | TIMESTAMP | Time of data capture |
| `event_id_vendor` | TEXT | Vendor's unique ID for the game |
| `event_start_ts_utc` | TIMESTAMP | Scheduled start time (nullable) |
| `player_id_vendor` | TEXT | Vendor's unique ID for the player (nullable) |
| `player_name_raw` | TEXT | Player name as it appeared at the source |
| `market_type` | TEXT | GOALS, ASSISTS, POINTS, SOG, BLOCKS |
| `line` | DOUBLE | The betting line (e.g. 0.5, 2.5) |
| `side` | TEXT | OVER, UNDER |
| `book_id_vendor` | TEXT | Vendor's ID for the sportsbook |
| `book_name_raw` | TEXT | Sportsbook name as it appeared at the source |
| `odds_american` | INTEGER | American odds (e.g. -110, +150) |
| `odds_decimal` | DOUBLE | Decimal odds (e.g. 1.91, 2.50) |
| `is_live` | BOOLEAN | True if the market was live at capture |
| `raw_payload_path` | TEXT | Path to the immutable raw JSON/HTML |
| `raw_payload_hash` | TEXT | SHA256 hash of the raw payload |

### 1.2 raw_odds_payloads
| Column | Type | Description |
| :--- | :--- | :--- |
| `payload_hash` | TEXT | PRIMARY KEY (SHA256) |
| `source_vendor` | TEXT | PLAYNOW, UNABATED, ODDSSHARK |
| `capture_ts_utc` | TIMESTAMP | Time of capture |
| `file_path` | TEXT | Relative path to file |
| `ingested_at_utc` | TIMESTAMP | Time of normalization |

## 2. Dimension Tables (Mappings)

### 2.1 dim_books
| Column | Type | Description |
| :--- | :--- | :--- |
| `book_key` | TEXT | PRIMARY KEY (e.g. 'draftkings', 'pinnacle') |
| `book_name_canonical` | TEXT | Display name |
| `vendor_book_id` | TEXT | Vendor-specific ID |
| `source_vendor` | TEXT | Originating vendor |

### 2.2 dim_markets
| Column | Type | Description |
| :--- | :--- | :--- |
| `vendor_market_label` | TEXT | Raw label from vendor |
| `market_type` | TEXT | GOALS, ASSISTS, POINTS, SOG, BLOCKS |
| `source_vendor` | TEXT | Originating vendor |

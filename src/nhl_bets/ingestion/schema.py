import duckdb
from pathlib import Path

class OddsSchemaManager:
    """
    Phase 11: Schema Management for Odds Ingestion.
    Defines and initializes the DuckDB schema for fact_prop_odds and dimensions.
    """
    
    DDL_STATEMENTS = [
        """
        CREATE TABLE IF NOT EXISTS dim_books (
            book_id VARCHAR PRIMARY KEY,
            book_name_canonical VARCHAR,
            vendor_book_ids JSON  -- Map of vendor:id
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_markets (
            market_type VARCHAR PRIMARY KEY, -- GOALS, ASSISTS, etc.
            description VARCHAR
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_players (
            player_id VARCHAR PRIMARY KEY, -- Internal UUID or NHL ID
            player_name_canonical VARCHAR,
            nhl_id BIGINT,
            vendor_ids JSON -- Map of vendor:id
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_events (
            event_id VARCHAR PRIMARY KEY, -- Internal UUID or game_id
            game_id_canonical VARCHAR,
            event_start_ts_utc TIMESTAMP,
            vendor_ids JSON -- Map of vendor:id
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_prop_odds (
            source_vendor VARCHAR,
            capture_ts_utc TIMESTAMP,
            requested_asof_ts_utc TIMESTAMP,
            ingested_at_utc TIMESTAMP,
            event_id_vendor VARCHAR,
            event_start_ts_utc TIMESTAMP,
            player_id_vendor VARCHAR,
            player_name_raw VARCHAR,
            market_type VARCHAR,
            line DOUBLE,
            side VARCHAR,
            book_id_vendor VARCHAR,
            book_name_raw VARCHAR,
            odds_american INTEGER,
            odds_decimal DOUBLE,
            is_live BOOLEAN,
            raw_payload_path VARCHAR,
            raw_payload_hash VARCHAR,
            join_conf_event DOUBLE,
            join_conf_player DOUBLE,
            join_conf_market DOUBLE,
            is_dfs BOOLEAN DEFAULT FALSE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_dfs_props (
            source_vendor VARCHAR,
            capture_ts_utc TIMESTAMP,
            requested_asof_ts_utc TIMESTAMP,
            ingested_at_utc TIMESTAMP,
            event_id_vendor VARCHAR,
            event_start_ts_utc TIMESTAMP,
            player_id_vendor VARCHAR,
            player_name_raw VARCHAR,
            market_type VARCHAR,
            line DOUBLE,
            side VARCHAR,
            book_id_vendor VARCHAR,
            book_name_raw VARCHAR,
            odds_american INTEGER,
            odds_decimal DOUBLE,
            is_live BOOLEAN,
            raw_payload_path VARCHAR,
            raw_payload_hash VARCHAR,
            join_conf_event DOUBLE,
            join_conf_player DOUBLE,
            join_conf_market DOUBLE,
            is_dfs BOOLEAN DEFAULT TRUE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS stg_prop_odds_unresolved (
            source_vendor VARCHAR,
            capture_ts_utc TIMESTAMP,
            ingested_at_utc TIMESTAMP,
            event_id_vendor VARCHAR,
            player_name_raw VARCHAR,
            market_type VARCHAR,
            line DOUBLE,
            side VARCHAR,
            book_id_vendor VARCHAR,
            odds_american INTEGER,
            raw_payload_path VARCHAR,
            raw_payload_hash VARCHAR,
            failure_reasons JSON, -- List of strings
            raw_row_json JSON, -- Full row dump for debugging
            is_dfs BOOLEAN DEFAULT FALSE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_player_alias (
            alias_id VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
            source_vendor VARCHAR,
            alias_text_raw VARCHAR,
            alias_text_norm VARCHAR,
            canonical_player_id VARCHAR,
            team_abbrev VARCHAR, -- Optional context
            season VARCHAR, -- Optional context
            match_method VARCHAR,
            match_confidence DOUBLE,
            created_ts_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_ts_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (source_vendor, alias_text_norm, team_abbrev, season)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_team_roster_snapshot (
            team_abbrev VARCHAR,
            snapshot_date DATE,
            roster_json JSON, -- List of {player_id, full_name, etc.}
            ingested_at_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (team_abbrev, snapshot_date)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS stg_player_alias_review_queue (
            queue_id VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
            source_vendor VARCHAR,
            alias_text_raw VARCHAR,
            alias_text_norm VARCHAR,
            event_id_vendor VARCHAR,
            game_start_ts_utc TIMESTAMP,
            home_team_raw VARCHAR,
            away_team_raw VARCHAR,
            candidate_players_json JSON, -- List of potential matches
            decision_status VARCHAR DEFAULT 'PENDING', -- PENDING, RESOLVED, REJECTED
            created_ts_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            resolved_ts_utc TIMESTAMP,
            resolved_canonical_player_id VARCHAR,
            resolution_notes VARCHAR
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS raw_editorial_mentions (
            mention_id VARCHAR PRIMARY KEY, -- UUID
            raw_text_snippet VARCHAR,
            extracted_props JSON, -- Structure {player, market, line, odds...}
            derived_game_date DATE,
            confidence_score DOUBLE,
            status_code VARCHAR, -- MISSING_ODDS, AMBIGUOUS_DATE, CANDIDATE_READY
            rejection_reason VARCHAR,
            metadata JSON, -- {age_hours, page_title, url, etc.}
            ingest_ts_utc TIMESTAMP
        );
        """
    ]

    def __init__(self, db_path: str):
        self.db_path = db_path

    def ensure_schema(self):
        """Idempotently applies DDL and adds missing columns to existing tables."""
        con = duckdb.connect(self.db_path)
        try:
            for ddl in self.DDL_STATEMENTS:
                con.execute(ddl)
            
            # Phase 12: Ensure all required columns exist in fact_prop_odds
            # We'll check columns and ALTER TABLE if missing.
            required_cols = {
                "fact_prop_odds": {
                    "requested_asof_ts_utc": "TIMESTAMP",
                    "ingested_at_utc": "TIMESTAMP",
                    "event_start_ts_utc": "TIMESTAMP",
                    "join_conf_event": "DOUBLE",
                    "join_conf_player": "DOUBLE",
                    "join_conf_market": "DOUBLE",
                    "is_dfs": "BOOLEAN DEFAULT FALSE",
                    "player_resolve_method": "VARCHAR",
                    "player_resolve_conf": "DOUBLE",
                    "player_resolve_notes": "VARCHAR",
                    "player_id_canonical": "VARCHAR",
                    "home_team_raw": "VARCHAR",
                    "away_team_raw": "VARCHAR"
                },
                "stg_prop_odds_unresolved": {
                    "is_dfs": "BOOLEAN DEFAULT FALSE"
                }
            }
            
            for table, cols in required_cols.items():
                existing_cols = [c[1] for c in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
                for col_name, col_type in cols.items():
                    if col_name not in existing_cols:
                        # Special case: if event_start_ts_utc is missing but event_start_time_utc exists, we might want to rename or just add it.
                        # For now, just add it.
                        print(f"Adding column {col_name} to {table}")
                        con.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
        finally:
            con.close()

    def insert_idempotent(self, df, table_name="fact_prop_odds", key_cols=None):
        """
        Inserts DataFrame into table, ignoring duplicates based on key_cols.
        Uses explicit column names to avoid count mismatches.
        """
        if df.empty:
            return 0
            
        if key_cols is None:
            # Default Phase 11/12 Dedup Key
            key_cols = [
                "source_vendor", "capture_ts_utc", "event_id_vendor", 
                "player_name_raw", "market_type", 
                "line", "side", "book_id_vendor"
            ]
            
        con = duckdb.connect(self.db_path)
        try:
            # Register DF
            con.register("staging_df", df)
            
            # Filter df columns to only those that exist in target table
            existing_cols = [c[1] for c in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()]
            df_cols = [c for c in df.columns if c in existing_cols]
            col_str = ", ".join(df_cols)
            source_col_str = ", ".join([f"source.{c}" for c in df_cols])
            
            join_conditions = []
            for col in key_cols:
                if col in existing_cols and col in df.columns:
                    join_conditions.append(f"target.{col} IS NOT DISTINCT FROM source.{col}")
            
            join_clause = " AND ".join(join_conditions)
            
            query = f"""
            INSERT INTO {table_name} ({col_str})
            SELECT {source_col_str}
            FROM staging_df source
            LEFT JOIN {table_name} target ON {join_clause}
            WHERE target.source_vendor IS NULL
            """
            
            con.execute(query)
            return True
            
        except Exception as e:
            print(f"Insert failed: {e}")
            raise
        finally:
            con.close()

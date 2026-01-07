import duckdb
import logging
import os
import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = os.path.join("data", "db", "nhl_backtest.duckdb")

def initialize_phase11_tables(con: duckdb.DuckDBPyConnection):
    """
    Initializes the schema for Phase 11 Historical Odds Ingestion.
    """
    logger.info("Initializing Phase 11 tables...")
    
    # 1. fact_prop_odds (Main unified odds table)
    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_prop_odds (
        source_vendor TEXT,
        capture_ts_utc TIMESTAMP,
        event_id_vendor TEXT,
        event_id_vendor_raw TEXT,
        vendor_event_id TEXT, -- Phase 12.8
        event_name_raw TEXT,
        event_start_time_utc TIMESTAMP,
        home_team TEXT,
        away_team TEXT,
        player_id_vendor TEXT,
        vendor_person_id TEXT, -- Phase 12.8
        player_name_raw TEXT,
        market_type TEXT,
        line DOUBLE,
        side TEXT,
        book_id_vendor TEXT,
        book_name_raw TEXT,
        odds_american INTEGER,
        odds_decimal DOUBLE,
        odds_quoted_raw TEXT,
        odds_quoted_format TEXT,
        odds_american_derived BOOLEAN,
        odds_decimal_derived BOOLEAN,
        is_live BOOLEAN DEFAULT FALSE,
        raw_payload_path TEXT,
        raw_payload_hash TEXT,
        vendor_market_source_id TEXT,
        vendor_bet_type_id INTEGER,
        vendor_outcome_key TEXT,
        vendor_price_raw TEXT,
        vendor_price_format TEXT
    )
    """)

    # Ensure columns exist for older tables
    con.execute("ALTER TABLE fact_prop_odds ADD COLUMN IF NOT EXISTS event_id_vendor_raw TEXT")
    con.execute("ALTER TABLE fact_prop_odds ADD COLUMN IF NOT EXISTS vendor_event_id TEXT")
    con.execute("ALTER TABLE fact_prop_odds ADD COLUMN IF NOT EXISTS vendor_person_id TEXT")
    con.execute("ALTER TABLE fact_prop_odds ADD COLUMN IF NOT EXISTS odds_quoted_raw TEXT")
    con.execute("ALTER TABLE fact_prop_odds ADD COLUMN IF NOT EXISTS odds_quoted_format TEXT")
    con.execute("ALTER TABLE fact_prop_odds ADD COLUMN IF NOT EXISTS odds_american_derived BOOLEAN")
    con.execute("ALTER TABLE fact_prop_odds ADD COLUMN IF NOT EXISTS odds_decimal_derived BOOLEAN")
    con.execute("ALTER TABLE fact_prop_odds ADD COLUMN IF NOT EXISTS vendor_market_source_id TEXT")
    con.execute("ALTER TABLE fact_prop_odds ADD COLUMN IF NOT EXISTS vendor_bet_type_id INTEGER")
    con.execute("ALTER TABLE fact_prop_odds ADD COLUMN IF NOT EXISTS vendor_outcome_key TEXT")
    con.execute("ALTER TABLE fact_prop_odds ADD COLUMN IF NOT EXISTS vendor_price_raw TEXT")
    con.execute("ALTER TABLE fact_prop_odds ADD COLUMN IF NOT EXISTS vendor_price_format TEXT")

    # Phase 12.7 Migration: Rename event_start_ts_utc to event_start_time_utc
    cols = [row[1] for row in con.execute("PRAGMA table_info('fact_prop_odds')").fetchall()]
    if 'event_start_ts_utc' in cols and 'event_start_time_utc' not in cols:
        logger.info("Renaming event_start_ts_utc to event_start_time_utc...")
        # Check if index exists and drop it if it does
        indexes = con.execute("SELECT index_name FROM duckdb_indexes() WHERE table_name = 'fact_prop_odds'").fetchall()
        idx_names = [r[0] for r in indexes]
        
        # We'll drop ALL indexes on this table to be safe for the rename
        for idx in idx_names:
            con.execute(f"DROP INDEX {idx}")

        con.execute("ALTER TABLE fact_prop_odds RENAME COLUMN event_start_ts_utc TO event_start_time_utc")
        
        # Recreate the known dedup index if it was there (or always create it if missing)
        con.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_fact_prop_odds_dedup ON fact_prop_odds(
                source_vendor, capture_ts_utc, event_id_vendor, player_id_vendor, 
                player_name_raw, market_type, line, side, book_id_vendor, raw_payload_hash
            )
        """)
    elif 'event_start_time_utc' not in cols:
        con.execute("ALTER TABLE fact_prop_odds ADD COLUMN event_start_time_utc TIMESTAMP")

    
    # 2. raw_odds_payloads (Ingestion registry)
    con.execute("""
    CREATE TABLE IF NOT EXISTS raw_odds_payloads (
        payload_hash TEXT PRIMARY KEY,
        source_vendor TEXT,
        capture_ts_utc TIMESTAMP,
        file_path TEXT,
        ingested_at_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # 3. dim_books (Sportsbook mapping)
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_books (
        book_key TEXT,
        book_name_canonical TEXT,
        vendor_book_id TEXT,
        source_vendor TEXT
    )
    """)
    
    # 4. dim_markets (Market type mapping)
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_markets (
        vendor_market_label TEXT,
        market_type TEXT,
        source_vendor TEXT
    )
    """)
    
    # 5. Mapping tables for players and events (Simplified for now)
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_players_mapping (
        vendor_player_id TEXT,
        vendor_player_name TEXT,
        source_vendor TEXT,
        canonical_player_id BIGINT
    )
    """)
    
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_events_mapping (
        vendor_event_id TEXT,
        source_vendor TEXT,
        canonical_game_id TEXT
    )
    """)

    # Phase 12.8 Metadata Tables
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_events_unabated (
        vendor_event_id TEXT PRIMARY KEY,
        event_start_time_utc TIMESTAMP,
        home_team TEXT,
        away_team TEXT,
        league TEXT,
        capture_ts_utc TIMESTAMP
    )
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_players_unabated (
        vendor_person_id TEXT PRIMARY KEY,
        player_name TEXT,
        team_abbr TEXT,
        capture_ts_utc TIMESTAMP
    )
    """)
    
    logger.info("Phase 11 tables initialized.")

def insert_odds_records(con: duckdb.DuckDBPyConnection, df):
    """
    Inserts odds records from a DataFrame into fact_prop_odds with idempotency.
    Uses a temporary staging table to perform an anti-join.
    """
    if df is None or len(df) == 0:
        return
    
    # Ensure UTC timestamp columns are naive UTC (stripping tzinfo) to avoid DuckDB auto-converting to Local
    # We want the DB to store the UTC wall clock time in the TIMESTAMP column.
    ts_cols = ['capture_ts_utc', 'event_start_time_utc']
    for col in ts_cols:
        if col in df.columns:
            try:
                # Log before normalization
                if not df[col].empty:
                    first_val = df[col].iloc[0]
                    logger.info(f"Col {col} raw sample: {first_val} (type: {type(first_val)})")

                # 1. Force to UTC Aware (handles naive as UTC, aware as conversion)
                # 2. Remove timezone info (localize to None) so DuckDB stores exact UTC values
                if not df[col].isnull().all():
                     df[col] = pd.to_datetime(df[col], utc=True).dt.tz_localize(None)
                     
                # Log after normalization
                if not df[col].empty:
                    first_val_after = df[col].iloc[0]
                    logger.info(f"Col {col} normalized sample: {first_val_after} (type: {type(first_val_after)})")
            except Exception as e:
                logger.warning(f"Failed to normalize timestamp col {col}: {e}")

    # Align DataFrame to table schema
    table_cols = [row[1] for row in con.execute("PRAGMA table_info('fact_prop_odds')").fetchall()]
    for col in table_cols:
        if col not in df.columns:
            df[col] = None
    df = df[table_cols]

    # Register DataFrame as a virtual table
    con.register("stg_new_odds", df)
    
    # perform anti-join to only insert rows that don't already exist
    # Note: we use COALESCE for nullable fields in the join to ensure correct comparison
    con.execute("""
    INSERT INTO fact_prop_odds
    SELECT n.* FROM stg_new_odds n
    LEFT JOIN fact_prop_odds e ON 
        n.source_vendor = e.source_vendor AND
        n.capture_ts_utc = e.capture_ts_utc AND
        n.event_id_vendor = e.event_id_vendor AND
        COALESCE(n.player_id_vendor, 'NULL') = COALESCE(e.player_id_vendor, 'NULL') AND
        n.player_name_raw = e.player_name_raw AND
        n.market_type = e.market_type AND
        n.line = e.line AND
        n.side = e.side AND
        n.book_id_vendor = e.book_id_vendor
    WHERE e.source_vendor IS NULL
    """)
    
    con.unregister("stg_new_odds")

def insert_unabated_metadata(con: duckdb.DuckDBPyConnection, events_df: pd.DataFrame, players_df: pd.DataFrame):
    """
    Inserts Unabated event and player metadata with upsert logic.
    """
    if events_df is not None and len(events_df) > 0:
        logger.info(f"Upserting {len(events_df)} Unabated events...")
        con.register("stg_events_unabated", events_df)
        con.execute("""
            INSERT OR REPLACE INTO dim_events_unabated
            SELECT * FROM stg_events_unabated
        """)
        con.unregister("stg_events_unabated")

    if players_df is not None and len(players_df) > 0:
        logger.info(f"Upserting {len(players_df)} Unabated players...")
        con.register("stg_players_unabated", players_df)
        con.execute("""
            INSERT OR REPLACE INTO dim_players_unabated
            SELECT * FROM stg_players_unabated
        """)
        con.unregister("stg_players_unabated")

def get_db_connection(db_path: str = DEFAULT_DB_PATH) -> duckdb.DuckDBPyConnection:
    """
    Returns a DuckDB connection with standard pragmas.
    """
    con = duckdb.connect(db_path)
    con.execute("SET memory_limit = '8GB';")
    con.execute("SET threads = 8;")
    con.execute("SET temp_directory = './duckdb_temp/';")
    con.execute("SET TimeZone = 'UTC';")
    return con

import duckdb
import logging

logger = logging.getLogger(__name__)

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
        event_start_ts_utc TIMESTAMP,
        player_id_vendor TEXT,
        player_name_raw TEXT,
        market_type TEXT,
        line DOUBLE,
        side TEXT,
        book_id_vendor TEXT,
        book_name_raw TEXT,
        odds_american INTEGER,
        odds_decimal DOUBLE,
        is_live BOOLEAN DEFAULT FALSE,
        raw_payload_path TEXT,
        raw_payload_hash TEXT
    )
    """)
    
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
    
    logger.info("Phase 11 tables initialized.")

def insert_odds_records(con: duckdb.DuckDBPyConnection, df):
    """
    Inserts odds records from a DataFrame into fact_prop_odds with idempotency.
    Uses a temporary staging table to perform an anti-join.
    """
    if df is None or len(df) == 0:
        return
    
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

def get_db_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    """
    Returns a DuckDB connection with standard pragmas.
    """
    con = duckdb.connect(db_path)
    con.execute("SET memory_limit = '8GB';")
    con.execute("SET threads = 8;")
    con.execute("SET temp_directory = './duckdb_temp/';")
    return con

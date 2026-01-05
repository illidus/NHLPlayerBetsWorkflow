
import duckdb
import pandas as pd
import os
import sys
from datetime import datetime

# Add current dir to path
sys.path.append(os.path.dirname(__file__))
from normalize_odds_schema import get_teams_from_slug, resolve_game_date, normalize_market, infer_side

DB_PATH = 'data/db/nhl_backtest.duckdb'
ODDS_FILE = 'data/raw/nhl_player_props_all.csv'

def ingest_odds():
    if not os.path.exists(ODDS_FILE):
        print(f"Odds file not found: {ODDS_FILE}")
        return

    con = duckdb.connect(DB_PATH)
    
    # Create table if not exists
    con.execute("DROP TABLE IF EXISTS fact_odds_props")
    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_odds_props (
        asof_ts TIMESTAMP,
        game_date DATE,
        book VARCHAR,
        market VARCHAR,
        line DOUBLE,
        player_name VARCHAR,
        player_id BIGINT,
        team VARCHAR,
        odds_decimal DOUBLE,
        side VARCHAR,
        source_file VARCHAR
    )
    """)
    
    print("Reading CSV...")
    df = pd.read_csv(ODDS_FILE)
    
    # Resolve Dates Cache
    slug_date_map = {}
    
    # Prepare rows
    rows_to_insert = []
    
    print("Processing rows...")
    # Group by game to minimize DB calls
    for slug in df['Game'].unique():
        away, home = get_teams_from_slug(slug)
        if not away:
            print(f"Could not parse slug: {slug}")
            continue
            
        g_date = resolve_game_date(con, away, home)
        if not g_date:
            print(f"Could not resolve date for {slug} ({away}@{home})")
            continue
            
        slug_date_map[slug] = g_date.date()
        # print(f"Mapped {slug} -> {g_date.date()}")

    # Iterate rows
    # We need to track pairs for side inference
    # Assume file is sorted by Game, Market
    
    current_market_key = None
    row_counter = 0 # 0 for first, 1 for second
    
    count = 0
    for idx, row in df.iterrows():
        slug = row['Game']
        market_str = row['Market']
        if slug not in slug_date_map:
            continue
            
        game_date = slug_date_map[slug]
        
        # Parse Market
        p_name, market_type, line, side_hint = normalize_market(market_str, row['Player'], row.get('Raw_Line'))
        
        if not p_name or not market_type:
            continue
            
        # Determine Side
        # Check if new market context
        market_key = (slug, market_str, p_name)
        if market_key != current_market_key:
            current_market_key = market_key
            row_counter = 0
        else:
            row_counter += 1
            
        side = infer_side(row['Odds_1'], market_type, is_first_row=(row_counter == 0))
        
        # Extract Odds
        try:
            odds = float(row['Odds_1'])
        except:
            continue
            
        # Player ID mapping (simple lookup in DB later, or we insert name and map downstream)
        # We'll insert name and map in the SQL View or Join.
        
        rows_to_insert.append((
            datetime.now(), # asof_ts (simulated ingestion time, or file mod time)
            game_date,
            'Consensus', # Book
            market_type,
            line,
            p_name,
            None, # player_id (map later)
            None, # team (unknown from CSV row, can infer from DB)
            odds,
            side,
            ODDS_FILE
        ))
        count += 1
        
    print(f"Prepared {len(rows_to_insert)} odds rows.")
    
    # Bulk Insert
    # We can create a DataFrame and use to_sql equivalent in duckdb appender
    if rows_to_insert:
        insert_df = pd.DataFrame(rows_to_insert, columns=[
            'asof_ts', 'game_date', 'book', 'market', 'line', 
            'player_name', 'player_id', 'team', 'odds_decimal', 'side', 'source_file'
        ])
        
        # Map Player IDs using dim_players
        # We fetch all players
        players_df = con.execute("SELECT player_id, player_name, team FROM dim_players").df()
        
        # Normalize names for join
        # Simple lowercase strip
        insert_df['norm_name'] = insert_df['player_name'].str.lower().str.strip()
        players_df['norm_name'] = players_df['player_name'].str.lower().str.strip()
        
        # Deduplicate players (take most recent team?)
        # For now just drop dupes on name
        players_dedup = players_df.drop_duplicates(subset=['norm_name'])
        
        merged = insert_df.merge(players_dedup[['norm_name', 'player_id', 'team']], on='norm_name', how='left')
        
        # Update original columns
        merged['player_id'] = merged['player_id_y']
        merged['team'] = merged['team_y'] # Use DB team
        
        # Select final cols
        final_df = merged[[
            'asof_ts', 'game_date', 'book', 'market', 'line', 
            'player_name', 'player_id', 'team', 'odds_decimal', 'side', 'source_file'
        ]]
        
        # Convert player_id to numeric (nullable)
        final_df['player_id'] = pd.to_numeric(final_df['player_id'], errors='coerce').astype('Int64')

        # Register and Insert
        con.register('df_odds_staging', final_df)
        con.execute("""
        INSERT INTO fact_odds_props (
            asof_ts, game_date, book, market, line, 
            player_name, player_id, team, odds_decimal, side, source_file
        )
        SELECT 
            asof_ts, game_date, book, market, line, 
            player_name, player_id, team, odds_decimal, side, source_file
        FROM df_odds_staging
        """)
        print("Inserted into DuckDB.")
        
    con.close()

if __name__ == "__main__":
    ingest_odds()

"""
PlayNow API Scraper
-------------------
This module fetches NHL player props directly from the PlayNow content-service API.
It replaces the legacy Selenium-based scraper with a faster, more reliable approach.

Chrome DevTools Capture:
1. Open Chrome DevTools (F12) -> Network tab.
2. Filter by "Fetch/XHR" and search for "content-service".
3. Look for "event-list" or "events-by-ids" requests.
4. Right-click the request -> Copy -> Copy as cURL to see full headers/cookies.
5. If cookies are required, set the PLAYNOW_COOKIE environment variable.

Usage:
    python src/nhl_bets/scrapers/scrape_playnow_api.py
"""

import os
import sys
import json
import logging
import datetime
import pandas as pd
import duckdb
import re

# Ensure project root is in path for nhl_bets import
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
src_dir = os.path.join(project_root, "src")
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from nhl_bets.scrapers.playnow_api_client import PlayNowAPIClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = 'data/db/nhl_backtest.duckdb'
RAW_RESPONSES_TABLE = 'raw_playnow_responses'
FACT_MARKETS_TABLE = 'fact_playnow_markets'

def slugify(text):
    return text.lower().replace(' at ', '-at-').replace(' ', '-').replace("'", "").replace(".", "")

def extract_market_rows(event):
    normalized_rows = []
    legacy_rows = []
    
    event_id = event['id']
    event_name = event['name']
    start_time_raw = event['startTime']
    game_slug = slugify(event_name)
    game_date = start_time_raw.split('T')[0]
    
    for market in event.get('markets', []):
        market_id = market['id']
        market_name = market['name']
        
        # Determine if it's a player prop market we care about
        stat_keyword = None
        if 'Total Shots on Goal' in market_name or 'Total Shots On Goal' in market_name: 
            stat_keyword = 'Shots On Goal'
        elif 'Total Points' in market_name: 
            stat_keyword = 'Points'
        elif 'Total Assists' in market_name: 
            stat_keyword = 'Assists'
        elif 'Total Blocks' in market_name or 'Total Blocked Shots' in market_name: 
            stat_keyword = 'Blocks'
        elif 'Total Powerplay Points' in market_name:
            stat_keyword = 'Powerplay Points'
        elif market_name == 'Player 1+ Goals': 
            stat_keyword = 'Goals'
        
        if not stat_keyword:
            continue

        for outcome in market.get('outcomes', []):
            outcome_id = outcome['id']
            outcome_name = outcome['name']
            
            price_decimal = None
            if outcome.get('prices'):
                price = outcome['prices'][0]
                price_decimal = price.get('decimal')
            
            # Normalized row for DuckDB
            normalized_rows.append({
                'event_id': event_id,
                'event_name': event_name,
                'start_time': start_time_raw,
                'market_id': market_id,
                'market_name': market_name,
                'outcome_id': outcome_id,
                'outcome_name': outcome_name,
                'price_decimal': price_decimal,
                'price_numerator': outcome['prices'][0].get('numerator') if outcome.get('prices') else None,
                'price_denominator': outcome['prices'][0].get('denominator') if outcome.get('prices') else None,
                'channel': 'I',
                'captured_at': datetime.datetime.now()
            })
            
            # Legacy CSV Rows
            if stat_keyword == 'Goals' and market_name == 'Player 1+ Goals':
                 legacy_rows.append({
                    'Game': game_slug,
                    'Market': 'Player 1+ Goals',
                    'Sub_Header': None,
                    'Player': outcome_name,
                    'Odds_1': price_decimal,
                    'Odds_2': None,
                    'Raw_Line': f"{outcome_name} {price_decimal}",
                    'Game_Date': game_date
                })
            
            elif stat_keyword in ['Shots On Goal', 'Points', 'Assists', 'Blocks', 'Powerplay Points']:
                line = None
                player = None
                sub_header = None
                
                # O/U format: "Player Name Total Points 0.5"
                if outcome_name.lower() in ['over', 'under']:
                    sub_header = outcome_name
                    line = market.get('handicapValue')
                    
                    # Split on " Total " to get player name
                    if ' Total ' in market_name:
                        player = market_name.split(' Total ')[0].strip()
                    else:
                        # Fallback regex if split fails for some reason
                        player_match = re.search(r'^(.*?)\s+Total\s+', market_name, re.IGNORECASE)
                        if player_match:
                            player = player_match.group(1).strip()
                
                # Maltese format: "3+"
                else:
                    maltese_match = re.match(r"(\d+)\+", outcome_name)
                    if maltese_match:
                        k = int(maltese_match.group(1))
                        line = k - 0.5
                        player = market_name.split(' Total ')[0].strip()
                        sub_header = None
                
                if player and line is not None:
                    legacy_market = f"{player} Total {stat_keyword} {line}"
                    legacy_rows.append({
                        'Game': game_slug,
                        'Market': legacy_market,
                        'Sub_Header': sub_header,
                        'Player': player,
                        'Odds_1': price_decimal,
                        'Odds_2': None,
                        'Raw_Line': f"{outcome_name} {price_decimal}",
                        'Game_Date': game_date
                    })

    return normalized_rows, legacy_rows

def main():
    client = PlayNowAPIClient()
    con = None
    try:
        con = duckdb.connect(DB_PATH)
        con.execute(f"CREATE TABLE IF NOT EXISTS {RAW_RESPONSES_TABLE} (captured_at TIMESTAMP, endpoint VARCHAR, request_url VARCHAR, payload_json JSON)")
        con.execute(f"CREATE TABLE IF NOT EXISTS {FACT_MARKETS_TABLE} (event_id VARCHAR, event_name VARCHAR, start_time TIMESTAMP, market_id VARCHAR, market_name VARCHAR, outcome_id VARCHAR, outcome_name VARCHAR, price_decimal DOUBLE, price_numerator INTEGER, price_denominator INTEGER, channel VARCHAR, captured_at TIMESTAMP)")
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
    
    try:
        # Fetch event list with multiple sorts to be thorough
        url, data = client.fetch_event_list(event_sorts="MTCH,TNMT")
        if con:
            con.execute(f"INSERT INTO {RAW_RESPONSES_TABLE} VALUES (current_timestamp, 'event-list', ?, ?)", [url, json.dumps(data)])
        
        events = data.get('data', {}).get('events', [])
        # Include events that have markets (marketCount > 0)
        # Sort events by marketCount descending so we process ones with props first
        events = sorted(events, key=lambda x: x.get('marketCount', 0), reverse=True)
        
        # We only care about events with more than a few markets (props games)
        event_ids = [e['id'] for e in events if e.get('marketCount', 0) > 5]
        
        if not event_ids:
            logger.warning("No events with props found.")
            pd.DataFrame(columns=['Game','Market','Sub_Header','Player','Odds_1','Odds_2','Raw_Line','Game_Date']).to_csv("nhl_player_props.csv", index=False)
            return

        url_det, data_det = client.fetch_event_details(event_ids)
        if con:
            con.execute(f"INSERT INTO {RAW_RESPONSES_TABLE} VALUES (current_timestamp, 'events-by-ids', ?, ?)", [url_det, json.dumps(data_det)])
        
        all_normalized_rows = []
        all_legacy_rows = []
        
        detailed_events = data_det.get('data', {}).get('events', [])
        for event in detailed_events:
            norm_rows, leg_rows = extract_market_rows(event)
            all_normalized_rows.extend(norm_rows)
            all_legacy_rows.extend(leg_rows)
                
        if con and all_normalized_rows:
            df_norm = pd.DataFrame(all_normalized_rows)
            con.append(FACT_MARKETS_TABLE, df_norm)
            
        csv_path = "nhl_player_props.csv"
        df_legacy = pd.DataFrame(all_legacy_rows) if all_legacy_rows else pd.DataFrame(columns=['Game','Market','Sub_Header','Player','Odds_1','Odds_2','Raw_Line','Game_Date'])
        df_legacy.to_csv(csv_path, index=False)
        logger.info(f"Summary: {len(detailed_events)} games processed. {len(all_legacy_rows)} prop lines captured.")

    except Exception as e:
        logger.error(f"Error during API scraping: {e}", exc_info=True)
    finally:
        if con:
            con.close()

if __name__ == "__main__":
    main()

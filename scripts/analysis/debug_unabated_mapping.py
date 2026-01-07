import os
import sys
import json
import duckdb
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

# Ensure project root is in path
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from nhl_bets.scrapers.unabated_client import UnabatedClient
from nhl_bets.common.db_init import DEFAULT_DB_PATH

def get_latest_unabated_file(root_dir: Path) -> Optional[Path]:
    raw_dir = root_dir / "outputs" / "odds" / "raw" / "UNABATED"
    if not raw_dir.exists():
        return None
    
    files = list(raw_dir.glob("**/*.json"))
    if not files:
        return None
    
    # Sort by modification time
    files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return files[0]

def debug_mapping(file_path: Path, 
                  event_id: Optional[str] = None, 
                  person_id: Optional[str] = None, 
                  bet_type_id: Optional[int] = None, 
                  points: Optional[float] = None,
                  market_source_id: Optional[str] = None):
    
    print(f"Loading Unabated snapshot: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    client = UnabatedClient()
    
    # Mocking capture_ts, raw_path, raw_hash for the parser
    capture_ts = datetime.fromtimestamp(file_path.stat().st_mtime)
    raw_path = str(file_path.relative_to(project_root))
    raw_hash = "DEBUG_HASH"
    
    people = data.get("people", {})
    market_sources = {str(ms["id"]): ms["name"] for ms in data.get("marketSources", [])}
    odds_dict = data.get("odds", {})
    
    pregame_props = odds_dict.get("lg6:pt1:pregame", [])
    
    found_count = 0
    for prop in pregame_props:
        p_event_id = str(prop.get("eventId"))
        p_person_id = str(prop.get("personId"))
        p_bet_type_id = prop.get("betTypeId")
        
        # Filter logic
        if event_id and p_event_id != event_id:
            continue
        if person_id and p_person_id != person_id:
            continue
        if bet_type_id and p_bet_type_id != bet_type_id:
            continue
            
        sides = prop.get("sides", {})
        
        for side_key, book_data in sides.items():
            for ms_key, price_data in book_data.items():
                p_ms_id = ms_key.replace("ms", "")
                p_points = price_data.get("points")
                
                if market_source_id and p_ms_id != market_source_id:
                    continue
                if points is not None and p_points != points:
                    continue
                
                found_count += 1
                if found_count > 10:
                    print("... (showing first 10 matches)")
                    return

                print("-" * 40)
                print(f"MATCH {found_count}:")
                print(f"RAW DATA:")
                print(f"  eventId: {p_event_id}")
                print(f"  personId: {p_person_id}")
                print(f"  betTypeId: {p_bet_type_id} ({client.BET_TYPE_MAP.get(p_bet_type_id, 'UNKNOWN')})")
                print(f"  sideKey: {side_key}")
                print(f"  marketSourceId: {p_ms_id} ({market_sources.get(p_ms_id, 'UNKNOWN')})")
                print(f"  points: {p_points}")
                print(f"  americanPrice: {price_data.get('americanPrice')}")
                
                # Manual call to parser logic or similar
                if side_key.startswith("si0"):
                    side = "OVER"
                elif side_key.startswith("si1"):
                    side = "UNDER"
                else:
                    side = "UNKNOWN"
                price_american = price_data.get("americanPrice")
                odds_decimal = None
                if price_american is not None:
                    if price_american > 0:
                        odds_decimal = (price_american / 100) + 1
                    elif price_american < 0:
                        odds_decimal = (100 / abs(price_american)) + 1

                print(f"MAPPED FIELDS:")
                print(f"  market_type: {client.BET_TYPE_MAP.get(p_bet_type_id)}")
                print(f"  line: {p_points}")
                print(f"  side: {side}")
                print(f"  odds_american: {price_american}")
                print(f"  odds_decimal: {odds_decimal:.4f}" if odds_decimal else "  odds_decimal: None")

                # Check DuckDB
                con = duckdb.connect(str(project_root / DEFAULT_DB_PATH))
                try:
                    query = """
                    SELECT source_vendor, capture_ts_utc, player_name_raw, market_type, line, side, book_name_raw, odds_american 
                    FROM fact_prop_odds 
                    WHERE source_vendor = 'UNABATED'
                      AND event_id_vendor = ?
                      AND player_id_vendor = ?
                      AND vendor_bet_type_id = ?
                      AND line = ?
                      AND side = ?
                      AND vendor_market_source_id = ?
                    ORDER BY capture_ts_utc DESC
                    LIMIT 5
                    """
                    rows = con.execute(query, [p_event_id, p_person_id, p_bet_type_id, p_points, side, p_ms_id]).fetchall()
                    if rows:
                        print(f"DUCKDB ROWS (Last 5):")
                        for row in rows:
                            print(f"  {row}")
                    else:
                        print(f"DUCKDB ROWS: None found for these specific keys.")
                finally:
                    con.close()

    if found_count == 0:
        print("No matches found for the given filters.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Debug Unabated mapping forensics.")
    parser.add_argument("--file", type=str, help="Path to a specific Unabated JSON file.")
    parser.add_argument("--event-id", type=str, help="Filter by eventId.")
    parser.add_argument("--person-id", type=str, help="Filter by personId.")
    parser.add_argument("--bet-type-id", type=int, help="Filter by betTypeId.")
    parser.add_argument("--points", type=float, help="Filter by points/line.")
    parser.add_argument("--ms-id", type=str, help="Filter by marketSourceId.")
    
    args = parser.parse_args()
    
    if args.file:
        target_file = Path(args.file)
    else:
        target_file = get_latest_unabated_file(project_root)
        
    if not target_file or not target_file.exists():
        print("Error: No Unabated snapshot file found.")
        sys.exit(1)
        
    debug_mapping(
        target_file,
        event_id=args.event_id,
        person_id=args.person_id,
        bet_type_id=args.bet_type_id,
        points=args.points,
        market_source_id=args.ms_id
    )

import os
import sys
import pandas as pd
from pathlib import Path

def main():
    # Determine paths
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent
    data_root = project_root / "data" / "raw" / "moneypuck"
    report_path = project_root / "outputs" / "backtest_reports" / "moneypuck_data_inventory.csv"
    
    print(f"Validating data in {data_root}")
    
    inventory_rows = []
    
    # 1. Validate Lookup
    lookup_file = data_root / "allPlayersLookup.csv"
    if lookup_file.exists():
        try:
            df = pd.read_csv(lookup_file)
            print(f"allPlayersLookup.csv: Found. Rows: {len(df)}")
            print(f"  Columns: {', '.join(df.columns[:5])}...")
        except Exception as e:
             print(f"allPlayersLookup.csv: Error reading file. {e}")
    else:
        print("allPlayersLookup.csv: MISSING")

    # 2. Validate Game By Game
    gbg_root = data_root / "teamPlayerGameByGame"
    
    if not gbg_root.exists():
        print(f"Game by game directory not found at {gbg_root}")
        return

    # Walk through the directory structure
    # Structure: season / type / group / file.csv
    
    # Get all season directories
    seasons = [d for d in gbg_root.iterdir() if d.is_dir()]
    seasons.sort()
    
    for season_dir in seasons:
        season_name = season_dir.name
        
        # We expect a 'regular' folder inside, but let's just look for any type folders
        type_dirs = [d for d in season_dir.iterdir() if d.is_dir()]
        
        for type_dir in type_dirs:
            season_type = type_dir.name # e.g., regular
            
            # Inside are groups: skaters, goalies
            group_dirs = [d for d in type_dir.iterdir() if d.is_dir()]
            
            for group_dir in group_dirs:
                group_name = group_dir.name
                
                csv_files = list(group_dir.glob("*.csv"))
                file_count = len(csv_files)
                total_bytes = sum(f.stat().st_size for f in csv_files)
                
                sample_cols = ""
                sample_rows = 0
                
                if file_count > 0:
                    # Sample first file
                    sample_file = csv_files[0]
                    try:
                        df = pd.read_csv(sample_file)
                        sample_cols = ", ".join(df.columns.tolist())
                        sample_rows = len(df)
                        
                        # Check required keys
                        required_keys_options = {
                            "playerId": ["playerId"],
                            "team": ["team", "playerTeam"],
                            "gameDate": ["gameDate"],
                            "situation": ["situation"]
                        }
                        
                        if group_name == "goalies":
                            # Goalies might not have situation in all files or it might be named differently
                            required_keys_options.pop("situation", None)

                        missing = []
                        for key, options in required_keys_options.items():
                            if not any(opt in df.columns for opt in options):
                                missing.append(key)
                        
                        # Game ID check: gameId OR game_id
                        if "gameId" not in df.columns and "game_id" not in df.columns:
                            missing.append("gameId")

                        if missing:
                             print(f"  WARNING: {season_name}/{group_name} missing keys: {missing}")

                    except Exception as e:
                        print(f"  Error reading sample {sample_file}: {e}")
                
                inventory_rows.append({
                    "season": season_name,
                    "group": group_name,
                    "file_count": file_count,
                    "total_bytes": total_bytes,
                    "sample_columns": sample_cols,
                    "sample_row_count": sample_rows
                })
                
                print(f"  {season_name} - {group_name}: {file_count} files")

    # Write report
    if inventory_rows:
        report_df = pd.DataFrame(inventory_rows)
        report_df.to_csv(report_path, index=False)
        print(f"\nReport written to {report_path}")
    else:
        print("\nNo data found to report.")

if __name__ == "__main__":
    main()

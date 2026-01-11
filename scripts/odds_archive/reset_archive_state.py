import pandas as pd
from pathlib import Path
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parents[2]))

from src.odds_archive import config, io

def main():
    print("Resetting URL Lake status to 'fetched'...")
    url_lake = io.load_url_lake()
    if not url_lake.empty:
        # Reset all 'parsed' to 'fetched' so we can re-parse with new logic
        url_lake.loc[url_lake["status"] == "parsed", "status"] = "fetched"
        io.save_url_lake(url_lake)
        print(f"Reset {len(url_lake)} URLs.")

    print("Clearing raw props...")
    if config.RAW_PROPS_PATH.exists():
        # Backup? No, just clear for this forensic run
        config.RAW_PROPS_PATH.unlink()
        print("Deleted props_odds_raw.jsonl")

if __name__ == "__main__":
    main()

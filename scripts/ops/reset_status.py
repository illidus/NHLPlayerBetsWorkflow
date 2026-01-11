import pandas as pd
from odds_archive import io

def main():
    df = io.load_url_lake()
    targets = ["www.pickswise.com"]
    
    print("Status before reset:")
    print(df[df['source'].isin(targets)]['status'].value_counts())
    
    # Reset 'fetched' to 'new' for targets
    mask = (df['source'].isin(targets)) & (df['status'] == 'fetched')
    count = mask.sum()
    df.loc[mask, 'status'] = 'new'
    
    # Also reset 'parsed' if any, just in case
    mask_parsed = (df['source'].isin(targets)) & (df['status'] == 'parsed')
    count_parsed = mask_parsed.sum()
    df.loc[mask_parsed, 'status'] = 'new'
    
    print(f"Reset {count} fetched and {count_parsed} parsed URLs to 'new'.")
    
    io.save_url_lake(df)
    io.write_duckdb_table("fact_odds_archive_url_lake", df)

if __name__ == "__main__":
    main()

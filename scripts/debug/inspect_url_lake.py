import pandas as pd
from odds_archive import config

def inspect():
    df = pd.read_parquet(config.URL_LAKE_PATH)
    print(f"Total URLs: {len(df)}")
    
    # Props specific
    nhl_props = df[df['url'].str.contains('nhl', case=False) & df['url'].str.contains('prop', case=False)]
    print(f"NHL Prop URLs: {len(nhl_props)}")
    
    if not nhl_props.empty:
        print("\nSample NHL Prop URLs:")
        for u in nhl_props['url'].head(20).tolist():
            print(f"  {u}")
            
    # Picks specific
    nhl_picks = df[df['url'].str.contains('nhl', case=False) & df['url'].str.contains('pick', case=False)]
    print(f"\nNHL Pick URLs: {len(nhl_picks)}")
    
    # Filtered articles (excluding calculators, bonus codes, etc)
    exclude = ["bonus-code", "promo-code", "legal-online", "calculator", "/video/", "/tag/", "/category/", "/author/"]
    filtered = nhl_props.copy()
    for ex in exclude:
        filtered = filtered[~filtered['url'].str.contains(ex, case=False)]
    
    print(f"\nFiltered NHL Prop Articles: {len(filtered)}")
    if not filtered.empty:
        print("\nSample Filtered NHL Prop Articles:")
        for u in filtered['url'].head(20).tolist():
            print(f"  {u}")

if __name__ == "__main__":
    inspect()
import pandas as pd
from odds_archive import io

def main():
    df = io.load_url_lake()
    
    # Strict filter for NHL prop-related URLs
    # Looking for 'nhl' AND ('prop' OR 'pick' OR 'bet' OR 'prediction') 
    # but excluding obviously non-NHL terms
    nhl_mask = df['url'].str.contains("nhl", case=False)
    prop_mask = df['url'].str.contains("prop|pick|bet|prediction", case=False)
    
    nhl_props = df[nhl_mask & prop_mask & (df['status'] == 'new')]
    
    print("--- New NHL Prop URL Candidates ---")
    print(nhl_props['source'].value_counts())
    
    print("\nSample URLs:")
    print(nhl_props['url'].head(10))

if __name__ == "__main__":
    main()


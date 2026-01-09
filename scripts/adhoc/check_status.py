import pandas as pd
from odds_archive import io

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

def main():
    df = io.load_url_lake()
    an_df = df[(df['url'].str.contains("actionnetwork.com/nhl")) & (df['status'] == 'error')]
    
    if not an_df.empty:
        print("\nSample errors:")
        print(an_df[['url', 'error']].head(10))

if __name__ == "__main__":
    main()
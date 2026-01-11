import pandas as pd
from odds_archive import io

pd.set_option('display.max_colwidth', None)

def main():
    df = io.load_pages()
    # Filter for a specific URL that yielded 0 candidates (from my previous log observation)
    # The log said: "Parsed 0 candidates from https://www.pickswise.com/news/fanduel-promo-code-for-nhl-on-tnt-today-bet-5-get-150-in-bonuses/"
    # Let's try to find an Action Network one which is more likely to have props but might be missed.
    # I'll just look for any actionnetwork url in the df
    
    an_df = df[df['url'].str.contains("actionnetwork.com/nhl")]
    if not an_df.empty:
        row = an_df.iloc[0]
        print(f"\n--- URL: {row['url']} ---")
        print(row['extracted_text'])
    else:
        print("No Action Network pages found in pages.parquet")

if __name__ == "__main__":
    main()


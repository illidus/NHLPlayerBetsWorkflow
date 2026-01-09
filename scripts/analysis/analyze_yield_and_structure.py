import pandas as pd
from odds_archive import io

pd.set_option('display.max_colwidth', None)

def main():
    # 1. Baseline Yield
    props_df = pd.read_parquet("data/odds_archive/props_odds.parquet")
    print("--- Baseline Yield by Domain ---")
    if not props_df.empty:
        print(props_df['source'].value_counts())
        print(f"Total: {len(props_df)}")
    else:
        print("No props found.")

    # 2. Inspect Text Structure
    pages_df = io.load_pages()
    targets = ["www.covers.com", "www.thelines.com"]
    
    for domain in targets:
        print(f"\n--- Inspecting Text for {domain} ---")
        subset = pages_df[pages_df['source'] == domain]
        if subset.empty:
            print("No pages found.")
            continue
            
        sample = subset.sample(n=min(3, len(subset)))
        for idx, row in sample.iterrows():
            text = row['extracted_text']
            if not text:
                continue
            print(f"URL: {row['url']}")
            print(f"Text Sample (first 500 chars):\n{repr(text[:500])}")
            # Check newline distribution
            newlines = text.count('\n')
            print(f"Length: {len(text)}, Newlines: {newlines}")
            print("-" * 20)

if __name__ == "__main__":
    main()

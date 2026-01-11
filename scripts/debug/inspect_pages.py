import pandas as pd
from odds_archive import io

pd.set_option('display.max_colwidth', None)

def main():
    df = io.load_pages()
    print(f"Total pages: {len(df)}")
    
    # Filter for non-empty extracted text
    df = df[df["extracted_text"].notna() & (df["extracted_text"] != "")]
    print(f"Pages with text: {len(df)}")
    
    if df.empty:
        print("No pages with extracted text found.")
        return

    sample = df.sample(n=min(5, len(df)))
    
    for idx, row in sample.iterrows():
        print(f"\n--- URL: {row['url']} ---")
        print(row['extracted_text'][:2000]) # Print first 2000 chars

if __name__ == "__main__":
    main()


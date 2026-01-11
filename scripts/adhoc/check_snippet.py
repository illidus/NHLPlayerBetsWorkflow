import pandas as pd
from odds_archive import io

pd.set_option('display.max_colwidth', None)

def main():
    df = io.load_pages()
    print(f"Total pages: {len(df)}")
    
    # Check html_snippet
    with_snippet = df[df['html_snippet'].notna() & (df['html_snippet'] != "")]
    print(f"Pages with html_snippet: {len(with_snippet)}")
    
    if not with_snippet.empty:
        sample = with_snippet.sample(1).iloc[0]
        print(f"\nURL: {sample['url']}")
        print(f"Snippet length: {len(sample['html_snippet'])}")
        print(f"Snippet preview: {sample['html_snippet'][:500]}")
    else:
        print("No HTML snippets found.")

if __name__ == "__main__":
    main()


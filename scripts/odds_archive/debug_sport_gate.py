import pandas as pd
from pathlib import Path
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parents[2]))

from src.odds_archive import config, parsers, io

def main():
    print("Loading pages...")
    pages = io.load_pages().head(50)
    
    print(f"Loaded {len(pages)} pages.")
    
    # Load players explicitly (parsers does it on import but let's confirm)
    print(f"Loaded {len(parsers.NHL_PLAYERS)} players.")

    for i, page in pages.iterrows():
        title = page["title"] or ""
        text = page["extracted_text"] or ""
        content = title + " " + text
        
        is_nhl = parsers.is_nhl_content(content)
        print(f"URL: {page['url']}")
        print(f"Title: {title}")
        print(f"Is NHL: {is_nhl}")
        
        if not is_nhl:
            # Why?
            # Check negative
            neg_found = [kw for kw in config.NEGATIVE_KEYWORDS if kw in content.lower()]
            if neg_found:
                print(f"  REJECTED due to NEGATIVE keywords: {neg_found}")
            else:
                # Check positive
                pos_found = [team for team in config.NHL_TEAMS if team in content.lower()]
                player_found = [p for p in parsers.NHL_PLAYERS if p.lower() in content.lower()]
                if not pos_found and not player_found:
                     print("  REJECTED due to NO POSITIVE match.")
                else:
                     print(f"  Odd.. should have passed? Teams: {pos_found}, Players: {player_found}")
        print("-" * 20)

if __name__ == "__main__":
    main()

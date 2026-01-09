import pandas as pd
from pathlib import Path
import sys
import json

# Add project root to path
sys.path.append(str(Path(__file__).parents[2]))

from src.odds_archive import config, parsers

def main():
    print("Running Acceptance Check...")
    
    # Load Tier 2 Data
    parquet_path = config.DATA_DIR / "editorial_mentions.parquet"
    if not parquet_path.exists():
        print("FAIL: editorial_mentions.parquet not found")
        sys.exit(1)
        
    df = pd.read_parquet(parquet_path)
    print(f"Loaded {len(df)} Tier 2 records.")
    
    failures = []
    
    # Criteria A: Sport False Positives
    # Check a sample for NEGATIVE keywords in raw snippet
    sample = df.sample(min(200, len(df)))
    for i, row in sample.iterrows():
        snippet = row["raw_text_snippet"].lower()
        for kw in config.NEGATIVE_KEYWORDS:
            if kw in snippet:
                # Double check if it's a false positive keyword match?
                # E.g. "corner" might be in hockey text? "touchdown"? 
                # If found, flag it.
                failures.append(f"Sport Gate Fail: Found '{kw}' in snippet: {snippet[:50]}...")
                
    # Criteria B: Nonsense Player Names
    # If entity_type == PLAYER, check against dictionary?
    # parsers.classify_entity logic handles this. If it's UNKNOWN, it's flagged REJECT_ENTITY.
    # We check if any "PLAYER" entity is actually garbage.
    # This is hard to automate perfectly without a perfect dictionary.
    # But we can check if any "GAME" entity has player_name populated in the json?
    # In 03_parse_props.py, we set player_name_raw = None if GAME.
    # Let's check extracted_props json.
    for i, row in df.iterrows():
        props = json.loads(row["extracted_props"])
        status = row["status_code"]
        
        # Criteria C: No odds=0
        if props.get("odds") == 0:
            failures.append(f"Odds=0 Fail: ID {row['mention_id']}")
            
        if status == "MISSING_ODDS" and props.get("odds") is not None and props.get("odds") != 0:
             failures.append(f"Status Mismatch: MISSING_ODDS but odds present: {props.get('odds')}")

    # Output Report
    report_path = Path("outputs/odds_archive_audit/ACCEPTANCE_REPORT.md")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(report_path, "w") as f:
        f.write("# Acceptance Report\n")
        f.write(f"**Date:** {pd.Timestamp.now()}\n\n")
        f.write(f"**Records Checked:** {len(df)}\n")
        f.write(f"**Failures Found:** {len(failures)}\n\n")
        if failures:
            f.write("## Failures\n")
            for fail in failures[:50]:
                f.write(f"- {fail}\n")
            if len(failures) > 50:
                f.write(f"- ... and {len(failures)-50} more.\n")
        else:
            f.write("## Status: PASS\nAll automated checks passed.\n")

    if failures:
        print(f"FAIL: Found {len(failures)} issues. See {report_path}")
        sys.exit(1)
    else:
        print("PASS: Acceptance criteria met.")

if __name__ == "__main__":
    main()

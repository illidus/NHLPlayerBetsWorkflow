import pandas as pd
import numpy as np
import os
import random
from datetime import datetime

# Configuration
PROPS_PATH = 'data/odds_archive/props_odds.parquet'
PAGES_PATH = 'data/odds_archive/pages.parquet'
OUTPUT_DIR = 'outputs/odds_archive_audit'
REPORT_HTML = os.path.join(OUTPUT_DIR, 'verification_report.html')
REPORT_CSV = os.path.join(OUTPUT_DIR, 'verification_report.csv')
SUMMARY_MD = os.path.join(OUTPUT_DIR, 'verification_summary.md')

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_data():
    print(f"Loading data from {PROPS_PATH} and {PAGES_PATH}...")
    props = pd.read_parquet(PROPS_PATH)
    pages = pd.read_parquet(PAGES_PATH)
    return props, pages

def get_stratified_sample(df, n_target=100):
    # Stratify by source and market
    # specific logic: ensure we get a mix of lines if possible, but source/market is primary
    
    # Filter for interesting markets if they exist (as per prompt)
    interesting_markets = ['GOALS', 'ASSISTS', 'POINTS', 'SOG', 'BLOCKS', 'ANYTIME_GOAL']
    # Normalize market names to upper just in case
    df['market'] = df['market'].str.upper()
    
    # Filter only relevant markets if we have enough data, otherwise keep all to reach count
    df_relevant = df[df['market'].isin(interesting_markets)]
    if len(df_relevant) < 50: # Fallback if filtering removes too much
        df_relevant = df
    
    # Stratified sampling
    # We want roughly equal representation from each source
    sources = df_relevant['source'].unique()
    per_source_target = n_target // len(sources) if len(sources) > 0 else n_target
    
    sample_indices = []
    
    for source in sources:
        source_df = df_relevant[df_relevant['source'] == source]
        
        # Within source, stratify by market
        markets = source_df['market'].unique()
        per_market_target = max(1, per_source_target // len(markets))
        
        for market in markets:
            market_df = source_df[source_df['market'] == market]
            
            # Simple random sample within this bucket
            n_sample = min(len(market_df), per_market_target)
            # Try to get diverse lines if possible
            sampled = market_df.sample(n=n_sample, random_state=42)
            sample_indices.extend(sampled.index.tolist())
            
    # If we undershot, fill up with random rows from the rest
    if len(sample_indices) < 75:
        remaining = df.drop(sample_indices)
        needed = 75 - len(sample_indices)
        if len(remaining) > 0:
            extra = remaining.sample(n=min(len(remaining), needed), random_state=42)
            sample_indices.extend(extra.index.tolist())
            
    # If we overshot (unlikely with this logic but possible if many markets), trim
    # But prompt says 75-150 is fine.
    
    final_sample = df.loc[sample_indices].copy()
    
    # Cap at 150 just in case
    if len(final_sample) > 150:
        final_sample = final_sample.sample(n=150, random_state=42)
        
    return final_sample

def extract_context(row, pages_df):
    url = row['source_url']
    snippet = row.get('snippet', '')
    
    # Find page content
    page_row = pages_df[pages_df['url'] == url]
    
    if page_row.empty:
        # Try canonical if available? source_url is the join key
        return None, False
    
    full_text = page_row.iloc[0]['extracted_text']
    if not isinstance(full_text, str):
        return None, False
        
    if not isinstance(snippet, str) or not snippet:
        return None, False
        
    # Find snippet in text
    idx = full_text.find(snippet)
    if idx != -1:
        start = max(0, idx - 400)
        end = min(len(full_text), idx + len(snippet) + 400)
        excerpt = full_text[start:end]
        # Highlight logic could go here, but raw text is requested
        return excerpt, True
    else:
        # Fallback: maybe just return a chunk of text? 
        # Prompt says "if found". If not found, maybe return first 500 chars?
        # But specifically asks for "around the odds snippet".
        return None, False

def generate_html(df, stats):
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>NHL Odds Archive Verification Report</title>
        <style>
            body {{ font-family: sans-serif; margin: 20px; background-color: #f4f4f9; }}
            .card {{ background: white; border: 1px solid #ddd; padding: 15px; margin-bottom: 15px; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }}
            .header {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #eee; padding-bottom: 10px; margin-bottom: 10px; }}
            .header-main {{ font-weight: bold; font-size: 1.1em; }}
            .header-sub {{ color: #666; font-size: 0.9em; }}
            .details {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; font-size: 0.9em; }}
            .evidence {{ margin-top: 15px; border-top: 1px dashed #eee; padding-top: 10px; }}
            .evidence-section {{ margin-bottom: 10px; }}
            .evidence-label {{ font-weight: bold; font-size: 0.8em; color: #555; text-transform: uppercase; }}
            .evidence-content {{ background: #f8f8f8; padding: 8px; border-radius: 4px; font-family: monospace; font-size: 0.85em; white-space: pre-wrap; word-break: break-all; max-height: 200px; overflow-y: auto; }}
            .checklist {{ margin-top: 15px; background: #eef; padding: 10px; border-radius: 4px; display: flex; gap: 20px; align-items: center; }}
            .pass-fail {{ font-weight: bold; }}
            a {{ color: #007bff; text-decoration: none; }}
            a:hover {{ text-decoration: underline; }}
            .summary-box {{ background: #fff; padding: 20px; border: 1px solid #ccc; margin-bottom: 30px; }}
        </style>
    </head>
    <body>
        <div class="summary-box">
            <h1>Verification Report</h1>
            <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><strong>Total Rows:</strong> {stats['total_rows']}</p>
            <p><strong>Match Rate (Snippet in Text):</strong> {stats['match_rate']:.1f}%</p>
        </div>
    """
    
    for idx, row in df.iterrows():
        # Helpers
        matchup = row.get('matchup_text_raw', 'N/A')
        if pd.isna(matchup): matchup = "N/A"
        
        player = row.get('player_name_clean', row.get('player_name_raw', 'Unknown'))
        market = row.get('market', 'Unknown')
        line = row.get('line', '')
        side = row.get('side', '')
        odds = row.get('odds', '')
        source = row.get('source', 'Unknown')
        url = row.get('source_url', '#')
        
        # Evidence
        snippet = row.get('snippet', '')
        extracted = row.get('extracted_text_excerpt', '')
        found = row.get('price_text_found', False)
        
        found_badge = '<span style="color:green">✓ Found</span>' if found else '<span style="color:red">✗ Not Found</span>'
        
        html += f"""
        <div class="card">
            <div class="header">
                <div class="header-main">
                    {player} | {market} {side} {line} @ {odds}
                </div>
                <div class="header-sub">
                    <a href="{url}" target="_blank">{source}</a>
                </div>
            </div>
            
            <div class="details">
                <div>
                    <strong>Matchup:</strong> {matchup}<br>
                    <strong>Date:</strong> {row.get('game_date', 'N/A')}<br>
                    <strong>Team:</strong> {row.get('player_team', 'N/A')}
                </div>
                <div>
                    <strong>Bookmaker:</strong> {row.get('bookmaker', 'N/A')}<br>
                    <strong>Publish TS:</strong> {row.get('publish_ts', 'N/A')}<br>
                    <strong>Crawl TS:</strong> {row.get('crawl_ts', 'N/A')}
                </div>
            </div>
            
            <div class="evidence">
                <div class="evidence-section">
                    <div class="evidence-label">Raw Snippet (price_text_raw)</div>
                    <div class="evidence-content">{snippet}</div>
                </div>
                
                <div class="evidence-section">
                    <div class="evidence-label">Extracted Text Context {found_badge}</div>
                    <div class="evidence-content">{extracted if extracted else "(No context found around snippet in extracted page text)"}</div>
                </div>
            </div>
            
            <div class="checklist">
                <span class="pass-fail">Verify:</span>
                <label><input type="checkbox"> PASS</label>
                <label><input type="checkbox"> FAIL</label>
                <label><input type="checkbox"> CHANGED</label>
                <label><input type="checkbox"> PARSE_ERR</label>
            </div>
        </div>
        """
        
    html += "</body></html>"
    return html

def main():
    props, pages = load_data()
    
    # 2. Stratified Sample
    print("Sampling...")
    sample = get_stratified_sample(props, n_target=100)
    print(f"Sample size: {len(sample)}")
    
    # 3. Join & Extract
    print("Extracting context...")
    
    excerpts = []
    found_flags = []
    
    for idx, row in sample.iterrows():
        excerpt, found = extract_context(row, pages)
        excerpts.append(excerpt)
        found_flags.append(found)
        
    sample['extracted_text_excerpt'] = excerpts
    sample['price_text_found'] = found_flags
    # Mapping snippet to price_text_raw for the report requirements
    sample['price_text_raw'] = sample['snippet']
    sample['context_text_raw'] = "" # Placeholder as discussed
    
    # 4. Generate Stats
    stats = {
        'total_rows': len(sample),
        'by_source': sample['source'].value_counts().to_dict(),
        'by_market': sample['market'].value_counts().to_dict(),
        'match_rate': (sum(found_flags) / len(sample) * 100) if len(sample) > 0 else 0,
        'has_publish_ts': (sample['publish_ts'].notna().sum() / len(sample) * 100),
        'has_bookmaker': (sample['bookmaker'].notna().sum() / len(sample) * 100)
    }
    
    # 5. Write CSV
    print(f"Writing CSV to {REPORT_CSV}...")
    sample.to_csv(REPORT_CSV, index=False)
    
    # 6. Write HTML
    print(f"Writing HTML to {REPORT_HTML}...")
    html_content = generate_html(sample, stats)
    with open(REPORT_HTML, 'w', encoding='utf-8') as f:
        f.write(html_content)
        
    # 7. Write Summary MD
    print(f"Writing Summary to {SUMMARY_MD}...")
    md_content = f"""# Verification Report Summary
**Date:** {datetime.now().strftime('%Y-%m-%d')}
**Total Rows Reviewed:** {stats['total_rows']}

## Breakdown by Source
{pd.Series(stats['by_source']).to_markdown()}

## Breakdown by Market
{pd.Series(stats['by_market']).to_markdown()}

## Data Quality Stats
- **Publish TS Present:** {stats['has_publish_ts']:.1f}%
- **Bookmaker Present:** {stats['has_bookmaker']:.1f}%
- **Snippet Found in Page Text:** {stats['match_rate']:.1f}%

## Caveats
- `game_date`, `away_team`, `home_team` are currently null in the raw parquet (Phase 11 ingestion in progress).
- Pickswise lines may update intra-day; verify against `publish_ts`.
"""
    with open(SUMMARY_MD, 'w', encoding='utf-8') as f:
        f.write(md_content)
        
    print("Done.")

if __name__ == "__main__":
    main()

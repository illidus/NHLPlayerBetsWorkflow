import requests
import json
from pathlib import Path
from datetime import datetime

OUTPUT_DIR = Path("outputs/odds_archive_audit")
EXAMPLES_DIR = OUTPUT_DIR / "examples"
EXAMPLES_DIR.mkdir(parents=True, exist_ok=True)

URLS = [
    {
        "url": "https://www.pickswise.com/news/best-player-prop-bets-for-thursdays-nhl-slate-zibanejad-looking-to-dominate-philadelphia-again/",
        "classification_expected": "NHL_PASS",
        "detected_source": "pickswise"
    },
    {
        "url": "https://www.thelines.com/best-nhl-player-props-parlay-picks-today-nov-21/",
        "classification_expected": "NHL_PASS",
        "detected_source": "thelines"
    },
    {
        "url": "https://www.pickswise.com/news/best-nhl-player-prop-bets-today-0101-timo-meier-artemi-panarin/",
        "classification_expected": "NHL_PASS",
        "detected_source": "pickswise"
    },
    {
        "url": "https://www.covers.com/nfl/mnf-odds",
        "classification_expected": "REJECT_NON_NHL",
        "detected_source": "covers"
    },
    {
        "url": "https://www.pickswise.com/news/nhl-thursday-parlay-at-mega-879-odds-today-1-12-gambling-on-golden-knights-in-vegas/",
        "classification_expected": "GAME_TOTAL", # Or Mixed, but mostly Game content
        "detected_source": "pickswise"
    },
    {
        "url": "https://www.covers.com/nba/warriors-vs-clippers-prediction-picks-best-bets-sgp-monday-1-5-2026",
        "classification_expected": "REJECT_NON_NHL",
        "detected_source": "covers"
    }
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive"
}

def main():
    results = []
    
    for item in URLS:
        url = item["url"]
        print(f"Fetching {url}...")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            
            slug = url.split("/")[-2] if url.endswith("/") else url.split("/")[-1]
            slug = slug[:50] # truncated
            
            # Save snapshots
            html_path = EXAMPLES_DIR / f"{slug}.html"
            txt_path = EXAMPLES_DIR / f"{slug}.txt"
            
            html_path.write_bytes(resp.content)
            txt_path.write_text(resp.text[:5000], encoding="utf-8") # Sample text
            
            item["crawl_ts"] = datetime.utcnow().isoformat()
            item["status_code"] = resp.status_code
            item["headers_sent"] = HEADERS
            item["output_html"] = str(html_path)
            item["output_txt"] = str(txt_path)
            
            results.append(item)
            
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            item["error"] = str(e)
            results.append(item)

    # Save JSON
    json_path = OUTPUT_DIR / "manual_examples.json"
    json_path.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    
    # Save MD
    md_path = OUTPUT_DIR / "manual_examples.md"
    with open(md_path, "w") as f:
        f.write("# Manual Validation Evidence Pack\n\n")
        f.write("| URL | Expected Classification | Source |\n")
        f.write("| :--- | :--- | :--- |\n")
        for item in results:
            f.write(f"| {item['url']} | {item['classification_expected']} | {item['detected_source']} |\n")
            
        f.write("\n## Curl Commands\n")
        for item in results:
            f.write(f"### {item['url']}\n")
            f.write("```bash\n")
            f.write(f"curl '{item['url']}' \
")
            for k, v in HEADERS.items():
                f.write(f"  -H '{k}: {v}' \
")
            f.write("  --compressed\n")
            f.write("```\n")

if __name__ == "__main__":
    main()

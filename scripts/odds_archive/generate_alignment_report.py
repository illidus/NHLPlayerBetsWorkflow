import duckdb
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parents[2]))

from src.odds_archive import config

def get_latest_parse_summary():
    # Find latest parse summary
    summaries = list(config.RUN_LOGS_DIR.glob("parse_*_summary.json"))
    if not summaries:
        return {}
    latest = max(summaries, key=lambda p: p.stat().st_mtime)
    return json.loads(latest.read_text(encoding="utf-8"))

def generate_report():
    con = duckdb.connect(str(config.ODDS_ARCHIVE_DB_PATH))
    
    # 1. Get Metrics
    summary = get_latest_parse_summary()
    total_pages = summary.get("pages_parsed", 0)
    rejected_non_nhl = summary.get("rejected_non_nhl", 0)
    
    # Tier 2 Stats
    try:
        tier2_df = con.execute("SELECT * FROM raw_editorial_mentions").df()
        status_counts = tier2_df["status_code"].value_counts().to_dict()
    except Exception as e:
        print(f"Error reading raw_editorial_mentions: {e}")
        tier2_df = pd.DataFrame()
        status_counts = {}

    missing_odds = status_counts.get("MISSING_ODDS", 0)
    ambiguous_date = status_counts.get("AMBIGUOUS_DATE", 0) # Not implemented yet but good to have
    candidate_ready = status_counts.get("CANDIDATE_READY", 0)
    
    # Check Contamination
    # Assuming fact_prop_odds exists. If not, 0.
    contamination_count = 0
    try:
        # Check for rows with source_vendor that implies editorial, or just recent rows if we know only editorial ran
        # Editorial uses 'source' column in tier 2, but in fact_prop_odds it would be source_vendor.
        # Ideally, we query for source_vendor NOT IN ('UNABATED', 'PLAYNOW', 'ODDSSHARK')
        # But wait, fact_prop_odds might be empty if Phase 11 hasn't populated it yet.
        # Let's check for any row that might be editorial.
        contamination_df = con.execute("""
            SELECT count(*) as cnt 
            FROM fact_prop_odds 
            WHERE source_vendor NOT IN ('UNABATED', 'PLAYNOW', 'ODDSSHARK')
        """).df()
        contamination_count = contamination_df["cnt"][0]
    except Exception:
        # Table might not exist
        contamination_count = 0

    # 2. Write Metrics CSV/MD
    metrics = {
        "metric": [
            "total_editorial_pages_processed",
            "rejected_non_nhl_blocks",
            "tier2_rows_total",
            "status_missing_odds",
            "status_candidate_ready",
            "contamination_fact_prop_odds"
        ],
        "value": [
            total_pages,
            rejected_non_nhl,
            len(tier2_df),
            missing_odds,
            candidate_ready,
            contamination_count
        ]
    }
    metrics_df = pd.DataFrame(metrics)
    output_dir = Path("outputs/odds_archive_audit")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    metrics_df.to_csv(output_dir / "alignment_metrics.csv", index=False)
    
    md_report = f"""# Quantitative Alignment Report
**Generated:** {datetime.utcnow()}

## Metrics
| Metric | Value |
| :--- | :--- |
| Total Pages Processed | {total_pages} |
| Rejected Non-NHL Blocks | {rejected_non_nhl} |
| **Tier 2 Total Rows** | **{len(tier2_df)}** |
| Missing Odds | {missing_odds} |
| Candidate Ready | {candidate_ready} |
| **Contamination (ROI Table)** | **{contamination_count}** (Must be 0) |

"""
    (output_dir / "alignment_metrics.md").write_text(md_report, encoding="utf-8")

    # 3. Generate HTML Artifacts
    
    # Editorial Validation
    if not tier2_df.empty:
        sample_editorial = tier2_df.sample(min(50, len(tier2_df)))
        html_ed = "<html><body><h1>Editorial Validation (Tier 2)</h1><table border='1'><tr><th>ID</th><th>Snippet</th><th>Status</th><th>Reason</th><th>Props</th></tr>"
        for _, row in sample_editorial.iterrows():
            html_ed += f"<tr><td>{row['mention_id']}</td><td>{row['raw_text_snippet']}</td><td>{row['status_code']}</td><td>{row['rejection_reason']}</td><td>{row['extracted_props']}</td></tr>"
        html_ed += "</table></body></html>"
        (output_dir / "editorial_validation.html").write_text(html_ed, encoding="utf-8")
        
    # ROI Safety Validation
    try:
        roi_df = con.execute("SELECT * FROM fact_prop_odds LIMIT 50").df()
        if not roi_df.empty:
            html_roi = "<html><body><h1>ROI Safety Validation (fact_prop_odds)</h1><table border='1'><tr><th>Vendor</th><th>Date</th><th>Player</th><th>Market</th><th>Odds</th></tr>"
            for _, row in roi_df.iterrows():
                html_roi += f"<tr><td>{row.get('source_vendor')}</td><td>{row.get('capture_ts_utc')}</td><td>{row.get('player_name_raw')}</td><td>{row.get('market_type')}</td><td>{row.get('odds_american')}</td></tr>"
            html_roi += "</table></body></html>"
            (output_dir / "roi_safety_validation.html").write_text(html_roi, encoding="utf-8")
        else:
             (output_dir / "roi_safety_validation.html").write_text("<html><body><h1>ROI Safety Validation</h1><p>Table fact_prop_odds is empty (SAFE).</p></body></html>", encoding="utf-8")
    except Exception as e:
         (output_dir / "roi_safety_validation.html").write_text(f"<html><body><h1>ROI Safety Validation</h1><p>Error reading table: {e}</p></body></html>", encoding="utf-8")

    con.close()
    print("Reports generated in outputs/odds_archive_audit/")

if __name__ == "__main__":
    generate_report()

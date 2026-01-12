import json
import logging
import sys
from pathlib import Path
from dataclasses import asdict

# Adjust path to include src
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from odds_archive import parsers
from odds_archive import config

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples"
OUTPUT_REPORT = Path(__file__).resolve().parents[1] / "outputs" / "odds_archive_audit" / "fixture_test_report.md"

def normalize_name(name):
    if not name: return ""
    return name.lower().replace(".", "").replace(" ", "")

def run_test():
    registry = parsers.build_registry()
    results = []
    
    if not EXAMPLES_DIR.exists():
        logger.error(f"Examples directory not found: {EXAMPLES_DIR}")
        return

    fixture_files = list(EXAMPLES_DIR.glob("expected_*.json"))
    if not fixture_files:
        logger.error("No expected_*.json files found.")
        return

    logger.info(f"Found {len(fixture_files)} fixtures.")
    
    # Ensure output dir exists
    OUTPUT_REPORT.parent.mkdir(parents=True, exist_ok=True)

    report_lines = ["# Fixture Validation Report", "", "| Fixture | Source | Result | Details |", "|---|---|---|---|"]
    
    all_passed = True

    for fixture_path in fixture_files:
        with open(fixture_path, "r") as f:
            expected = json.load(f)
        
        # Determine content file path
        # Convention: expected_NAME.json -> NAME.html
        base_name = fixture_path.stem.replace("expected_", "")
        content_path = EXAMPLES_DIR / f"{base_name}.html"
        
        source = "Snapshot"
        if not content_path.exists():
            # Try .txt
            content_path = EXAMPLES_DIR / f"{base_name}.txt"
            if not content_path.exists():
                logger.warning(f"Content file missing for {fixture_path.name}")
                report_lines.append(f"| {fixture_path.name} | {source} | ERROR | Content file missing |")
                all_passed = False
                continue

        with open(content_path, "r", encoding="utf-8") as f:
            content = f.read()

        logger.info(f"Processing {fixture_path.name}...")
        
        # Step 1: Gating
        title = ""
        if "<title>" in content:
            title = content.split("<title>")[1].split("</title>")[0]
        
        page_gate = parsers.is_nhl_page(expected["meta"]["url"], title)
        content_gate = parsers.is_nhl_block(content)

        # "REJECT_NON_NHL"
        if expected["expected_classification"] == "REJECT_NON_NHL":
            if not page_gate and not content_gate:
                report_lines.append(f"| {fixture_path.name} | {source} | PASS | Correctly rejected (Page & Content Gate) |")
            elif not content_gate:
                 report_lines.append(f"| {fixture_path.name} | {source} | PASS | Correctly rejected (Content Gate) |")
            else:
                 if parsers.is_nhl_block(content):
                      report_lines.append(f"| {fixture_path.name} | {source} | FAIL | Expected REJECT, but passed Content Gate |")
                      all_passed = False
                 else:
                      report_lines.append(f"| {fixture_path.name} | {source} | PASS | Correctly rejected |")
            continue

        # For NHL_PASS
        if expected["expected_classification"] == "NHL_PASS":
            candidates = registry.parse(content)
            
            # Find matching candidate
            target = expected["candidate"]
            match = None
            
            for c in candidates:
                rec = c.to_record()
                # Normalize matching
                c_name = normalize_name(rec["player_name_raw"])
                t_name = normalize_name(target["player_name_raw"])
                
                if c_name == t_name and rec["market"] == target["market"] and rec["line"] == target["line"] and rec["side"] == target["side"]:
                    match = rec
                    break
            
            if match:
                # Validate Fields
                diffs = []
                if match["odds"] != target["odds"]:
                    diffs.append(f"Odds: {match['odds']} != {target['odds']}")
                if match["bookmaker"] != target["bookmaker"]:
                    diffs.append(f"Book: {match['bookmaker']} != {target['bookmaker']}")
                
                if diffs:
                    report_lines.append(f"| {fixture_path.name} | {source} | FAIL | Matched but diffs: {'; '.join(diffs)} |")
                    all_passed = False
                else:
                    report_lines.append(f"| {fixture_path.name} | {source} | PASS | Exact match found |")
            else:
                report_lines.append(f"| {fixture_path.name} | {source} | FAIL | No matching candidate found |")
                all_passed = False

    # Write Report
    with open(OUTPUT_REPORT, "w") as f:
        f.write("\n".join(report_lines))
    
    logger.info(f"Report written to {OUTPUT_REPORT}")
    
    if not all_passed:
        sys.exit(1)

if __name__ == "__main__":
    run_test()

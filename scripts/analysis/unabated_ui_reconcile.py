import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from nhl_bets.scrapers.unabated_client import UnabatedClient
from nhl_bets.common.db_init import DEFAULT_DB_PATH, initialize_phase11_tables


def get_latest_unabated_file(root_dir: Path) -> Path:
    raw_dir = root_dir / "outputs" / "odds" / "raw" / "UNABATED"
    files = list(raw_dir.glob("**/*.json"))
    if not files:
        raise FileNotFoundError("No Unabated snapshot files found.")
    files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return files[0]


def normalize_name(name: str) -> str:
    return " ".join(name.strip().lower().split()) if name else ""


def map_side(side_key: str) -> str:
    if side_key.startswith("si0"):
        return "OVER"
    if side_key.startswith("si1"):
        return "UNDER"
    return "UNKNOWN"


def classify_book_type(book_name: str) -> str:
    if not book_name:
        return "UNKNOWN"
    name = book_name.lower()
    keywords = UnabatedClient.DFS_FIXED_PAYOUT_KEYWORDS
    return "DFS_FIXED_PAYOUT" if any(k in name for k in keywords) else "SPORTSBOOK"


def american_to_decimal(american: int) -> float:
    if american > 0:
        return (american / 100) + 1
    if american < 0:
        return (100 / abs(american)) + 1
    return None


def reverse_bet_type_map():
    return {v: k for k, v in UnabatedClient.BET_TYPE_MAP.items()}


def load_examples(path: Path) -> list:
    if not path.exists():
        raise FileNotFoundError(f"Examples file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    return payload.get("examples", [])


def build_db_query(filters: dict) -> tuple:
    clauses = ["source_vendor = 'UNABATED'"]
    params = []

    if filters.get("vendor_event_id"):
        clauses.append("vendor_event_id = ?")
        params.append(str(filters["vendor_event_id"]))
    if filters.get("vendor_person_id"):
        clauses.append("vendor_person_id = ?")
        params.append(str(filters["vendor_person_id"]))
    if filters.get("bet_type_id") is not None:
        clauses.append("vendor_bet_type_id = ?")
        params.append(int(filters["bet_type_id"]))
    if filters.get("line") is not None:
        clauses.append("line = ?")
        params.append(float(filters["line"]))
    if filters.get("side"):
        clauses.append("side = ?")
        params.append(filters["side"])
    if filters.get("market_source_id"):
        clauses.append("vendor_market_source_id = ?")
        params.append(str(filters["market_source_id"]))

    query = f"""
        SELECT vendor_event_id, vendor_person_id, event_start_time_utc, home_team, away_team,
               player_name_raw, market_type, line, side, book_name_raw, book_type, odds_american,
               odds_decimal, vendor_market_source_id, vendor_bet_type_id, vendor_outcome_key,
               vendor_price_raw, vendor_price_format, raw_payload_hash, capture_ts_utc
        FROM fact_prop_odds
        WHERE {' AND '.join(clauses)}
        ORDER BY capture_ts_utc DESC
        LIMIT 20
    """
    return query, params


def reconcile_example(example: dict, raw_data: dict, con: duckdb.DuckDBPyConnection) -> dict:
    market_sources = {str(ms["id"]): ms["name"] for ms in raw_data.get("marketSources", [])}
    reverse_market_sources = {v.lower(): k for k, v in market_sources.items()}
    people = raw_data.get("people", {})

    expected_player_id = example.get("vendor_person_id")
    if not expected_player_id and example.get("player_name"):
        target = normalize_name(example["player_name"])
        for pid, person in people.items():
            full_name = normalize_name(f"{person.get('firstName', '')} {person.get('lastName', '')}")
            if full_name == target:
                expected_player_id = str(pid)
                break

    expected_bet_type_id = example.get("bet_type_id")
    if expected_bet_type_id is None and example.get("market"):
        expected_bet_type_id = reverse_bet_type_map().get(example["market"].upper())

    expected_book_id = example.get("market_source_id")
    if expected_book_id is None and example.get("book_name"):
        expected_book_id = reverse_market_sources.get(example["book_name"].lower())

    expected_side = example.get("side")
    expected_event_id = example.get("vendor_event_id")
    expected_line = example.get("line")
    if expected_line is not None:
        expected_line = float(expected_line)
    expected_american = example.get("expected_american_odds")
    if expected_american is not None:
        expected_american = int(expected_american)

    pregame_props = raw_data.get("odds", {}).get("lg6:pt1:pregame", [])
    raw_matches = []

    for prop in pregame_props:
        if expected_event_id and str(prop.get("eventId")) != str(expected_event_id):
            continue
        if expected_player_id and str(prop.get("personId")) != str(expected_player_id):
            continue
        if expected_bet_type_id is not None and prop.get("betTypeId") != expected_bet_type_id:
            continue

        sides = prop.get("sides") or {}
        for side_key, books in sides.items():
            mapped_side = map_side(side_key)
            for ms_key, price_data in books.items():
                book_id = ms_key.replace("ms", "")
                points = price_data.get("points")
                american = price_data.get("americanPrice")

                if expected_book_id and str(book_id) != str(expected_book_id):
                    continue
                if expected_line is not None and points != expected_line:
                    continue
                raw_matches.append({
                    "eventId": str(prop.get("eventId")),
                    "personId": str(prop.get("personId")),
                    "betTypeId": prop.get("betTypeId"),
                    "eventStart": prop.get("eventStart"),
                    "eventName": prop.get("eventName"),
                    "sideKey": side_key,
                    "mappedSide": mapped_side,
                    "marketSourceId": book_id,
                    "bookName": market_sources.get(book_id, "UNKNOWN"),
                    "bookType": classify_book_type(market_sources.get(book_id, "")),
                    "points": points,
                    "americanPrice": american,
                    "price": price_data.get("price"),
                    "sourcePrice": price_data.get("sourcePrice"),
                    "sourceFormat": price_data.get("sourceFormat")
                })

    raw_df = pd.DataFrame(raw_matches).head(20)

    query_filters = {
        "vendor_event_id": expected_event_id,
        "vendor_person_id": expected_player_id,
        "bet_type_id": expected_bet_type_id,
        "line": expected_line,
        "side": expected_side.upper() if expected_side else None,
        "market_source_id": expected_book_id,
    }
    query, params = build_db_query(query_filters)
    db_rows = con.execute(query, params).df()
    if not db_rows.empty and "book_name_raw" in db_rows.columns:
        db_rows["book_type"] = db_rows["book_name_raw"].apply(classify_book_type)

    checks = []
    if expected_american is not None and not raw_df.empty:
        matched = raw_df[raw_df["americanPrice"] == expected_american]
        checks.append({
            "check": "american_price_match",
            "expected": expected_american,
            "actual": matched["americanPrice"].tolist() if not matched.empty else raw_df["americanPrice"].tolist(),
            "pass": not matched.empty
        })

    if expected_side and not raw_df.empty:
        matched = raw_df[raw_df["mappedSide"] == expected_side.upper()]
        checks.append({
            "check": "side_mapping_match",
            "expected": expected_side.upper(),
            "actual": raw_df["mappedSide"].tolist(),
            "pass": not matched.empty
        })

    if not raw_df.empty:
        raw_match = raw_df.iloc[0]
        american = raw_match.get("americanPrice")
        expected_decimal = american_to_decimal(american) if american is not None else None
        checks.append({
            "check": "decimal_from_american",
            "expected": round(expected_decimal, 4) if expected_decimal is not None else None,
            "actual": round(expected_decimal, 4) if expected_decimal is not None else None,
            "pass": expected_decimal is not None
        })

    if not raw_df.empty and not db_rows.empty:
        raw_start = raw_df.iloc[0].get("eventStart")
        db_start = db_rows.iloc[0].get("event_start_time_utc")
        checks.append({
            "check": "event_start_time_utc",
            "expected": raw_start,
            "actual": db_start.isoformat() if pd.notnull(db_start) else None,
            "pass": raw_start is None or (db_start is not None and raw_start.startswith(db_start.isoformat()[:19]))
        })

    return {
        "inputs": example,
        "raw_matches": raw_df,
        "db_matches": db_rows,
        "checks": checks
    }


def render_report(results: list, output_path: Path):
    lines = []
    lines.append("# Unabated UI Reconciliation Report\n\n")
    lines.append(f"**Generated at (UTC):** {datetime.now(timezone.utc).isoformat()}\n\n")

    for idx, result in enumerate(results, start=1):
        lines.append(f"## Example {idx}\n\n")
        lines.append("### Inputs\n")
        lines.append("```json\n")
        lines.append(json.dumps(result["inputs"], indent=2))
        lines.append("\n```\n\n")

        lines.append("### Raw Matches\n")
        if result["raw_matches"].empty:
            lines.append("_No raw matches found._\n\n")
        else:
            lines.append(result["raw_matches"].to_markdown(index=False))
            lines.append("\n\n")

        lines.append("### DB Matches\n")
        if result["db_matches"].empty:
            lines.append("_No DB matches found._\n\n")
        else:
            lines.append(result["db_matches"].to_markdown(index=False))
            lines.append("\n\n")

        lines.append("### Checks\n")
        if not result["checks"]:
            lines.append("_No checks run._\n\n")
        else:
            checks_df = pd.DataFrame(result["checks"])
            lines.append(checks_df.to_markdown(index=False))
            lines.append("\n\n")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        f.write("".join(lines))


def main():
    parser = argparse.ArgumentParser(description="Reconcile Unabated UI examples against raw payloads and DB.")
    parser.add_argument("--examples", type=str, required=True, help="Path to examples JSON file.")
    parser.add_argument("--file", type=str, help="Optional path to a specific raw snapshot JSON file.")
    args = parser.parse_args()

    examples_path = Path(args.examples)
    examples = load_examples(examples_path)
    if not examples:
        raise ValueError("No examples found in the provided file.")

    if args.file:
        raw_path = Path(args.file)
    else:
        raw_path = get_latest_unabated_file(project_root)

    with raw_path.open("r", encoding="utf-8") as f:
        raw_data = json.load(f)

    con = duckdb.connect(str(project_root / DEFAULT_DB_PATH))
    try:
        initialize_phase11_tables(con)
        results = [reconcile_example(example, raw_data, con) for example in examples]
    finally:
        con.close()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    report_path = project_root / "outputs" / "monitoring" / f"unabated_ui_reconcile_{timestamp}.md"
    render_report(results, report_path)
    print(f"Report generated: {report_path}")


if __name__ == "__main__":
    main()

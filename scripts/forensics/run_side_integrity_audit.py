import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys

import duckdb
import pandas as pd

# Ensure src is on path
REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from nhl_bets.analysis.side_integrity import (
    normalize_book,
    normalize_market,
    normalize_player,
    normalize_side,
    resolve_odds_side,
    build_odds_side_lookup,
)


def _load_odds(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    query = """
        SELECT
            source_vendor,
            capture_ts_utc,
            event_start_time_utc,
            player_name_raw,
            market_type,
            line,
            side,
            book_name_raw,
            odds_american,
            odds_decimal,
            raw_payload_path,
            raw_payload_hash
        FROM fact_prop_odds
    """
    df = con.execute(query).df()
    if df.empty:
        return df
    df["event_date"] = pd.to_datetime(df["event_start_time_utc"], errors="coerce").dt.date
    df["player_key"] = df["player_name_raw"].apply(normalize_player)
    df["market_key"] = df["market_type"].apply(normalize_market)
    df["line_key"] = pd.to_numeric(df["line"], errors="coerce").round(3)
    df["book_key"] = df["book_name_raw"].apply(normalize_book)
    return df


def _build_trace_keys(trace_df: pd.DataFrame) -> pd.DataFrame:
    df = trace_df.copy()
    df["player_key"] = df["player"].apply(normalize_player)
    df["market_key"] = df["market"].apply(normalize_market)
    df["line_key"] = pd.to_numeric(df["line"], errors="coerce").round(3)
    df["book_key"] = df["book"].apply(normalize_book)
    df["bet_side_norm"] = df["side"].apply(normalize_side)
    df["game_date_key"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date
    return df


def _select_candidates(trace_row: pd.Series, odds_df: pd.DataFrame) -> pd.DataFrame:
    mask = (
        (odds_df["player_key"] == trace_row["player_key"])
        & (odds_df["market_key"] == trace_row["market_key"])
        & (odds_df["line_key"] == trace_row["line_key"])
        & (odds_df["book_key"] == trace_row["book_key"])
    )
    return odds_df[mask].copy()


def _apply_date_preference(trace_row: pd.Series, candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty:
        return candidates
    game_date = trace_row.get("game_date_key")
    if game_date and candidates["event_date"].notna().any():
        date_match = candidates[candidates["event_date"] == game_date]
        if not date_match.empty:
            candidates = date_match
        else:
            candidates = candidates.copy()
            candidates["date_delta"] = candidates["event_date"].apply(
                lambda d: abs((d - game_date).days) if pd.notna(d) else None
            )
            min_delta = candidates["date_delta"].min()
            if pd.notna(min_delta):
                candidates = candidates[candidates["date_delta"] == min_delta]
    return candidates


def _apply_odds_preference(trace_row: pd.Series, candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty:
        return candidates
    target_odds = trace_row.get("odds_american")
    if pd.notna(target_odds):
        matches = candidates[candidates["odds_american"] == int(target_odds)]
        if not matches.empty:
            return matches
    return candidates


def main() -> None:
    parser = argparse.ArgumentParser(description="Side integrity audit for traced odds joins.")
    parser.add_argument(
        "--trace-csv",
        default="outputs/backtesting/projection_trace_audit_rows_20260108_000058.csv",
    )
    parser.add_argument("--duckdb-path", default="data/db/nhl_backtest.duckdb")
    parser.add_argument("--out-dir", default="outputs/backtesting")
    args = parser.parse_args()

    trace_path = Path(args.trace_csv)
    if not trace_path.exists():
        raise SystemExit(f"Trace CSV not found: {trace_path}")

    trace_df = pd.read_csv(trace_path)
    trace_df = _build_trace_keys(trace_df)

    con = duckdb.connect(args.duckdb_path)
    try:
        odds_df = _load_odds(con)
    finally:
        con.close()

    if odds_df.empty:
        raise SystemExit("No odds rows found in fact_prop_odds.")

    side_lookup = build_odds_side_lookup(odds_df)

    audit_rows = []
    for _, row in trace_df.iterrows():
        candidates = _select_candidates(row, odds_df)
        candidates = _apply_date_preference(row, candidates)
        candidates = _apply_odds_preference(row, candidates)

        if candidates.empty:
            audit_rows.append(
                {
                    "player": row["player"],
                    "market": row["market"],
                    "line": row["line"],
                    "book": row["book"],
                    "bet_side": row["bet_side_norm"],
                    "trace_odds_american": row["odds_american"],
                    "trace_odds_decimal": row["odds_decimal"],
                    "trace_path": row.get("trace_path"),
                    "join_status": "NO_MATCH",
                }
            )
            continue

        for _, cand in candidates.iterrows():
            odds_side_interpreted = normalize_side(cand["side"])
            odds_side_lookup, lookup_reason = resolve_odds_side(
                side_lookup,
                cand["player_name_raw"],
                cand["market_type"],
                cand["line"],
                cand["book_name_raw"],
                cand["odds_american"],
            )
            bet_side = row["bet_side_norm"]
            side_match = bet_side == odds_side_interpreted if bet_side else False

            audit_rows.append(
                {
                    "player": row["player"],
                    "market": row["market"],
                    "line": row["line"],
                    "book": row["book"],
                    "game_date": row["game_date"],
                    "bet_side": bet_side,
                    "trace_odds_american": row["odds_american"],
                    "trace_odds_decimal": row["odds_decimal"],
                    "trace_path": row.get("trace_path"),
                    "join_status": "MATCHED",
                    "odds_source_vendor": cand["source_vendor"],
                    "odds_capture_ts_utc": cand["capture_ts_utc"],
                    "odds_event_start_time_utc": cand["event_start_time_utc"],
                    "odds_event_date": cand["event_date"],
                    "odds_player_raw": cand["player_name_raw"],
                    "odds_market_raw": cand["market_type"],
                    "odds_line": cand["line"],
                    "odds_side_raw": cand["side"],
                    "odds_side_interpreted": odds_side_interpreted,
                    "odds_side_lookup": odds_side_lookup,
                    "odds_side_lookup_reason": lookup_reason,
                    "odds_book_raw": cand["book_name_raw"],
                    "odds_american": cand["odds_american"],
                    "odds_decimal": cand["odds_decimal"],
                    "odds_raw_payload_path": cand["raw_payload_path"],
                    "odds_raw_payload_hash": cand["raw_payload_hash"],
                    "side_match": side_match,
                }
            )

    audit_df = pd.DataFrame(audit_rows)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / f"odds_side_integrity_audit_{ts}.csv"
    out_md = out_dir / f"odds_side_integrity_audit_{ts}.md"

    audit_df.to_csv(out_csv, index=False)

    side_mismatch = audit_df[(audit_df["join_status"] == "MATCHED") & (~audit_df["side_match"])]
    counts = (
        side_mismatch.groupby(["book", "market"]).size().reset_index(name="n")
        if not side_mismatch.empty
        else pd.DataFrame(columns=["book", "market", "n"])
    )

    with out_md.open("w", encoding="utf-8") as f:
        f.write("# Odds Side Integrity Audit\n\n")
        f.write(f"- trace_csv: `{trace_path}`\n")
        f.write(f"- duckdb_path: `{args.duckdb_path}`\n")
        f.write(f"- total_trace_rows: `{len(trace_df)}`\n")
        f.write(f"- matched_rows: `{int((audit_df['join_status'] == 'MATCHED').sum())}`\n")
        f.write(f"- side_mismatch_rows: `{int(len(side_mismatch))}`\n\n")

        f.write("## Side Mismatch Counts (book/market)\n\n")
        if counts.empty:
            f.write("No side mismatches detected.\n\n")
        else:
            f.write(counts.to_markdown(index=False))
            f.write("\n\n")

        f.write("## Sample Mismatches (up to 10)\n\n")
        if side_mismatch.empty:
            f.write("No mismatches.\n")
        else:
            sample_cols = [
                "player",
                "market",
                "line",
                "book",
                "bet_side",
                "odds_side_raw",
                "odds_side_interpreted",
                "odds_side_lookup",
                "trace_odds_american",
                "odds_american",
                "odds_event_date",
                "odds_source_vendor",
            ]
            f.write(side_mismatch[sample_cols].head(10).to_markdown(index=False))
            f.write("\n")

    print(f"Wrote audit CSV: {out_csv}")
    print(f"Wrote audit MD: {out_md}")


if __name__ == "__main__":
    main()

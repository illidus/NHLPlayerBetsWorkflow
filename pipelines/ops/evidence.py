import csv
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import duckdb

from nhl_bets.common.db_init import get_db_connection, DEFAULT_DB_PATH

DB_PATH = DEFAULT_DB_PATH
PROBS_PATH = "outputs/projections/SingleGamePropProbabilities.csv"
ACCURACY_REPORT_PATH = "outputs/backtest_reports/forecast_accuracy.md"


def ensure_evidence_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_run_registry (
            run_id TEXT PRIMARY KEY,
            run_date DATE,
            start_ts_utc TIMESTAMP,
            end_ts_utc TIMESTAMP,
            git_sha TEXT,
            flags_json TEXT,
            step_status_json TEXT,
            counts_json TEXT,
            vendor_failures_json TEXT
        )
    """)
    # Schema migration: Add step_timings_json if not exists
    try:
        con.execute("SELECT step_timings_json FROM fact_run_registry LIMIT 0")
    except duckdb.BinderException:
        con.execute("ALTER TABLE fact_run_registry ADD COLUMN step_timings_json TEXT")

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_odds_coverage_daily (
            run_id TEXT,
            run_date DATE,
            source_vendor TEXT,
            book_name_raw TEXT,
            market_type TEXT,
            snapshot_count BIGINT,
            record_count BIGINT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_mapping_quality_daily (
            run_id TEXT,
            run_date DATE,
            source_vendor TEXT,
            total_records BIGINT,
            mapped_player_count BIGINT,
            unmapped_player_count BIGINT,
            mapped_event_count BIGINT,
            unmapped_event_count BIGINT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_ev_summary_daily (
            run_id TEXT,
            run_date DATE,
            source_vendor TEXT,
            book_name_raw TEXT,
            market_type TEXT,
            record_count BIGINT,
            ev_avg DOUBLE,
            ev_min DOUBLE,
            ev_max DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_forecast_accuracy_daily (
            run_id TEXT,
            run_date DATE,
            market TEXT,
            variant TEXT,
            log_loss DOUBLE,
            log_loss_improvement DOUBLE,
            ece DOUBLE,
            top5_hit_rate DOUBLE,
            top10_hit_rate DOUBLE
        )
    """)


def record_run_registry(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    run_date: str,
    start_ts: datetime,
    end_ts: datetime,
    git_sha: str,
    flags: Dict[str, Any],
    step_status: Dict[str, Any],
    counts: Dict[str, Any],
    vendor_failures: Dict[str, Any],
    step_timings: Dict[str, float] = None,
) -> None:
    if step_timings is None:
        step_timings = {}
        
    con.execute(
        """
        INSERT INTO fact_run_registry (
            run_id, run_date, start_ts_utc, end_ts_utc, git_sha, 
            flags_json, step_status_json, counts_json, vendor_failures_json, step_timings_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            run_date,
            start_ts,
            end_ts,
            git_sha,
            json.dumps(flags, sort_keys=True),
            json.dumps(step_status, sort_keys=True),
            json.dumps(counts, sort_keys=True),
            json.dumps(vendor_failures, sort_keys=True),
            json.dumps(step_timings, sort_keys=True),
        ],
    )


def _normalize_name_sql(expr: str) -> str:
    return (
        "lower(trim(regexp_replace(regexp_replace(regexp_replace("
        + expr
        + ", '\\\\s*\\\\(.*?\\\\)', ''), '[^\\\\w\\\\s]', ''), '\\\\s+', ' ')))"
    )


def _collect_prob_columns(probs_path: str) -> List[Dict[str, Any]]:
    with open(probs_path, "r", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        headers = next(reader, [])
    cols = []
    for col in headers:
        if not col.startswith("p_"):
            continue
        if "_plus" not in col and "plus" not in col:
            continue
        parts = col.split("_")
        if len(parts) < 3:
            continue
        market_token = parts[1]
        k_token = parts[2]
        if not k_token.endswith("plus"):
            continue
        try:
            k_val = int(k_token.replace("plus", ""))
        except ValueError:
            continue
        line = k_val - 0.5
        variant = "p_over_calibrated" if col.endswith("_calibrated") else "p_over"
        market_map = {
            "G": "GOALS",
            "A": "ASSISTS",
            "PTS": "POINTS",
            "SOG": "SOG",
            "BLK": "BLOCKS",
        }
        market = market_map.get(market_token)
        if not market:
            continue
        cols.append(
            {
                "column": col,
                "market": market,
                "line": line,
                "variant": variant,
            }
        )
    return cols


def refresh_evidence_tables(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    run_date: str,
    disable_calibration: bool,
    probs_path: str = PROBS_PATH,
    accuracy_report_path: str = ACCURACY_REPORT_PATH,
) -> Dict[str, Any]:
    counts: Dict[str, Any] = {}

    con.execute(
        """
        INSERT INTO fact_odds_coverage_daily
        SELECT
            ? AS run_id,
            DATE(capture_ts_utc) AS run_date,
            source_vendor,
            book_name_raw,
            market_type,
            COUNT(DISTINCT raw_payload_hash) AS snapshot_count,
            COUNT(*) AS record_count
        FROM fact_prop_odds
        WHERE DATE(capture_ts_utc) = ?
        GROUP BY 2, 3, 4, 5
        """,
        [run_id, run_date],
    )

    con.execute(
        """
        INSERT INTO fact_mapping_quality_daily
        SELECT
            ? AS run_id,
            DATE(o.capture_ts_utc) AS run_date,
            o.source_vendor,
            COUNT(*) AS total_records,
            SUM(CASE WHEN pm.canonical_player_id IS NOT NULL THEN 1 ELSE 0 END) AS mapped_player_count,
            SUM(CASE WHEN pm.canonical_player_id IS NULL THEN 1 ELSE 0 END) AS unmapped_player_count,
            SUM(CASE WHEN em.canonical_game_id IS NOT NULL THEN 1 ELSE 0 END) AS mapped_event_count,
            SUM(CASE WHEN em.canonical_game_id IS NULL THEN 1 ELSE 0 END) AS unmapped_event_count
        FROM fact_prop_odds o
        LEFT JOIN dim_players_mapping pm
            ON o.player_name_raw = pm.vendor_player_name AND o.source_vendor = pm.source_vendor
        LEFT JOIN dim_events_mapping em
            ON o.event_id_vendor = em.vendor_event_id AND o.source_vendor = em.source_vendor
        WHERE DATE(o.capture_ts_utc) = ?
        GROUP BY 2, 3
        """,
        [run_id, run_date],
    )

    if os.path.exists(probs_path):
        prob_cols = _collect_prob_columns(probs_path)
        if prob_cols:
            safe_probs_path = probs_path.replace("'", "''")
            con.execute(f"CREATE OR REPLACE TEMP VIEW probs_base AS SELECT * FROM read_csv_auto('{safe_probs_path}')")
            union_sql = []
            for col in prob_cols:
                union_sql.append(
                    f"""
                    SELECT
                        {_normalize_name_sql("Player")} AS norm_name,
                        '{col['market']}' AS market_type,
                        {col['line']} AS line,
                        '{col['variant']}' AS variant,
                        {col['column']} AS p_over
                    FROM probs_base
                    """
                )
            con.execute(
                f"""
                CREATE OR REPLACE TEMP VIEW probs_long AS
                {' UNION ALL '.join(union_sql)}
                """
            )
            con.execute(
                """
                CREATE OR REPLACE TEMP VIEW probs_pivot AS
                SELECT
                    norm_name,
                    market_type,
                    line,
                    MAX(CASE WHEN variant = 'p_over' THEN p_over END) AS p_over,
                    MAX(CASE WHEN variant = 'p_over_calibrated' THEN p_over END) AS p_over_calibrated
                FROM probs_long
                GROUP BY 1, 2, 3
                """
            )
            safe_run_date = run_date.replace("'", "''")
            con.execute(
                f"""
                CREATE OR REPLACE TEMP VIEW odds_norm AS
                SELECT
                    *,
                    {_normalize_name_sql("player_name_raw")} AS norm_name
                FROM fact_prop_odds
                WHERE DATE(capture_ts_utc) = DATE '{safe_run_date}'
                """
            )
            disable_flag = "true" if disable_calibration else "false"
            con.execute(
                f"""
                CREATE OR REPLACE TEMP VIEW ev_rows AS
                SELECT
                    o.source_vendor,
                    o.book_name_raw,
                    o.market_type,
                    o.line,
                    o.side,
                    o.odds_decimal,
                    CASE
                        WHEN {disable_flag} THEN p.p_over
                        WHEN o.market_type IN ('ASSISTS', 'POINTS') AND p.p_over_calibrated IS NOT NULL THEN p.p_over_calibrated
                        ELSE p.p_over
                    END AS p_over_selected
                FROM odds_norm o
                LEFT JOIN probs_pivot p
                    ON o.norm_name = p.norm_name
                    AND o.market_type = p.market_type
                    AND o.line = p.line
                WHERE o.odds_decimal IS NOT NULL AND o.odds_decimal > 1.0
                """
            )
            con.execute(
                """
                INSERT INTO fact_ev_summary_daily
                SELECT
                    ? AS run_id,
                    ? AS run_date,
                    source_vendor,
                    book_name_raw,
                    market_type,
                    COUNT(*) AS record_count,
                    AVG(ev) AS ev_avg,
                    MIN(ev) AS ev_min,
                    MAX(ev) AS ev_max
                FROM (
                    SELECT
                        source_vendor,
                        book_name_raw,
                        market_type,
                        CASE
                            WHEN p_over_selected IS NULL THEN NULL
                            WHEN UPPER(side) = 'OVER' THEN (p_over_selected * odds_decimal) - 1
                            ELSE ((1 - p_over_selected) * odds_decimal) - 1
                        END AS ev
                    FROM ev_rows
                ) t
                WHERE ev IS NOT NULL
                GROUP BY 3, 4, 5
                """,
                [run_id, run_date],
            )

    accuracy_rows = _parse_accuracy_table(accuracy_report_path)
    for row in accuracy_rows:
        con.execute(
            """
            INSERT INTO fact_forecast_accuracy_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                run_date,
                row.get("Market"),
                row.get("Variant"),
                row.get("Log Loss"),
                row.get("Log Loss Improvement"),
                row.get("ECE"),
                row.get("Top-5 Hit Rate"),
                row.get("Top-10 Hit Rate"),
            ],
        )

    counts["odds_coverage_rows"] = con.execute(
        "SELECT COUNT(*) FROM fact_odds_coverage_daily WHERE run_id = ?",
        [run_id],
    ).fetchone()[0]
    counts["mapping_quality_rows"] = con.execute(
        "SELECT COUNT(*) FROM fact_mapping_quality_daily WHERE run_id = ?",
        [run_id],
    ).fetchone()[0]
    counts["ev_summary_rows"] = con.execute(
        "SELECT COUNT(*) FROM fact_ev_summary_daily WHERE run_id = ?",
        [run_id],
    ).fetchone()[0]
    counts["forecast_accuracy_rows"] = con.execute(
        "SELECT COUNT(*) FROM fact_forecast_accuracy_daily WHERE run_id = ?",
        [run_id],
    ).fetchone()[0]
    return counts


def _parse_accuracy_table(accuracy_report_path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(accuracy_report_path):
        return []

    rows: List[Dict[str, Any]] = []
    header: Optional[List[str]] = None
    capture = False

    with open(accuracy_report_path, "r", encoding="utf-8") as handle:
        for line in handle:
            if "| Market" in line and "| Variant" in line and "Log Loss" in line:
                header = [part.strip() for part in line.strip().strip("|").split("|")]
                capture = True
                continue
            if capture and line.strip().startswith("|---"):
                continue
            if capture and line.strip().startswith("|"):
                values = [part.strip() for part in line.strip().strip("|").split("|")]
                if header and len(values) == len(header):
                    row = dict(zip(header, values))
                    for key in ["Log Loss", "Log Loss Improvement", "ECE", "Top-5 Hit Rate", "Top-10 Hit Rate"]:
                        if key in row:
                            try:
                                row[key] = float(row[key])
                            except ValueError:
                                row[key] = None
                    rows.append(row)
                continue
            if capture and line.strip() == "":
                break
    return rows


def write_daily_report(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    run_date: str,
    report_path: str,
    step_status: Dict[str, Any],
    vendor_failures: Dict[str, Any],
    counts: Dict[str, Any],
) -> None:
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    coverage_rows = con.execute(
        """
        SELECT source_vendor, market_type, SUM(record_count) AS records
        FROM fact_odds_coverage_daily
        WHERE run_id = ?
        GROUP BY 1, 2
        ORDER BY 1, 2
        """,
        [run_id],
    ).fetchall()

    mapping_rows = con.execute(
        """
        SELECT source_vendor, total_records, mapped_player_count, mapped_event_count
        FROM fact_mapping_quality_daily
        WHERE run_id = ?
        ORDER BY 1
        """,
        [run_id],
    ).fetchall()

    ev_rows = con.execute(
        """
        SELECT source_vendor, market_type, COUNT(*) AS groups, AVG(ev_avg) AS avg_ev
        FROM fact_ev_summary_daily
        WHERE run_id = ?
        GROUP BY 1, 2
        ORDER BY 1, 2
        """,
        [run_id],
    ).fetchall()

    with open(report_path, "w", encoding="utf-8") as handle:
        handle.write(f"# Daily Report ({run_date})\n\n")
        handle.write("## Step Status\n")
        for step, status in step_status.items():
            handle.write(f"- {step}: {status}\n")
        handle.write("\n## Vendor Failures\n")
        if vendor_failures:
            for vendor, error in vendor_failures.items():
                handle.write(f"- {vendor}: {error}\n")
        else:
            handle.write("- None\n")
        handle.write("\n## Evidence Counts\n")
        for key, value in counts.items():
            handle.write(f"- {key}: {value}\n")
        handle.write("\n## Odds Coverage (Records)\n")
        if coverage_rows:
            for vendor, market, records in coverage_rows:
                handle.write(f"- {vendor} {market}: {records}\n")
        else:
            handle.write("- No odds coverage rows.\n")
        handle.write("\n## Mapping Quality\n")
        if mapping_rows:
            for vendor, total, mapped_player, mapped_event in mapping_rows:
                handle.write(
                    f"- {vendor}: total={total}, mapped_player={mapped_player}, mapped_event={mapped_event}\n"
                )
        else:
            handle.write("- No mapping quality rows.\n")
        handle.write("\n## EV Summary\n")
        if ev_rows:
            for vendor, market, groups, avg_ev in ev_rows:
                handle.write(f"- {vendor} {market}: groups={groups}, avg_ev={avg_ev:.4f}\n")
        else:
            handle.write("- No EV summary rows.\n")


def write_rolling_report(con: duckdb.DuckDBPyConnection, report_path: str) -> None:
    os.makedirs(os.path.dirname(report_path), exist_ok=True)

    rolling_7 = con.execute(
        """
        SELECT COUNT(*) AS runs,
               COALESCE(SUM(record_count), 0) AS odds_records
        FROM fact_odds_coverage_daily
        WHERE run_date >= CURRENT_DATE - INTERVAL 7 DAY
        """
    ).fetchone()
    rolling_30 = con.execute(
        """
        SELECT COUNT(*) AS runs,
               COALESCE(SUM(record_count), 0) AS odds_records
        FROM fact_odds_coverage_daily
        WHERE run_date >= CURRENT_DATE - INTERVAL 30 DAY
        """
    ).fetchone()

    mapping_7 = con.execute(
        """
        SELECT
            COALESCE(SUM(total_records), 0) AS total_records,
            COALESCE(SUM(mapped_player_count), 0) AS mapped_player_count,
            COALESCE(SUM(mapped_event_count), 0) AS mapped_event_count
        FROM fact_mapping_quality_daily
        WHERE run_date >= CURRENT_DATE - INTERVAL 7 DAY
        """
    ).fetchone()
    mapping_30 = con.execute(
        """
        SELECT
            COALESCE(SUM(total_records), 0) AS total_records,
            COALESCE(SUM(mapped_player_count), 0) AS mapped_player_count,
            COALESCE(SUM(mapped_event_count), 0) AS mapped_event_count
        FROM fact_mapping_quality_daily
        WHERE run_date >= CURRENT_DATE - INTERVAL 30 DAY
        """
    ).fetchone()

    with open(report_path, "w", encoding="utf-8") as handle:
        handle.write("# Rolling Report\n\n")
        handle.write("## Last 7 Days\n")
        handle.write(f"- runs: {rolling_7[0]}\n")
        handle.write(f"- odds_records: {rolling_7[1]}\n")
        handle.write(f"- mapped_player_count: {mapping_7[1]}\n")
        handle.write(f"- mapped_event_count: {mapping_7[2]}\n")
        handle.write("\n## Last 30 Days\n")
        handle.write(f"- runs: {rolling_30[0]}\n")
        handle.write(f"- odds_records: {rolling_30[1]}\n")
        handle.write(f"- mapped_player_count: {mapping_30[1]}\n")
        handle.write(f"- mapped_event_count: {mapping_30[2]}\n")


def run_diagnostics(
    run_id: str,
    run_date: str,
    disable_calibration: bool,
    step_status: Dict[str, Any],
    vendor_failures: Dict[str, Any],
    report_dir: str = "outputs/monitoring",
) -> Dict[str, Any]:
    con = get_db_connection(DB_PATH)
    try:
        ensure_evidence_tables(con)
        counts = refresh_evidence_tables(
            con,
            run_id=run_id,
            run_date=run_date,
            disable_calibration=disable_calibration,
        )
        daily_report = os.path.join(report_dir, f"daily_report_{run_date}.md")
        rolling_report = os.path.join(report_dir, "rolling_report.md")
        write_daily_report(con, run_id, run_date, daily_report, step_status, vendor_failures, counts)
        write_rolling_report(con, rolling_report)
        counts["daily_report_path"] = daily_report
        counts["rolling_report_path"] = rolling_report
        return counts
    finally:
        con.close()

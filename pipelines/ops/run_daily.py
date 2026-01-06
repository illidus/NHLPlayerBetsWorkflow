import argparse
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from typing import Dict, List

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from nhl_bets.common.db_init import get_db_connection
from pipelines.ops.evidence import ensure_evidence_tables, record_run_registry, run_diagnostics

PYTHON = sys.executable
DB_PATH = "data/db/nhl_backtest.duckdb"


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _resolve_flag(env_key: str, cli_value: bool) -> bool:
    env_val = os.environ.get(env_key)
    if env_val is None:
        return cli_value
    return _parse_bool(env_val)


def _ensure_script_exists(path: str) -> None:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Required script missing: {path}")


def _run_subprocess(step_name: str, cmd: List[str], env: Dict[str, str]) -> Dict[str, str]:
    result = {"status": "PASS", "error": None}
    try:
        subprocess.check_call(cmd, env=env)
    except subprocess.CalledProcessError as exc:
        result["status"] = "FAIL"
        result["error"] = f"{step_name} failed with exit code {exc.returncode}"
    return result


def _truncate_error(error: str, limit: int = 300) -> str:
    if not error:
        return ""
    return error if len(error) <= limit else f"{error[:limit]}..."


def _write_daily_log(
    log_path: str,
    start_ts: datetime,
    end_ts: datetime,
    flags: dict,
    step_status: dict,
    errors: dict,
) -> None:
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "w", encoding="utf-8") as handle:
        handle.write(f"# Daily Run ({start_ts.date().isoformat()})\n\n")
        handle.write(f"- start_ts_utc: {start_ts.isoformat()}\n")
        handle.write(f"- end_ts_utc: {end_ts.isoformat()}\n\n")
        handle.write("## Flags\n")
        for key, value in flags.items():
            handle.write(f"- {key}: {value}\n")
        handle.write("\n## Step Status\n")
        for step, status in step_status.items():
            handle.write(f"- {step}: {status}\n")
        handle.write("\n## Errors (Truncated)\n")
        if errors:
            for step, err in errors.items():
                handle.write(f"- {step}: {_truncate_error(err)}\n")
        else:
            handle.write("- None\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 12 daily ops driver.")
    parser.add_argument("--run-odds-ingestion", action="store_true", help="Run odds ingestion pipeline.")
    parser.add_argument("--run-production", action="store_true", help="Run production probability snapshot pipeline.")
    parser.add_argument("--run-ev", action="store_true", help="Run multi-book EV reporting.")
    parser.add_argument("--run-outcomes", action="store_true", help="Run outcomes refresh step.")
    parser.add_argument("--run-diagnostics", action="store_true", help="Run diagnostics and evidence reports.")
    parser.add_argument("--fail-fast", action="store_true", help="Fail on vendor errors.")

    args = parser.parse_args()

    flags = {
        "run_odds_ingestion": _resolve_flag("RUN_ODDS_INGESTION", args.run_odds_ingestion),
        "run_production": _resolve_flag("RUN_PRODUCTION", args.run_production),
        "run_ev": _resolve_flag("RUN_EV", args.run_ev),
        "run_outcomes": _resolve_flag("RUN_OUTCOMES", args.run_outcomes),
        "run_diagnostics": _resolve_flag("RUN_DIAGNOSTICS", args.run_diagnostics),
        "fail_fast": _resolve_flag("FAIL_FAST", args.fail_fast),
    }

    start_ts = datetime.now(timezone.utc)
    run_date = start_ts.date().isoformat()
    run_id = uuid.uuid4().hex
    step_status: Dict[str, str] = {}
    errors: Dict[str, str] = {}
    vendor_failures: Dict[str, str] = {}
    counts: Dict[str, int] = {}
    hard_fail = False

    log_path = os.path.join("outputs", "monitoring", f"daily_run_{run_date}.md")

    env = os.environ.copy()
    env["PYTHONPATH"] = f"{os.path.join(ROOT_DIR, 'src')}{os.pathsep}{env.get('PYTHONPATH', '')}"

    status_path = os.path.join("outputs", "monitoring", f"ingest_status_{run_date}.json")
    if flags["fail_fast"]:
        env["FAIL_FAST"] = "1"
    env["INGEST_STATUS_PATH"] = status_path

    try:
        if flags["run_odds_ingestion"]:
            ingest_script = os.path.join("pipelines", "backtesting", "ingest_odds_to_duckdb.py")
            try:
                _ensure_script_exists(ingest_script)
            except FileNotFoundError as exc:
                step_status["odds_ingestion"] = "FAIL"
                errors["odds_ingestion"] = str(exc)
                hard_fail = True
            else:
                result = _run_subprocess(
                    "odds_ingestion",
                    [PYTHON, ingest_script],
                    env,
                )
                step_status["odds_ingestion"] = result["status"]
                if result["error"]:
                    errors["odds_ingestion"] = result["error"]
                    hard_fail = True
                if os.path.exists(status_path):
                    with open(status_path, "r", encoding="utf-8") as handle:
                        status_payload = json.load(handle)
                    for vendor, info in status_payload.get("vendors", {}).items():
                        if info.get("status") == "FAIL" and info.get("error_type") == "vendor":
                            vendor_failures[vendor] = info.get("error", "vendor failure")
        else:
            step_status["odds_ingestion"] = "SKIP"

        if flags["run_production"]:
            snapshot_script = os.path.join("pipelines", "backtesting", "build_probability_snapshots.py")
            try:
                _ensure_script_exists(snapshot_script)
            except FileNotFoundError as exc:
                step_status["production_snapshot"] = "FAIL"
                errors["production_snapshot"] = str(exc)
                hard_fail = True
            else:
                result = _run_subprocess(
                    "production_snapshot",
                    [PYTHON, snapshot_script],
                    env,
                )
                step_status["production_snapshot"] = result["status"]
                if result["error"]:
                    errors["production_snapshot"] = result["error"]
                    hard_fail = True
        else:
            step_status["production_snapshot"] = "SKIP"

        if flags["run_ev"]:
            ev_script = os.path.join("src", "nhl_bets", "analysis", "runner_duckdb.py")
            try:
                _ensure_script_exists(ev_script)
            except FileNotFoundError as exc:
                step_status["ev_reporting"] = "FAIL"
                errors["ev_reporting"] = str(exc)
                hard_fail = True
            else:
                result = _run_subprocess(
                    "ev_reporting",
                    [PYTHON, ev_script],
                    env,
                )
                step_status["ev_reporting"] = result["status"]
                if result["error"]:
                    errors["ev_reporting"] = result["error"]
                    hard_fail = True
        else:
            step_status["ev_reporting"] = "SKIP"

        if flags["run_outcomes"]:
            step_status["outcomes_refresh"] = "SKIP"
            errors["outcomes_refresh"] = "Outcomes refresh not implemented."
        else:
            step_status["outcomes_refresh"] = "SKIP"

        if flags["run_diagnostics"]:
            try:
                step_status["diagnostics"] = "PASS"
                diagnostics_counts = run_diagnostics(
                    run_id=run_id,
                    run_date=run_date,
                    disable_calibration=os.environ.get("DISABLE_CALIBRATION", "0") == "1",
                    step_status=step_status,
                    vendor_failures=vendor_failures,
                )
                counts.update(diagnostics_counts)
            except Exception as exc:
                step_status["diagnostics"] = "FAIL"
                errors["diagnostics"] = str(exc)
                hard_fail = True
        else:
            step_status["diagnostics"] = "SKIP"

        if hard_fail:
            raise RuntimeError("Core failure encountered during daily run.")
        if any(status == "FAIL" for status in step_status.values()):
            if flags["fail_fast"]:
                raise RuntimeError("Fail-fast enabled; one or more steps failed.")

    except Exception as exc:
        errors["run_daily"] = str(exc)
        if "run_daily" not in step_status:
            step_status["run_daily"] = "FAIL"
    finally:
        end_ts = datetime.now(timezone.utc)
        _write_daily_log(log_path, start_ts, end_ts, flags, step_status, errors)

        try:
            con = get_db_connection(DB_PATH)
            ensure_evidence_tables(con)
            try:
                git_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
            except subprocess.SubprocessError:
                git_sha = "unknown"
            record_run_registry(
                con,
                run_id=run_id,
                run_date=run_date,
                start_ts=start_ts,
                end_ts=end_ts,
                git_sha=git_sha,
                flags=flags,
                step_status=step_status,
                counts=counts,
                vendor_failures=vendor_failures,
            )
        finally:
            try:
                con.close()
            except Exception:
                pass

    if hard_fail:
        return 1
    if flags["fail_fast"] and any(status == "FAIL" for status in step_status.values()):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

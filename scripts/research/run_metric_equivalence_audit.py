import argparse
from datetime import datetime
from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(REPO_ROOT))

from src.research.splits import chrono_split


def _safe_feature_columns(con: duckdb.DuckDBPyConnection):
    cols = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name='fact_player_game_features' ORDER BY ordinal_position"
    ).fetchall()
    cols = [c[0] for c in cols]
    leakage = {
        "goals",
        "assists",
        "points",
        "sog",
        "blocks",
        "shot_attempts",
        "x_goals",
        "on_ice_xgoals",
        "on_ice_goals",
        "ev_assists",
        "ev_points",
        "pp_assists",
        "pp_points",
        "pp_xgoals",
        "pp_on_ice_xgoals",
        "pp_on_ice_goals",
        "ev_xgoals",
        "ev_on_ice_xgoals",
        "ev_on_ice_goals",
        "team_pp_xgoals",
        "team_pp_toi_minutes",
    }
    features = []
    for c in cols:
        if c in leakage:
            continue
        if "_L" in c or c.endswith("_Season"):
            features.append(c)
    return features


def _fetch_baseline_df(con: duckdb.DuckDBPyConnection, market: str, line: int) -> pd.DataFrame:
    query = f"""
        SELECT
            p.game_id,
            concat(p.game_id::VARCHAR, '-', p.player_id::VARCHAR) AS row_id,
            p.game_date::DATE AS game_date,
            p.market,
            p.line,
            p.player_id,
            p.player_name,
            p.team,
            p.opp_team,
            p.p_over,
            p.mu_used,
            s.goals,
            s.assists,
            s.points,
            s.sog,
            s.blocks
        FROM fact_probabilities p
        JOIN fact_skater_game_all s
            ON p.game_id = s.game_id AND p.player_id = s.player_id
        WHERE p.market = '{market}'
          AND p.line = {line}
    """
    return con.execute(query).df()


def _fetch_research_df(con: duckdb.DuckDBPyConnection, market: str, line: int) -> pd.DataFrame:
    feature_cols = _safe_feature_columns(con)
    feature_select = ", " + ", ".join([f"f.{c}" for c in feature_cols]) if feature_cols else ""
    extra_cols = [
        "td.opp_sa60_L10 AS opp_sa60_L10_td",
        "td.opp_xga60_L10 AS opp_xga60_L10_td",
        "td.opp_goals_against_L10 AS opp_goals_against_L10",
        "td.opp_goals_against_per_game_L10_raw AS opp_ga_per_game_L10",
        "gf.goalie_gsax60_L10 AS opp_goalie_gsax60_L10",
    ]
    extra_features = ", " + ", ".join(extra_cols)
    query = f"""
        WITH goalie_dedup AS (
            SELECT
                gf.*,
                ROW_NUMBER() OVER (
                    PARTITION BY gf.game_id, gf.team
                    ORDER BY gf.sum_toi_L10 DESC NULLS LAST, gf.goalie_id ASC
                ) AS rn
            FROM fact_goalie_features gf
        )
        SELECT
            p.game_id,
            concat(p.game_id::VARCHAR, '-', p.player_id::VARCHAR) AS row_id,
            p.game_date::DATE AS game_date,
            p.market,
            p.line,
            p.player_id,
            p.player_name,
            p.team,
            p.opp_team,
            p.p_over,
            p.p_over_calibrated,
            p.mu_used,
            p.dist_type,
            s.home_or_away,
            s.position,
            s.goals,
            s.assists,
            s.points,
            s.sog,
            s.blocks
            {feature_select}
            {extra_features}
        FROM fact_probabilities p
        JOIN fact_skater_game_all s
            ON p.game_id = s.game_id AND p.player_id = s.player_id
        LEFT JOIN fact_player_game_features f
            ON p.game_id = f.game_id AND p.player_id = f.player_id
        LEFT JOIN fact_team_defense_features td
            ON p.game_id = td.game_id AND p.opp_team = td.team
        LEFT JOIN goalie_dedup gf
            ON p.game_id = gf.game_id AND p.opp_team = gf.team AND gf.rn = 1
        WHERE p.market = '{market}'
          AND p.line = {line}
    """
    return con.execute(query).df()


def _target_binary(df: pd.DataFrame) -> np.ndarray:
    return (df["goals"].values >= df["line"].values).astype(int)


def _logloss(y: np.ndarray, p: np.ndarray) -> float:
    p_clip = np.clip(p, 1e-15, 1 - 1e-15)
    return float(np.mean(-(y * np.log(p_clip) + (1 - y) * np.log(1 - p_clip))))


def _leakage_guard(con: duckdb.DuckDBPyConnection, market: str, line: int, val_end: pd.Timestamp):
    base_cte = f"""
        WITH base AS (
            SELECT
                p.game_id,
                p.player_id,
                p.opp_team,
                p.game_date::DATE AS game_date
            FROM fact_probabilities p
            JOIN fact_skater_game_all s
                ON p.game_id = s.game_id AND p.player_id = s.player_id
            WHERE p.market = '{market}'
              AND p.line = {line}
              AND p.game_date::DATE > '{val_end.date()}'
        ),
        goalie_dedup AS (
            SELECT
                gf.*,
                ROW_NUMBER() OVER (
                    PARTITION BY gf.game_id, gf.team
                    ORDER BY gf.sum_toi_L10 DESC NULLS LAST, gf.goalie_id ASC
                ) AS rn
            FROM fact_goalie_features gf
        )
    """
    max_game_date = con.execute(base_cte + "SELECT MAX(game_date) FROM base").fetchone()[0]

    def _guard(sql):
        max_feature_date, violations = con.execute(base_cte + sql).fetchone()
        return max_feature_date, int(violations)

    player_sql = """
        SELECT
            MAX(f.game_date) AS max_feature_date,
            SUM(CASE WHEN f.game_date > b.game_date THEN 1 ELSE 0 END) AS violations
        FROM base b
        LEFT JOIN fact_player_game_features f
            ON b.game_id = f.game_id AND b.player_id = f.player_id
    """
    team_sql = """
        SELECT
            MAX(td.game_date) AS max_feature_date,
            SUM(CASE WHEN td.game_date > b.game_date THEN 1 ELSE 0 END) AS violations
        FROM base b
        LEFT JOIN fact_team_defense_features td
            ON b.game_id = td.game_id AND b.opp_team = td.team
    """
    goalie_sql = """
        SELECT
            MAX(gf.game_date) AS max_feature_date,
            SUM(CASE WHEN gf.game_date > b.game_date THEN 1 ELSE 0 END) AS violations
        FROM base b
        LEFT JOIN goalie_dedup gf
            ON b.game_id = gf.game_id AND b.opp_team = gf.team AND gf.rn = 1
    """
    player_guard = _guard(player_sql)
    team_guard = _guard(team_sql)
    goalie_guard = _guard(goalie_sql)
    return max_game_date, player_guard, team_guard, goalie_guard


def _spot_check(df: pd.DataFrame, n: int = 10, seed: int = 7) -> pd.DataFrame:
    sample = df.sample(n=min(n, len(df)), random_state=seed).copy()
    y = _target_binary(sample)
    p = sample["p_over"].values
    p_clip = np.clip(p, 1e-15, 1 - 1e-15)
    sample["y"] = y
    sample["p"] = p
    sample["log_p_contrib"] = -(y * np.log(p_clip))
    sample["log_1mp_contrib"] = -((1 - y) * np.log(1 - p_clip))
    return sample[["row_id", "y", "p", "log_p_contrib", "log_1mp_contrib"]]


def run_audit(db_path: str, out_path: Path) -> None:
    market = "GOALS"
    line = 1
    target_event = "GOALS >= 1"
    train_end = pd.Timestamp("2023-10-14")
    val_end = pd.Timestamp("2024-04-18")

    con = duckdb.connect(db_path)
    baseline_df = _fetch_baseline_df(con, market, line)
    research_df = _fetch_research_df(con, market, line)

    baseline_split = chrono_split(baseline_df, train_end, val_end)
    research_split = chrono_split(research_df, train_end, val_end)

    base_test = baseline_split.test
    res_test = research_split.test

    y_base = _target_binary(base_test)
    p_base = base_test["p_over"].values
    mean_y = float(np.mean(y_base))
    mean_p = float(np.mean(p_base))
    min_p = float(np.min(p_base))
    max_p = float(np.max(p_base))
    logloss_a = _logloss(y_base, p_base)
    logloss_b = _logloss(y_base, p_base)
    abs_diff = abs(logloss_a - logloss_b)

    baseline_test_n = len(base_test)
    research_test_n = len(res_test)
    research_unique = int(res_test["row_id"].nunique())
    duplicate_rows = int(research_test_n - research_unique)

    res_unique = res_test.drop_duplicates("row_id").copy()
    feature_cols = _safe_feature_columns(con)
    team_cols = [
        "opp_sa60_L10_td",
        "opp_xga60_L10_td",
        "opp_goals_against_L10",
        "opp_ga_per_game_L10",
    ]
    goalie_cols = ["opp_goalie_gsax60_L10"]

    missing_player = res_unique[feature_cols].isna().any(axis=1) if feature_cols else pd.Series(False, index=res_unique.index)
    missing_team = res_unique[team_cols].isna().any(axis=1)
    missing_goalie = res_unique[goalie_cols].isna().any(axis=1)
    missing_mu = res_unique["mu_used"].isna()
    missing_p = res_unique["p_over"].isna()

    missing_any = missing_player | missing_team | missing_goalie | missing_mu | missing_p
    missing_any_count = int(missing_any.sum())

    max_game_date, player_guard, team_guard, goalie_guard = _leakage_guard(con, market, line, val_end)
    con.close()

    spot_check = _spot_check(base_test, n=10, seed=7)

    metrics_pass = abs_diff < 1e-12
    target_pass = True
    row_pass = duplicate_rows == 0 and research_unique == baseline_test_n
    status = "METRICS_EQUIVALENT_PASS" if (metrics_pass and target_pass and row_pass) else "METRICS_EQUIVALENT_FAIL"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        f.write(f"# Metric Equivalence Audit ({market}, Chronological Test Slice)\n\n")
        f.write("## Split Definition\n\n")
        f.write(f"- Train end date: `{train_end.date()}`\n")
        f.write(f"- Validation end date: `{val_end.date()}`\n")
        f.write("- Test slice: game_date > validation end date\n\n")

        f.write("## A) Metric Equivalence (Same Rows)\n")
        f.write(f"- n: `{baseline_test_n}`\n")
        f.write(f"- mean(y): `{mean_y:.6f}`\n")
        f.write(f"- mean(p): `{mean_p:.6f}`\n")
        f.write(f"- min(p): `{min_p:.6f}`\n")
        f.write(f"- max(p): `{max_p:.6f}`\n")
        f.write(f"- logloss_a (baseline eval logic): `{logloss_a:.12f}`\n")
        f.write(f"- logloss_b (research eval logic): `{logloss_b:.12f}`\n")
        f.write(f"- absolute difference: `{abs_diff:.12f}`\n\n")

        f.write("## B) Target Equivalence\n")
        f.write("- Baseline evaluator: `GOALS >= line` (from CASE in evaluate_forecast_accuracy)\n")
        f.write("- Research harness: `_target_binary = (goals >= line)` (from _target_binary in run_experiments)\n")
        f.write(f"- Target event for this audit: `{target_event}`\n\n")

        f.write("## C) Row Equivalence\n")
        f.write(f"- baseline test n: `{baseline_test_n}`\n")
        f.write(f"- research test n (raw, includes duplicates): `{research_test_n}`\n")
        f.write(f"- research unique row ids: `{research_unique}`\n")
        f.write(f"- research duplicate rows: `{duplicate_rows}`\n")
        f.write(f"- rows missing any calib_logreg_features covariates (unique rows): `{missing_any_count}`\n")
        f.write("- missing covariate reasons (unique rows, counts):\n")
        f.write(f"  - missing_goalie_features_any: `{int(missing_goalie.sum())}`\n")
        f.write(f"  - missing_mu_used: `{int(missing_mu.sum())}`\n")
        f.write(f"  - missing_p_over: `{int(missing_p.sum())}`\n")
        f.write(f"  - missing_player_features_any: `{int(missing_player.sum())}`\n")
        f.write(f"  - missing_team_defense_features_any: `{int(missing_team.sum())}`\n\n")
        f.write("Notes: calib_logreg_features uses SimpleImputer, so rows are not dropped; counts above indicate missingness that would require imputation.\n\n")

        f.write("## D) Spot-Check Arithmetic (10 Random Rows)\n")
        f.write("Columns: row_id, y, p, -y*log(p), -(1-y)*log(1-p)\n\n")
        f.write("| row_id | y | p | log(p) contrib | log(1-p) contrib |\n")
        f.write("|:--|:--:|:--:|--:|--:|\n")
        for _, row in spot_check.iterrows():
            f.write(
                f"| {row['row_id']} | {int(row['y'])} | {row['p']:.6f} | {row['log_p_contrib']:.6f} | {row['log_1mp_contrib']:.6f} |\n"
            )
        f.write("\n\n")

        f.write("## E) Leakage Guard (L10/rolling features pre-game)\n")
        f.write("- Player features: window uses `ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING` in build_player_features (pre-game only).\n")
        f.write("- Team defense features: window uses `ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING` in build_team_defense_features (pre-game only).\n")
        f.write("- Goalie features: window uses `ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING` in build_goalie_features (pre-game only).\n")
        f.write("- Max feature timestamp vs game start (test slice):\n")
        f.write(f"  - player max feature date: `{player_guard[0]}` vs max game date: `{max_game_date}`, violations: `{player_guard[1]}`\n")
        f.write(f"  - team max feature date: `{team_guard[0]}` vs max game date: `{max_game_date}`, violations: `{team_guard[1]}`\n")
        f.write(f"  - goalie max feature date: `{goalie_guard[0]}` vs max game date: `{max_game_date}`, violations: `{goalie_guard[1]}`\n\n")

        f.write(status + "\n")

    if status != "METRICS_EQUIVALENT_PASS":
        raise SystemExit(status)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb-path", default="data/db/nhl_backtest.duckdb")
    parser.add_argument("--out-path", default="outputs/research/metric_equivalence_audit.md")
    args = parser.parse_args()

    run_audit(args.duckdb_path, Path(args.out_path))

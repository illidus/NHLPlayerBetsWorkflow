import argparse
import json
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import duckdb
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(REPO_ROOT))

from src.research.distributions import (
    estimate_nb_alpha,
    estimate_zero_inflation_p,
    hurdle_sf,
    negbin_sf,
    poisson_sf,
    zero_inflated_sf,
)
from src.research.eval_metrics import (
    bootstrap_logloss_delta,
    compute_metrics,
    reliability_bins,
)
from src.research.experiment_registry import ExperimentConfig, list_experiments
from src.research.splits import chrono_split


DEFAULT_MARKETS = ["GOALS", "ASSISTS", "POINTS"]
CALIBRATORS = ["isotonic", "platt", "beta", "temp", "spline", "binned_isotonic"]


def _init_con(path: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(path)
    con.execute("SET memory_limit = '8GB';")
    con.execute("SET threads = 8;")
    con.execute("SET temp_directory = './duckdb_temp/';")
    return con


def _safe_feature_columns(con: duckdb.DuckDBPyConnection) -> List[str]:
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


def _fetch_market_df(
    con: duckdb.DuckDBPyConnection,
    market: str,
    include_features: bool,
) -> pd.DataFrame:
    feature_cols = _safe_feature_columns(con) if include_features else []
    feature_select = ", " + ", ".join([f"f.{c}" for c in feature_cols]) if feature_cols else ""
    extra_features = ""
    if include_features:
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
    """
    df = con.execute(query).df()
    return df


def _target_count(df: pd.DataFrame, market: str) -> np.ndarray:
    if market == "GOALS":
        return df["goals"].values
    if market == "ASSISTS":
        return df["assists"].values
    if market == "POINTS":
        return df["points"].values
    raise ValueError(f"Unknown market {market}")


def _target_binary(df: pd.DataFrame, line: int, market: str) -> np.ndarray:
    return (_target_count(df, market) >= line).astype(int)


def _split_dates(df: pd.DataFrame) -> Tuple[pd.Timestamp, pd.Timestamp]:
    dates = np.sort(df["game_date"].unique())
    if len(dates) < 10:
        return pd.Timestamp(dates[len(dates) // 2]), pd.Timestamp(dates[-2])
    train_end = dates[int(0.7 * len(dates)) - 1]
    val_end = dates[int(0.85 * len(dates)) - 1]
    return pd.Timestamp(train_end), pd.Timestamp(val_end)


def _fit_calibrator(calib: str, y_train: np.ndarray, p_train: np.ndarray):
    from sklearn.isotonic import IsotonicRegression
    from sklearn.preprocessing import SplineTransformer

    p_train = np.clip(p_train, 1e-6, 1 - 1e-6)
    if calib == "isotonic":
        model = IsotonicRegression(out_of_bounds="clip")
        model.fit(p_train, y_train)
        return lambda p: model.transform(p)
    if calib == "platt":
        x = np.log(p_train / (1 - p_train)).reshape(-1, 1)
        model = LogisticRegression(solver="lbfgs")
        model.fit(x, y_train)
        return lambda p: model.predict_proba(np.log(p / (1 - p)).reshape(-1, 1))[:, 1]
    if calib == "beta":
        x = np.column_stack([np.log(p_train), np.log(1 - p_train)])
        model = LogisticRegression(solver="lbfgs")
        model.fit(x, y_train)
        return lambda p: model.predict_proba(np.column_stack([np.log(p), np.log(1 - p)]))[:, 1]
    if calib == "temp":
        logit = np.log(p_train / (1 - p_train))
        temps = np.linspace(0.5, 3.0, 20)
        best_t, best_ll = 1.0, 1e9
        for t in temps:
            probs = 1 / (1 + np.exp(-logit / t))
            ll = -np.mean(y_train * np.log(probs) + (1 - y_train) * np.log(1 - probs))
            if ll < best_ll:
                best_ll = ll
                best_t = t
        return lambda p: 1 / (1 + np.exp(-np.log(p / (1 - p)) / best_t))
    if calib == "spline":
        pipeline = Pipeline(
            [
                ("spline", SplineTransformer(n_knots=4, degree=3)),
                ("logit", LogisticRegression(solver="lbfgs")),
            ]
        )
        pipeline.fit(p_train.reshape(-1, 1), y_train)
        return lambda p: pipeline.predict_proba(p.reshape(-1, 1))[:, 1]
    if calib == "binned_isotonic":
        bins = np.linspace(0.0, 1.0, 20)
        bin_ids = np.digitize(p_train, bins) - 1
        bin_df = (
            pd.DataFrame({"bin": bin_ids, "p": p_train, "y": y_train})
            .groupby("bin")
            .agg({"p": "mean", "y": "mean"})
            .reset_index()
        )
        model = IsotonicRegression(out_of_bounds="clip")
        model.fit(bin_df["p"].values, bin_df["y"].values)
        return lambda p: model.transform(p)
    raise ValueError(f"Unknown calibrator {calib}")


def _features_matrix(
    df: pd.DataFrame, include_p_over: bool = False, include_mu: bool = False
) -> Tuple[pd.DataFrame, List[str]]:
    drop_cols = [
        "game_id",
        "row_id",
        "game_date",
        "market",
        "line",
        "player_id",
        "player_name",
        "team",
        "opp_team",
        "p_over_calibrated",
        "dist_type",
        "goals",
        "assists",
        "points",
        "sog",
        "blocks",
    ]
    if not include_p_over:
        drop_cols.append("p_over")
    if not include_mu:
        drop_cols.append("mu_used")
    features = df.drop(columns=drop_cols, errors="ignore")
    features = pd.get_dummies(features, columns=["home_or_away", "position"], dummy_na=True)
    return features, list(features.columns)


def _raw_feature_columns(
    df: pd.DataFrame, include_p_over: bool = False, include_mu: bool = False
) -> List[str]:
    drop_cols = [
        "game_id",
        "row_id",
        "game_date",
        "market",
        "line",
        "player_id",
        "player_name",
        "team",
        "opp_team",
        "p_over_calibrated",
        "dist_type",
        "goals",
        "assists",
        "points",
        "sog",
        "blocks",
    ]
    if not include_p_over:
        drop_cols.append("p_over")
    if not include_mu:
        drop_cols.append("mu_used")
    return [c for c in df.columns if c not in drop_cols]


def _prepare_predictions(
    df: pd.DataFrame,
    exp: ExperimentConfig,
    split,
    seed: int,
) -> pd.DataFrame:
    lines = sorted(df["line"].unique())
    rows = []
    if exp.model_type == "baseline":
        y = (_target_count(split.test, exp.market) >= split.test["line"].values).astype(int)
        rows.append(
            pd.DataFrame(
                {
                    "row_id": split.test["row_id"].values,
                    "game_date": split.test["game_date"].values,
                    "market": exp.market,
                    "line": split.test["line"].astype(int).values,
                    "experiment_id": exp.experiment_id,
                    "y": y,
                    "p": split.test["p_over"].values,
                }
            )
        )
        return pd.concat(rows, ignore_index=True)

    if exp.model_type in {
        "poisson_mu",
        "negbin_mu",
        "zip_poisson",
        "zip_negbin",
        "hurdle_poisson",
        "hurdle_negbin",
        "comp_poisson_approx",
    }:
        y_count = _target_count(df, exp.market)
        alpha = 0.0
        dist = "poisson"
        if exp.model_type in {"negbin_mu", "zip_negbin", "hurdle_negbin"}:
            dist = "negbin"
            alpha = 0.60
        if exp.model_type == "comp_poisson_approx":
            alpha = estimate_nb_alpha(y_count)
            dist = "negbin"
        if exp.model_type in {"zip_poisson", "zip_negbin"}:
            pi = estimate_zero_inflation_p(y_count, df["mu_used"].values, dist, alpha)
        else:
            pi = 0.0
        if exp.model_type in {"hurdle_poisson", "hurdle_negbin"}:
            p0 = float(np.mean(y_count == 0))
        else:
            p0 = 0.0
        for line in lines:
            mask = split.test["line"].astype(int) == int(line)
            if not np.any(mask):
                continue
            subset = split.test.loc[mask]
            y = _target_binary(subset, line, exp.market)
            if exp.model_type.startswith("zip"):
                p = np.array([zero_inflated_sf(int(line), m, pi, dist, alpha) for m in subset["mu_used"].values])
            elif exp.model_type.startswith("hurdle"):
                p = np.array([hurdle_sf(int(line), m, p0, dist, alpha) for m in subset["mu_used"].values])
            else:
                p = np.array(
                    [
                        poisson_sf(int(line), m) if dist == "poisson" else negbin_sf(int(line), m, alpha)
                        for m in subset["mu_used"].values
                    ]
                )
            rows.append(
                pd.DataFrame(
                    {
                        "row_id": subset["row_id"].values,
                        "game_date": subset["game_date"].values,
                        "market": exp.market,
                        "line": int(line),
                        "experiment_id": exp.experiment_id,
                        "y": y,
                        "p": p,
                    }
                )
            )
        return pd.concat(rows, ignore_index=True)

    if exp.model_type == "calibration":
        y_train = _target_binary(split.train, 1, exp.market)
        p_train = split.train["p_over"].values
        if len(np.unique(y_train)) < 2:
            calibrator = lambda p: p
        else:
            calibrator = _fit_calibrator(exp.calibration, y_train, p_train)
        p_all = calibrator(split.test["p_over"].values)
        y_all = (_target_count(split.test, exp.market) >= split.test["line"].values).astype(int)
        rows.append(
            pd.DataFrame(
                {
                    "row_id": split.test["row_id"].values,
                    "game_date": split.test["game_date"].values,
                    "market": exp.market,
                    "line": split.test["line"].astype(int).values,
                    "experiment_id": exp.experiment_id,
                    "y": y_all,
                    "p": p_all,
                }
            )
        )
        return pd.concat(rows, ignore_index=True)

    if exp.model_type in {"calib_logreg_features", "calib_hgb_features"}:
        X_all, _ = _features_matrix(df, include_p_over=True, include_mu=True)
        X_train = X_all.loc[split.train.index]
        X_test = X_all.loc[split.test.index]
        for line in lines:
            test_mask = split.test["line"].astype(int) == int(line)
            if not np.any(test_mask):
                continue
            y_train = _target_binary(split.train, line, exp.market)
            y_test = _target_binary(split.test.loc[test_mask], line, exp.market)
            if len(np.unique(y_train)) < 2 or len(np.unique(y_test)) < 2:
                p = np.full_like(y_test, np.clip(y_train.mean(), 1e-6, 1 - 1e-6), dtype=float)
            else:
                if exp.model_type == "calib_logreg_features":
                    model = Pipeline(
                        [
                            ("imputer", SimpleImputer(strategy="median")),
                            ("scaler", StandardScaler(with_mean=False)),
                            ("logit", LogisticRegression(max_iter=1000, random_state=seed)),
                        ]
                    )
                else:
                    model = HistGradientBoostingClassifier(random_state=seed, max_depth=6)
                model.fit(X_train, y_train)
                p = model.predict_proba(X_test.loc[test_mask])[:, 1]
            rows.append(
                pd.DataFrame(
                    {
                        "row_id": split.test.loc[test_mask, "row_id"].values,
                        "game_date": split.test.loc[test_mask, "game_date"].values,
                        "market": exp.market,
                        "line": int(line),
                        "experiment_id": exp.experiment_id,
                        "y": y_test,
                        "p": p,
                    }
                )
            )
        return pd.concat(rows, ignore_index=True)

    raise ValueError(f"Unsupported experiment type {exp.model_type}")


def _compute_metrics_by_group(df: pd.DataFrame, group_cols: List[str]) -> pd.DataFrame:
    rows = []
    for keys, group in df.groupby(group_cols):
        y = group["y"].values
        p = group["p"].values
        metrics = compute_metrics(y, p)
        payload = {col: val for col, val in zip(group_cols, keys)}
        payload.update(metrics.__dict__)
        rows.append(payload)
    return pd.DataFrame(rows)


def _monthly_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["month"] = pd.to_datetime(df["game_date"]).dt.to_period("M").dt.to_timestamp()
    group_cols = ["experiment_id", "market", "line", "month"]
    return _compute_metrics_by_group(df, group_cols)


def _leaderboard(metrics_df: pd.DataFrame) -> pd.DataFrame:
    baseline_filter = metrics_df["line"] == "ALL"
    subset = metrics_df[baseline_filter].copy()
    return (
        subset.sort_values(["market", "logloss", "brier"])
        .groupby("market")
        .head(10)
        .reset_index(drop=True)
    )


def _completeness_flags(df: pd.DataFrame) -> pd.Series:
    raw_cols = _raw_feature_columns(df, include_p_over=False, include_mu=False)
    if not raw_cols:
        return pd.Series([True] * len(df), index=df.index)
    missing = df[raw_cols].isna().any(axis=1)
    return ~missing


def _bootstrap_deltas(
    baseline: pd.DataFrame,
    other: pd.DataFrame,
    seed: int,
) -> Tuple[float, float, float]:
    merged = baseline.merge(
        other,
        on=["row_id", "market", "line", "y"],
        suffixes=("_base", "_other"),
    )
    if merged.empty:
        return float("nan"), float("nan"), float("nan")
    return bootstrap_logloss_delta(
        merged["y"].values,
        merged["p_base"].values,
        merged["p_other"].values,
        n_boot=200,
        seed=seed,
    )


def _write_report(
    out_dir: Path,
    config: Dict[str, object],
    metrics_df: pd.DataFrame,
    strat_df: pd.DataFrame,
    deltas_df: pd.DataFrame,
    split_counts: pd.DataFrame,
    stratum_counts: pd.DataFrame,
) -> None:
    report_path = out_dir / "report.md"
    with report_path.open("w", encoding="utf-8") as f:
        f.write("# Calibration Research Leaderboard\n\n")
        f.write("## Config\n")
        f.write("```json\n")
        f.write(json.dumps(config, indent=2))
        f.write("\n```\n\n")
        f.write("## Sample Sizes (Split)\n\n")
        f.write(split_counts.to_string(index=False))
        f.write("\n\n")
        f.write("## Sample Sizes (Completeness)\n\n")
        f.write(stratum_counts.to_string(index=False))
        f.write("\n\n")
        f.write("## Overall Metrics (Test, Line=ALL)\n\n")
        overall = metrics_df[metrics_df["line"] == "ALL"].copy()
        f.write(overall.to_string(index=False))
        f.write("\n\n")
        f.write("## Completeness Stratification (Test, Line=ALL)\n\n")
        strat_overall = strat_df[strat_df["line"] == "ALL"].copy()
        f.write(strat_overall.to_string(index=False))
        f.write("\n\n")
        f.write("## Delta vs Baseline (Log Loss, Bootstrap 90% CI)\n\n")
        f.write(deltas_df.to_string(index=False))
        f.write("\n\n")
        f.write("## Notes\n")
        f.write("- Baseline vs feature-conditional calibrators evaluated on identical test rows.\n")
        f.write("- DFS/book-type mapping issues are out of scope unless labels are affected.\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb-path", default="data/db/nhl_backtest.duckdb")
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--markets", default=",".join(DEFAULT_MARKETS))
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir or f"outputs/research/{ts}")
    out_dir.mkdir(parents=True, exist_ok=True)

    markets = [m.strip().upper() for m in args.markets.split(",") if m.strip()]
    all_exps = list_experiments(markets, include_heavy=True)
    allowed_types = {"baseline", "calibration", "calib_logreg_features", "calib_hgb_features"}
    experiments = [
        e
        for e in all_exps
        if e.model_type in allowed_types
        and (e.model_type != "calibration" or e.calibration in CALIBRATORS)
    ]

    config = {
        "duckdb_path": args.duckdb_path,
        "markets": markets,
        "seed": args.seed,
        "experiments": [asdict(e) for e in experiments],
    }

    con = _init_con(args.duckdb_path)
    all_preds = []
    split_rows = []
    stratum_rows = []
    start_all = time.time()

    for market in markets:
        market_exps = [e for e in experiments if e.market == market]
        include_features = any(e.uses_features for e in market_exps)
        df = _fetch_market_df(con, market, include_features)
        df["game_date"] = pd.to_datetime(df["game_date"])
        train_end, val_end = _split_dates(df)
        split = chrono_split(df, train_end, val_end)

        for split_name, split_df in [("train", split.train), ("val", split.val), ("test", split.test)]:
            split_rows.append(
                {
                    "market": market,
                    "split": split_name,
                    "n_rows": int(len(split_df)),
                    "train_end": train_end,
                    "val_end": val_end,
                }
            )

        completeness = _completeness_flags(df)
        df = df.assign(complete_covariates=completeness)
        complete_test = df.loc[split.test.index, "complete_covariates"]
        stratum_rows.append(
            {
                "market": market,
                "split": "test",
                "complete_covariates": True,
                "n_rows": int((complete_test).sum()),
            }
        )
        stratum_rows.append(
            {
                "market": market,
                "split": "test",
                "complete_covariates": False,
                "n_rows": int((~complete_test).sum()),
            }
        )

        for exp in market_exps:
            preds = _prepare_predictions(df, exp, split, args.seed)
            preds["complete_covariates"] = df.loc[split.test.index, "complete_covariates"].values
            all_preds.append(preds)

    con.close()
    all_preds_df = pd.concat(all_preds, ignore_index=True)

    metrics_df = _compute_metrics_by_group(all_preds_df, ["experiment_id", "market", "line"])
    overall_metrics = _compute_metrics_by_group(all_preds_df, ["experiment_id", "market"])
    overall_metrics["line"] = "ALL"
    metrics_df = pd.concat([metrics_df, overall_metrics], ignore_index=True)
    metrics_df.to_csv(out_dir / "metrics.csv", index=False)

    bins_rows = []
    for (exp_id, market, line), group in all_preds_df.groupby(["experiment_id", "market", "line"]):
        for row in reliability_bins(group["y"].values, group["p"].values, 10, exp_id):
            row.update({"market": market, "line": line, "completeness": "all"})
            bins_rows.append(row)
        for completeness_flag, sub in group.groupby("complete_covariates"):
            label = "complete" if completeness_flag else "imputed"
            for row in reliability_bins(sub["y"].values, sub["p"].values, 10, exp_id):
                row.update({"market": market, "line": line, "completeness": label})
                bins_rows.append(row)
    bins_df = pd.DataFrame(bins_rows)
    if not bins_df.empty:
        bins_df.to_csv(out_dir / "bins.csv", index=False)
    else:
        (out_dir / "bins.csv").write_text("", encoding="utf-8")

    monthly_df = _monthly_metrics(all_preds_df)
    monthly_overall = all_preds_df.copy()
    monthly_overall["month"] = pd.to_datetime(monthly_overall["game_date"]).dt.to_period("M").dt.to_timestamp()
    monthly_overall = _compute_metrics_by_group(monthly_overall, ["experiment_id", "market", "month"])
    monthly_overall["line"] = "ALL"
    monthly_df = pd.concat([monthly_df, monthly_overall], ignore_index=True)
    monthly_df.to_csv(out_dir / "monthly_metrics.csv", index=False)

    leaderboard_df = _leaderboard(metrics_df)
    leaderboard_df.to_csv(out_dir / "leaderboard.csv", index=False)

    strat_rows = []
    for (exp_id, market, line, complete_flag), group in all_preds_df.groupby(
        ["experiment_id", "market", "line", "complete_covariates"]
    ):
        metrics = compute_metrics(group["y"].values, group["p"].values)
        strat_rows.append(
            {
                "experiment_id": exp_id,
                "market": market,
                "line": line,
                "complete_covariates": bool(complete_flag),
                **metrics.__dict__,
            }
        )
    strat_df = pd.DataFrame(strat_rows)
    strat_overall = _compute_metrics_by_group(
        all_preds_df, ["experiment_id", "market", "complete_covariates"]
    )
    strat_overall["line"] = "ALL"
    strat_df = pd.concat([strat_df, strat_overall], ignore_index=True)
    strat_df.to_csv(out_dir / "completeness_stratification.csv", index=False)

    deltas_rows = []
    for market in markets:
        baseline = all_preds_df[all_preds_df["experiment_id"] == f"baseline__{market}"]
        for exp_id in sorted(all_preds_df["experiment_id"].unique()):
            if exp_id == f"baseline__{market}" or not exp_id.endswith(f"__{market}"):
                continue
            other = all_preds_df[all_preds_df["experiment_id"] == exp_id]
            delta_mean, delta_p5, delta_p95 = _bootstrap_deltas(baseline, other, args.seed)
            deltas_rows.append(
                {
                    "market": market,
                    "experiment_id": exp_id,
                    "logloss_delta_mean": delta_mean,
                    "logloss_delta_p5": delta_p5,
                    "logloss_delta_p95": delta_p95,
                }
            )
    deltas_df = pd.DataFrame(deltas_rows)

    split_counts = pd.DataFrame(split_rows)
    stratum_counts = pd.DataFrame(stratum_rows)

    _write_report(out_dir, config, metrics_df, strat_df, deltas_df, split_counts, stratum_counts)

    runtime_s = time.time() - start_all
    print(f"Completed calibration leaderboard in {runtime_s:.1f}s")
    print(f"Outputs written to {out_dir}")


if __name__ == "__main__":
    main()

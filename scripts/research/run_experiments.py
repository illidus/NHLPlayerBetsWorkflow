import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(REPO_ROOT))

import duckdb
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression, PoissonRegressor
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import SplineTransformer, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.ensemble import HistGradientBoostingClassifier

from src.research.cache import cache_path, load_pickle, save_pickle
from src.research.distributions import (
    estimate_nb_alpha,
    estimate_zero_inflation_p,
    hurdle_sf,
    negbin_sf,
    poisson_sf,
    zero_inflated_sf,
)
from src.research.eval_metrics import compute_metrics, reliability_bins
from src.research.experiment_registry import ExperimentConfig, list_experiments
from src.research.splits import chrono_split


DEFAULT_MARKETS = ["GOALS", "ASSISTS", "POINTS", "SOG", "BLOCKS"]
PROD_CALIB_MARKETS = {"ASSISTS", "POINTS"}


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


def _log_join_counts(con: duckdb.DuckDBPyConnection, market: str) -> None:
    base_cte = f"""
        WITH base AS (
            SELECT
                p.game_id,
                p.player_id,
                p.opp_team,
                concat(p.game_id::VARCHAR, '-', p.player_id::VARCHAR) AS row_id
            FROM fact_probabilities p
            JOIN fact_skater_game_all s
                ON p.game_id = s.game_id AND p.player_id = s.player_id
            WHERE p.market = '{market}'
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
    stages = [
        (
            "base rows",
            """
            SELECT COUNT(*) AS n_rows, COUNT(DISTINCT row_id) AS n_unique
            FROM base
            """,
        ),
        (
            "+ player features",
            """
            SELECT COUNT(*) AS n_rows, COUNT(DISTINCT row_id) AS n_unique
            FROM base b
            LEFT JOIN fact_player_game_features f
                ON b.game_id = f.game_id AND b.player_id = f.player_id
            """,
        ),
        (
            "+ team defense features",
            """
            SELECT COUNT(*) AS n_rows, COUNT(DISTINCT row_id) AS n_unique
            FROM base b
            LEFT JOIN fact_player_game_features f
                ON b.game_id = f.game_id AND b.player_id = f.player_id
            LEFT JOIN fact_team_defense_features td
                ON b.game_id = td.game_id AND b.opp_team = td.team
            """,
        ),
        (
            "+ goalie features",
            """
            SELECT COUNT(*) AS n_rows, COUNT(DISTINCT row_id) AS n_unique
            FROM base b
            LEFT JOIN fact_player_game_features f
                ON b.game_id = f.game_id AND b.player_id = f.player_id
            LEFT JOIN fact_team_defense_features td
                ON b.game_id = td.game_id AND b.opp_team = td.team
            LEFT JOIN goalie_dedup gf
                ON b.game_id = gf.game_id AND b.opp_team = gf.team AND gf.rn = 1
            """,
        ),
    ]
    print(f"[research dataset] Join row counts (market={market})")
    for label, sql in stages:
        n_rows, n_unique = con.execute(base_cte + sql).fetchone()
        print(f"  - {label}: rows={n_rows}, unique_row_id={n_unique}")


def _ensure_unique_rows(df: pd.DataFrame) -> None:
    if "row_id" not in df.columns:
        return
    dup_counts = df["row_id"].value_counts()
    dupes = dup_counts[dup_counts > 1]
    if dupes.empty:
        return
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = Path("outputs/research") / f"row_uniqueness_fail_{ts}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    dupes.reset_index().rename(columns={"index": "row_id", "row_id": "duplicate_count"}).to_csv(out_path, index=False)
    raise ValueError(f"Duplicate row_id detected. Details written to {out_path}.")


def _fetch_market_df(
    con: duckdb.DuckDBPyConnection,
    market: str,
    include_features: bool,
    cache_dir: str,
) -> pd.DataFrame:
    payload = {"market": market, "include_features": include_features, "goalie_dedup": "toi_seconds_max_v1"}
    cache_file = cache_path(cache_dir, payload, "pkl")
    cached = load_pickle(cache_file)
    if cached is not None:
        _ensure_unique_rows(cached)
        return cached

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

    if include_features:
        _log_join_counts(con, market)

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
    _ensure_unique_rows(df)
    save_pickle(cache_file, df)
    return df


def _target_count(df: pd.DataFrame, market: str) -> np.ndarray:
    if market == "GOALS":
        return df["goals"].values
    if market == "ASSISTS":
        return df["assists"].values
    if market == "POINTS":
        return df["points"].values
    if market == "SOG":
        return df["sog"].values
    if market == "BLOCKS":
        return df["blocks"].values
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
    df: pd.DataFrame,
    include_p_over: bool = False,
    include_mu: bool = False,
    include_home_or_away: bool = True,
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
    features = df.drop(
        columns=[
            *drop_cols,
        ],
        errors="ignore",
    )
    if not include_home_or_away and "home_or_away" in features.columns:
        features = features.drop(columns=["home_or_away"])
    cat_cols = [c for c in ["home_or_away", "position"] if c in features.columns]
    features = pd.get_dummies(features, columns=cat_cols, dummy_na=True)
    return features, list(features.columns)


def _production_prob(df: pd.DataFrame) -> np.ndarray:
    use_calib = (df["market"].isin(PROD_CALIB_MARKETS)) & (df["line"] == 1)
    p_calib = df["p_over_calibrated"].values
    p_raw = df["p_over"].values
    p_use = np.where(use_calib & ~pd.isna(p_calib), p_calib, p_raw)
    return p_use.astype(float)


def _feature_missingness_rates(df: pd.DataFrame, columns: List[str]) -> Dict[str, float]:
    return {c: float(df[c].isna().mean()) for c in columns if c in df.columns}


def _run_experiment(
    df: pd.DataFrame,
    exp: ExperimentConfig,
    seed: int,
    drop_home_or_away: bool,
    feature_parity_gate: bool,
    parity_threshold: float,
    monitoring_log: List[str],
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    lines = sorted(df["line"].unique())
    y_count = _target_count(df, exp.market)
    train_end, val_end = _split_dates(df)
    split = chrono_split(df, train_end, val_end)

    results = []
    bins_rows: List[Dict[str, object]] = []
    if exp.model_type == "baseline":
        for line in lines:
            y = _target_binary(split.test, line, exp.market)
            p = split.test["p_over"].values
            metrics = compute_metrics(y, p)
            results.append(
                {
                    "experiment_id": exp.experiment_id,
                    "market": exp.market,
                    "line": int(line),
                    **metrics.__dict__,
                }
            )
            bins_rows.extend(reliability_bins(y, p, 10, exp.experiment_id))
        return results, bins_rows

    if exp.model_type in {"poisson_mu", "negbin_mu", "zip_poisson", "zip_negbin", "hurdle_poisson", "hurdle_negbin", "comp_poisson_approx"}:
        mu = df["mu_used"].values
        alpha = 0.0
        dist = "poisson" if exp.market in {"GOALS", "ASSISTS", "POINTS"} else "negbin"
        if dist == "negbin":
            alpha = 0.35 if exp.market == "SOG" else 0.60
        if exp.model_type == "comp_poisson_approx":
            alpha = estimate_nb_alpha(y_count)
            dist = "negbin"
        if exp.model_type in {"zip_poisson", "zip_negbin"}:
            pi = estimate_zero_inflation_p(y_count, mu, dist, alpha)
        else:
            pi = 0.0
        if exp.model_type in {"hurdle_poisson", "hurdle_negbin"}:
            p0 = float(np.mean(y_count == 0))
        else:
            p0 = 0.0
        for line in lines:
            y = _target_binary(split.test, line, exp.market)
            if exp.model_type.startswith("zip"):
                p = np.array([zero_inflated_sf(int(line), m, pi, dist, alpha) for m in split.test["mu_used"].values])
            elif exp.model_type.startswith("hurdle"):
                p = np.array([hurdle_sf(int(line), m, p0, dist, alpha) for m in split.test["mu_used"].values])
            else:
                p = np.array(
                    [
                        poisson_sf(int(line), m) if dist == "poisson" else negbin_sf(int(line), m, alpha)
                        for m in split.test["mu_used"].values
                    ]
                )
            metrics = compute_metrics(y, p)
            results.append(
                {
                    "experiment_id": exp.experiment_id,
                    "market": exp.market,
                    "line": int(line),
                    **metrics.__dict__,
                }
            )
            bins_rows.extend(reliability_bins(y, p, 10, exp.experiment_id))
        return results, bins_rows

    if exp.model_type == "calibration":
        y_train = _target_binary(split.train, 1, exp.market)
        p_train = split.train["p_over"].values
        if len(np.unique(y_train)) < 2:
            calibrator = lambda p: p
        else:
            calibrator = _fit_calibrator(exp.calibration, y_train, p_train)
        for line in lines:
            y = _target_binary(split.test, line, exp.market)
            p = calibrator(split.test["p_over"].values)
            metrics = compute_metrics(y, p)
            results.append(
                {
                    "experiment_id": exp.experiment_id,
                    "market": exp.market,
                    "line": int(line),
                    **metrics.__dict__,
                }
            )
            bins_rows.extend(reliability_bins(y, p, 10, exp.experiment_id))
        return results, bins_rows

    if exp.model_type in {"logreg_features", "hgb_features"}:
        X_all, _ = _features_matrix(df, include_home_or_away=not drop_home_or_away)
        X_train = X_all.loc[split.train.index]
        X_test = X_all.loc[split.test.index]
        for line in lines:
            y_train = _target_binary(split.train, line, exp.market)
            y_test = _target_binary(split.test, line, exp.market)
            if len(np.unique(y_train)) < 2 or len(np.unique(y_test)) < 2:
                p = np.full_like(y_test, np.clip(y_train.mean(), 1e-6, 1 - 1e-6), dtype=float)
            else:
                if exp.model_type == "logreg_features":
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
                p = model.predict_proba(X_test)[:, 1]
            metrics = compute_metrics(y_test, p)
            results.append(
                {
                    "experiment_id": exp.experiment_id,
                    "market": exp.market,
                    "line": int(line),
                    **metrics.__dict__,
                }
            )
            bins_rows.extend(reliability_bins(y_test, p, 10, exp.experiment_id))
        return results, bins_rows

    if exp.model_type == "poisson_reg_features":
        X_all, _ = _features_matrix(df, include_home_or_away=not drop_home_or_away)
        y_count_train = _target_count(split.train, exp.market)
        y_count_test = _target_count(split.test, exp.market)
        model = Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler(with_mean=False)),
                    ("poisson", PoissonRegressor(alpha=1.0, max_iter=1000)),
            ]
        )
        model.fit(X_all.loc[split.train.index], y_count_train)
        mu = model.predict(X_all.loc[split.test.index])
        dist = "poisson" if exp.market in {"GOALS", "ASSISTS", "POINTS"} else "negbin"
        alpha = 0.35 if exp.market == "SOG" else 0.60
        for line in lines:
            y = (y_count_test >= line).astype(int)
            if dist == "poisson":
                p = np.array([poisson_sf(int(line), m) for m in mu])
            else:
                p = np.array([negbin_sf(int(line), m, alpha) for m in mu])
            metrics = compute_metrics(y, p)
            results.append(
                {
                    "experiment_id": exp.experiment_id,
                    "market": exp.market,
                    "line": int(line),
                    **metrics.__dict__,
                }
            )
            bins_rows.extend(reliability_bins(y, p, 10, exp.experiment_id))
        return results, bins_rows

    if exp.model_type == "direct_threshold_logreg":
        target_line = exp.line if exp.line is not None else 1
        X_all, _ = _features_matrix(df, include_home_or_away=not drop_home_or_away)
        y_train = _target_binary(split.train, target_line, exp.market)
        y_test = _target_binary(split.test, target_line, exp.market)
        if len(np.unique(y_train)) < 2 or len(np.unique(y_test)) < 2:
            p = np.full_like(y_test, np.clip(y_train.mean(), 1e-6, 1 - 1e-6), dtype=float)
        else:
            model = Pipeline(
                [
                    ("imputer", SimpleImputer(strategy="median")),
                    ("scaler", StandardScaler(with_mean=False)),
                    ("logit", LogisticRegression(max_iter=1000, random_state=seed)),
                ]
            )
            model.fit(X_all.loc[split.train.index], y_train)
            p = model.predict_proba(X_all.loc[split.test.index])[:, 1]
        metrics = compute_metrics(y_test, p)
        results.append(
            {
                "experiment_id": exp.experiment_id,
                "market": exp.market,
                "line": int(target_line),
                **metrics.__dict__,
            }
        )
        bins_rows.extend(reliability_bins(y_test, p, 10, exp.experiment_id))
        return results, bins_rows

    if exp.model_type in {"calib_logreg_features", "calib_hgb_features"}:
        X_all, _ = _features_matrix(
            df,
            include_p_over=True,
            include_mu=True,
            include_home_or_away=not drop_home_or_away,
        )
        X_train = X_all.loc[split.train.index]
        X_test = X_all.loc[split.test.index]
        base_feature_cols = [
            "p_over",
            "mu_used",
            "avg_toi_minutes_L10",
            "pp_toi_minutes_L20",
            "opp_sa60_L10_td",
            "opp_xga60_L10_td",
            "home_or_away",
            "position",
        ]
        if drop_home_or_away:
            base_feature_cols = [c for c in base_feature_cols if c != "home_or_away"]
        for line in lines:
            y_train = _target_binary(split.train, line, exp.market)
            y_test = _target_binary(split.test, line, exp.market)
            if len(np.unique(y_train)) < 2 or len(np.unique(y_test)) < 2:
                p = np.full_like(y_test, np.clip(y_train.mean(), 1e-6, 1 - 1e-6), dtype=float)
            else:
                if feature_parity_gate:
                    train_rates = _feature_missingness_rates(split.train, base_feature_cols)
                    test_rates = _feature_missingness_rates(split.test, base_feature_cols)
                    max_delta = 0.0
                    worst_feature = None
                    for key in train_rates:
                        delta = abs(train_rates.get(key, 0.0) - test_rates.get(key, 0.0))
                        if delta > max_delta:
                            max_delta = delta
                            worst_feature = key
                    if max_delta > parity_threshold:
                        p_prod = _production_prob(split.test)
                        p = p_prod
                        monitoring_log.append(
                            f"Feature parity gate tripped (exp={exp.experiment_id}, line={line}): "
                            f"max_delta={max_delta:.3f} on {worst_feature}. "
                            f"Train rates={train_rates}, test rates={test_rates}"
                        )
                        metrics = compute_metrics(y_test, p)
                        results.append(
                            {
                                "experiment_id": exp.experiment_id,
                                "market": exp.market,
                                "line": int(line),
                                **metrics.__dict__,
                            }
                        )
                        bins_rows.extend(reliability_bins(y_test, p, 10, exp.experiment_id))
                        continue
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
                p = model.predict_proba(X_test)[:, 1]
            metrics = compute_metrics(y_test, p)
            results.append(
                {
                    "experiment_id": exp.experiment_id,
                    "market": exp.market,
                    "line": int(line),
                    **metrics.__dict__,
                }
            )
            bins_rows.extend(reliability_bins(y_test, p, 10, exp.experiment_id))
        return results, bins_rows

    raise ValueError(f"Unknown experiment type {exp.model_type}")


def _write_report(out_dir: Path, metrics_df: pd.DataFrame, bins_df: pd.DataFrame, config: Dict[str, object]) -> None:
    report = out_dir / "report.md"
    baseline_summary = (
        metrics_df[metrics_df["experiment_id"].str.startswith("baseline__")]
        .groupby("market")
        .apply(lambda g: (g["logloss"] * g["n"]).sum() / g["n"].sum())
        .reset_index(name="baseline_logloss_w")
    )
    best_full = []
    for market in sorted(metrics_df["market"].unique()):
        base_lines = set(
            metrics_df[
                (metrics_df["market"] == market)
                & (metrics_df["experiment_id"] == f"baseline__{market}")
            ]["line"].unique()
        )
        exp_lines = metrics_df[metrics_df["market"] == market].groupby("experiment_id")["line"].apply(set)
        full_exps = exp_lines[exp_lines.apply(lambda s: s == base_lines)].index
        sub = metrics_df[(metrics_df["market"] == market) & (metrics_df["experiment_id"].isin(full_exps))]
        agg = (
            sub.groupby("experiment_id")
            .apply(lambda g: (g["logloss"] * g["n"]).sum() / g["n"].sum())
            .reset_index(name="logloss_w")
            .sort_values("logloss_w")
        )
        if agg.empty:
            continue
        best = agg.iloc[0]
        base_ll = float(
            agg[agg["experiment_id"] == f"baseline__{market}"]["logloss_w"].iloc[0]
        )
        best_full.append(
            {
                "market": market,
                "best_experiment": best["experiment_id"],
                "best_logloss_w": best["logloss_w"],
                "baseline_logloss_w": base_ll,
                "delta": base_ll - best["logloss_w"],
            }
        )
    best_full_df = pd.DataFrame(best_full)
    runtime_summary = (
        metrics_df.groupby("experiment_id")["runtime_s"]
        .mean()
        .reset_index()
        .sort_values("runtime_s", ascending=False)
        .head(10)
    )
    missing_feature_calib = []
    for market in sorted(metrics_df["market"].unique()):
        has_feat_calib = metrics_df[
            (metrics_df["market"] == market)
            & metrics_df["experiment_id"].str.contains("calib_logreg_features|calib_hgb_features", regex=True)
        ]
        if has_feat_calib.empty:
            missing_feature_calib.append(market)
    with report.open("w", encoding="utf-8") as f:
        f.write("# Research Backtest Report\n\n")
        f.write("## Config\n")
        f.write("```json\n")
        f.write(json.dumps(config, indent=2))
        f.write("\n```\n\n")
        f.write("## Baseline Summary (Weighted Log Loss)\n\n")
        f.write(baseline_summary.to_string(index=False))
        f.write("\n\n")
        f.write("## Top Improvements per Market (Full-Line Coverage)\n\n")
        if not best_full_df.empty:
            f.write(best_full_df.to_string(index=False))
        else:
            f.write("No full-coverage experiments found.\n")
        f.write("\n\n")
        f.write("## Ablation Notes\n\n")
        f.write("- COM-Poisson replaced with NB variance-matched approximation.\n")
        f.write("- Direct-threshold models only scored on a single line; excluded from full-line leaderboard.\n")
        f.write("- Calibration-with-features uses p_over + mu_used + team/goalie L10 context where available.\n")
        if missing_feature_calib:
            f.write(
                f"- Missing feature-calibration results for: {', '.join(missing_feature_calib)} (runtime pending).\n"
            )
        f.write("\n")
        f.write("## Leakage Checks\n\n")
        f.write("- Chronological split (70/15/15 by date) enforced; no shuffling.\n")
        f.write("- Feature columns restricted to rolling/season aggregates only.\n\n")
        f.write("## Runtime Notes (Top 10 Longest Experiments)\n\n")
        f.write(runtime_summary.to_string(index=False))
        f.write("\n\n")
        f.write("## Top Experiments by Market (Log Loss)\n\n")
        for market in DEFAULT_MARKETS:
            subset = metrics_df[metrics_df["market"] == market].sort_values(["logloss", "brier"])
            top = subset.head(5)
            f.write(f"### {market}\n\n")
            f.write(top.to_string(index=False))
            f.write("\n\n")


def _write_baseline_diagnostics(con: duckdb.DuckDBPyConnection, out_dir: Path) -> None:
    query_monthly = """
        WITH base AS (
            SELECT
                date_trunc('month', p.game_date) AS month,
                p.market,
                p.line,
                p.p_over,
                CASE
                    WHEN p.market = 'GOALS' THEN (s.goals >= p.line)
                    WHEN p.market = 'ASSISTS' THEN (s.assists >= p.line)
                    WHEN p.market = 'POINTS' THEN (s.points >= p.line)
                    WHEN p.market = 'SOG' THEN (s.sog >= p.line)
                    WHEN p.market = 'BLOCKS' THEN (s.blocks >= p.line)
                END::INTEGER AS y
            FROM fact_probabilities p
            JOIN fact_skater_game_all s
                ON p.game_id = s.game_id AND p.player_id = s.player_id
        )
        SELECT
            month,
            market,
            line,
            count(*) AS n,
            avg(power(y - p_over, 2)) AS brier,
            avg(-(y * ln(greatest(least(p_over, 1 - 1e-15), 1e-15)) + (1 - y) * ln(greatest(least(1 - p_over, 1 - 1e-15), 1e-15)))) AS logloss
        FROM base
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """
    monthly = con.execute(query_monthly).df()
    monthly.to_csv(out_dir / "baseline_monthly_metrics.csv", index=False)

    query_book = """
        WITH odds AS (
            SELECT DISTINCT
                date(event_start_time_utc) AS game_date,
                lower(trim(player_name_raw)) AS player_name,
                upper(trim(player_team)) AS player_team,
                market_type,
                cast(line AS INTEGER) AS line,
                book_type,
                source_vendor,
                coalesce(vendor_person_id, player_id_vendor) AS vendor_player_id
            FROM fact_prop_odds
            WHERE player_name_raw IS NOT NULL
        ),
        mapped AS (
            SELECT
                o.*,
                m.canonical_player_id
            FROM odds o
            LEFT JOIN dim_players_mapping m
                ON o.vendor_player_id = m.vendor_player_id
                AND o.source_vendor = m.source_vendor
        ),
        joined AS (
            SELECT
                o.book_type,
                p.market,
                p.line,
                p.p_over,
                CASE
                    WHEN p.market = 'GOALS' THEN (s.goals >= p.line)
                    WHEN p.market = 'ASSISTS' THEN (s.assists >= p.line)
                    WHEN p.market = 'POINTS' THEN (s.points >= p.line)
                    WHEN p.market = 'SOG' THEN (s.sog >= p.line)
                    WHEN p.market = 'BLOCKS' THEN (s.blocks >= p.line)
                END::INTEGER AS y
            FROM mapped o
            JOIN fact_probabilities p
                ON date(p.game_date) = o.game_date
                AND p.market = o.market_type
                AND p.line = o.line
                AND (
                    (o.canonical_player_id IS NOT NULL AND p.player_id = o.canonical_player_id)
                    OR (o.canonical_player_id IS NULL AND lower(trim(p.player_name)) = o.player_name)
                )
                AND (o.player_team IS NULL OR o.player_team = p.team)
            JOIN fact_skater_game_all s
                ON p.game_id = s.game_id AND p.player_id = s.player_id
        )
        SELECT
            book_type,
            market,
            line,
            count(*) AS n,
            avg(power(y - p_over, 2)) AS brier,
            avg(-(y * ln(greatest(least(p_over, 1 - 1e-15), 1e-15)) + (1 - y) * ln(greatest(least(1 - p_over, 1 - 1e-15), 1e-15)))) AS logloss
        FROM joined
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """
    book = con.execute(query_book).df()
    book.to_csv(out_dir / "baseline_book_type_metrics.csv", index=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb-path", default="data/db/nhl_backtest.duckdb")
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--cache-dir", default="outputs/research_cache")
    parser.add_argument("--markets", default=",".join(DEFAULT_MARKETS))
    parser.add_argument("--include-heavy", action="store_true")
    parser.add_argument("--only-baseline", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--experiment-filter", default=None)
    parser.add_argument("--drop-home-or-away", action="store_true")
    parser.add_argument("--feature-parity-gate", action="store_true")
    parser.add_argument("--feature-parity-threshold", type=float, default=0.05)
    args = parser.parse_args()

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir or f"outputs/research/{ts}")
    out_dir.mkdir(parents=True, exist_ok=True)

    markets = [m.strip().upper() for m in args.markets.split(",") if m.strip()]
    experiments = list_experiments(markets, include_heavy=args.include_heavy and not args.only_baseline)
    if args.only_baseline:
        experiments = [e for e in experiments if e.model_type == "baseline"]
    if args.experiment_filter:
        filters = [f.strip() for f in args.experiment_filter.split(",") if f.strip()]
        experiments = [
            e for e in experiments if any(f in e.experiment_id for f in filters)
        ]

    config = {
        "duckdb_path": args.duckdb_path,
        "markets": markets,
        "include_heavy": args.include_heavy,
        "only_baseline": args.only_baseline,
        "seed": args.seed,
        "experiment_filter": args.experiment_filter,
        "experiments": [e.__dict__ for e in experiments],
    }
    (out_dir / "config_used.json").write_text(json.dumps(config, indent=2), encoding="utf-8")

    metrics_path = out_dir / "metrics.csv"
    bins_path = out_dir / "bins.csv"
    leaderboard_path = out_dir / "leaderboard.csv"

    existing_ids = set()
    all_metrics: List[Dict[str, object]] = []
    all_bins: List[Dict[str, object]] = []
    if args.resume and metrics_path.exists():
        metrics_df = pd.read_csv(metrics_path)
        existing_ids = set(metrics_df["experiment_id"].unique())
        all_metrics.extend(metrics_df.to_dict(orient="records"))
    if args.resume and bins_path.exists():
        bins_df = pd.read_csv(bins_path)
        all_bins.extend(bins_df.to_dict(orient="records"))

    con = _init_con(args.duckdb_path)
    monitoring_log: List[str] = []
    for market in markets:
        include_features = any(e.market == market and e.uses_features for e in experiments)
        df = _fetch_market_df(con, market, include_features, args.cache_dir)
        for exp in [e for e in experiments if e.market == market]:
            if exp.experiment_id in existing_ids:
                continue
            start = time.time()
            rows, bins_rows = _run_experiment(
                df,
                exp,
                args.seed,
                drop_home_or_away=args.drop_home_or_away,
                feature_parity_gate=args.feature_parity_gate,
                parity_threshold=args.feature_parity_threshold,
                monitoring_log=monitoring_log,
            )
            runtime = time.time() - start
            for row in rows:
                row["runtime_s"] = runtime
                all_metrics.append(row)
            all_bins.extend(bins_rows)
        if all_metrics:
            pd.DataFrame(all_metrics).to_csv(metrics_path, index=False)
        if all_bins:
            pd.DataFrame(all_bins).to_csv(bins_path, index=False)
    con.close()

    if monitoring_log:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        monitor_path = Path("outputs/monitoring") / f"feature_parity_gate_{ts}.md"
        monitor_path.parent.mkdir(parents=True, exist_ok=True)
        with monitor_path.open("w", encoding="utf-8") as f:
            f.write("# Feature Parity Gate (Research)\n\n")
            f.write(f"- drop_home_or_away: `{args.drop_home_or_away}`\n")
            f.write(f"- threshold: `{args.feature_parity_threshold}`\n\n")
            for line in monitoring_log:
                f.write(f"- {line}\n")

    metrics_df = pd.read_csv(metrics_path)
    leaderboard = (
        metrics_df.sort_values(["market", "logloss", "brier"])
        .groupby("market")
        .head(10)
    )
    leaderboard.to_csv(leaderboard_path, index=False)

    bins_df = pd.read_csv(bins_path) if bins_path.exists() else pd.DataFrame()
    _write_report(out_dir, metrics_df, bins_df, config)

    con = _init_con(args.duckdb_path)
    _write_baseline_diagnostics(con, out_dir)
    con.close()


if __name__ == "__main__":
    main()

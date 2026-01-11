import argparse
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.research.eval_metrics import (
    bootstrap_logloss_delta,
    compute_metrics,
    reliability_bins,
)


MARKETS = ["GOALS", "ASSISTS", "POINTS", "SOG", "BLOCKS"]
PROD_CALIB_MARKETS = {"ASSISTS", "POINTS"}


def _init_con(db_path: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db_path)
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


def _split_dates(dates: np.ndarray) -> Tuple[pd.Timestamp, pd.Timestamp]:
    dates = np.sort(pd.to_datetime(dates))
    if len(dates) < 10:
        mid = dates[len(dates) // 2]
        return pd.Timestamp(mid), pd.Timestamp(dates[-2])
    train_end = dates[int(0.7 * len(dates)) - 1]
    val_end = dates[int(0.85 * len(dates)) - 1]
    return pd.Timestamp(train_end), pd.Timestamp(val_end)


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


def _features_matrix(
    df: pd.DataFrame,
    include_p_over: bool = False,
    include_mu: bool = False,
    include_home_or_away: bool = True,
) -> pd.DataFrame:
    drop_cols = [
        "game_id",
        "row_id",
        "game_date",
        "season",
        "market",
        "line",
        "player_id",
        "player_name",
        "team",
        "opp_team",
        "split",
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
    # Drop any previously added model-prob columns to avoid leakage.
    extra_drop = [c for c in features.columns if c.startswith("p_calib_") or c.startswith("p_")]
    if include_p_over and "p_over" in extra_drop:
        extra_drop.remove("p_over")
    if include_mu and "mu_used" in extra_drop:
        extra_drop.remove("mu_used")
    features = features.drop(columns=extra_drop, errors="ignore")
    if not include_home_or_away and "home_or_away" in features.columns:
        features = features.drop(columns=["home_or_away"])
    cat_cols = [c for c in ["home_or_away", "position"] if c in features.columns]
    features = pd.get_dummies(features, columns=cat_cols, dummy_na=True)
    return features


def _production_prob(df: pd.DataFrame) -> np.ndarray:
    use_calib = (df["market"].isin(PROD_CALIB_MARKETS)) & (df["line"] == 1)
    p_calib = df["p_over_calibrated"].values
    p_raw = df["p_over"].values
    p_use = np.where(use_calib & ~pd.isna(p_calib), p_calib, p_raw)
    return p_use.astype(float)


def _fetch_eval_df(
    con: duckdb.DuckDBPyConnection,
    date_min: Optional[str],
    date_max: Optional[str],
    markets: Optional[List[str]] = None,
) -> pd.DataFrame:
    feature_cols = _safe_feature_columns(con)
    feature_select = ""
    if feature_cols:
        feature_select = ", " + ", ".join([f"f.{c}" for c in feature_cols])
    extra_features = """
        , td.opp_sa60_L10 AS opp_sa60_L10_td
        , td.opp_xga60_L10 AS opp_xga60_L10_td
        , td.opp_goals_against_L10 AS opp_goals_against_L10
        , td.opp_goals_against_per_game_L10_raw AS opp_ga_per_game_L10
        , gf.goalie_gsax60_L10 AS opp_goalie_gsax60_L10
    """
    date_filter = ""
    if date_min:
        date_filter += f" AND p.game_date::DATE >= DATE '{date_min}'"
    if date_max:
        date_filter += f" AND p.game_date::DATE <= DATE '{date_max}'"
    use_markets = markets or MARKETS
    markets_sql = ", ".join([f"'{m}'" for m in use_markets])

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
            p.player_id,
            p.player_name,
            p.game_date::DATE AS game_date,
            s.season,
            p.market,
            p.line,
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
        WHERE p.market IN ({markets_sql})
        {date_filter}
    """
    df = con.execute(query).df()
    if df.empty:
        return df
    df["row_id"] = (
        df["game_id"].astype(str)
        + "-"
        + df["player_id"].astype(str)
        + "-"
        + df["market"].astype(str)
        + "-"
        + df["line"].astype(str)
    )
    return df


def _train_feature_calibrator(
    X_train: pd.DataFrame,
    y_train: np.ndarray,
    model_type: str,
    seed: int,
):
    if model_type == "calib_logreg_features":
        return Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler(with_mean=False)),
                ("logit", LogisticRegression(max_iter=1000, random_state=seed)),
            ]
        )
    if model_type == "calib_hgb_features":
        return HistGradientBoostingClassifier(random_state=seed, max_depth=6)
    raise ValueError(f"Unknown model_type {model_type}")


def _feature_missingness_rates(df: pd.DataFrame, columns: List[str]) -> Dict[str, float]:
    return {c: float(df[c].isna().mean()) for c in columns if c in df.columns}


def _predict_feature_calibrators(
    df_market: pd.DataFrame,
    train_end: pd.Timestamp,
    val_end: pd.Timestamp,
    seed: int,
    verbose: bool = False,
    include_home_or_away: bool = True,
    feature_parity_gate: bool = False,
    parity_threshold: float = 0.05,
    monitoring_log: Optional[List[str]] = None,
) -> Dict[str, np.ndarray]:
    df_market = df_market.sort_values("game_date").copy()
    X_all = _features_matrix(
        df_market,
        include_p_over=True,
        include_mu=True,
        include_home_or_away=include_home_or_away,
    )
    pred_logreg = np.full(len(df_market), np.nan, dtype=float)
    pred_hgb = np.full(len(df_market), np.nan, dtype=float)

    train_mask = df_market["game_date"] <= train_end
    test_mask = df_market["game_date"] > val_end
    lines = sorted(df_market["line"].unique())
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
    if not include_home_or_away:
        base_feature_cols = [c for c in base_feature_cols if c != "home_or_away"]

    for line in lines:
        if verbose:
            print(f"  - calibrator line={line} (train_end={train_end.date()}, val_end={val_end.date()})", flush=True)
        line_mask = df_market["line"] == line
        idx_train = train_mask & line_mask
        idx_test = test_mask & line_mask
        if not idx_test.any():
            continue
        y_train = _target_binary(df_market[idx_train], int(line), df_market["market"].iloc[0])
        y_test = _target_binary(df_market[idx_test], int(line), df_market["market"].iloc[0])
        if len(y_train) == 0:
            # No train data: fallback to raw probabilities to avoid leakage.
            pred_logreg[idx_test] = df_market.loc[idx_test, "p_over"].values
            pred_hgb[idx_test] = df_market.loc[idx_test, "p_over"].values
            continue

        if feature_parity_gate:
            train_rates = _feature_missingness_rates(df_market.loc[idx_train], base_feature_cols)
            test_rates = _feature_missingness_rates(df_market.loc[idx_test], base_feature_cols)
            max_delta = 0.0
            worst_feature = None
            for key in train_rates:
                delta = abs(train_rates.get(key, 0.0) - test_rates.get(key, 0.0))
                if delta > max_delta:
                    max_delta = delta
                    worst_feature = key
            if monitoring_log is not None:
                monitoring_log.append(
                    f"Feature parity stats (market={df_market['market'].iloc[0]}, line={line}): "
                    f"max_delta={max_delta:.3f} on {worst_feature}. "
                    f"Train rates={train_rates}, test rates={test_rates}"
                )
            if max_delta > parity_threshold:
                fallback = df_market.loc[idx_test, "p_production"].values
                pred_logreg[idx_test] = fallback
                pred_hgb[idx_test] = fallback
                continue

        base_prob = float(np.clip(y_train.mean(), 1e-6, 1 - 1e-6))
        if len(np.unique(y_train)) < 2:
            pred_logreg[idx_test] = base_prob
            pred_hgb[idx_test] = base_prob
            continue

        X_train = X_all.loc[idx_train]
        X_test = X_all.loc[idx_test]

        logreg = _train_feature_calibrator(X_train, y_train, "calib_logreg_features", seed)
        logreg.fit(X_train, y_train)
        pred_logreg[idx_test] = logreg.predict_proba(X_test)[:, 1]

        hgb = _train_feature_calibrator(X_train, y_train, "calib_hgb_features", seed)
        hgb.fit(X_train, y_train)
        pred_hgb[idx_test] = hgb.predict_proba(X_test)[:, 1]

        # Guard against degenerate outputs on test with single class.
        if len(np.unique(y_test)) < 2:
            pred_logreg[idx_test] = np.clip(pred_logreg[idx_test], 1e-6, 1 - 1e-6)
            pred_hgb[idx_test] = np.clip(pred_hgb[idx_test], 1e-6, 1 - 1e-6)

    return {
        "calib_logreg_features": pred_logreg,
        "calib_hgb_features": pred_hgb,
    }


def _segment_metrics(
    df_long: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    bins_rows: List[Dict[str, object]] = []
    for (variant, market, line, side), group in df_long.groupby(
        ["variant", "market", "line", "side"]
    ):
        y_true = group["y"].values
        p_hat = group["p_hat"].values
        metrics = compute_metrics(y_true, p_hat)
        tail_rate = float(np.mean(p_hat > 0.5)) if len(p_hat) else float("nan")
        rows.append(
            {
                "variant": variant,
                "market": market,
                "line": line,
                "side": side,
                "n": metrics.n,
                "logloss": metrics.logloss,
                "brier": metrics.brier,
                "ece10": metrics.ece10,
                "ece20": metrics.ece20,
                "tail_rate_gt_0_5": tail_rate,
            }
        )
        bins = reliability_bins(y_true, p_hat, 10, variant)
        for b in bins:
            b.update(
                {
                    "market": market,
                    "line": line,
                    "side": side,
                }
            )
        bins_rows.extend(bins)
    metrics_df = pd.DataFrame(rows)
    bins_df = pd.DataFrame(bins_rows)
    if not bins_df.empty:
        bins_df = bins_df.rename(columns={"bin": "bin_id", "count": "n"})
        bins_df = bins_df[
            ["variant", "market", "line", "side", "bin_id", "bin_lo", "bin_hi", "n", "avg_p", "actual_rate"]
        ]
    return metrics_df, bins_df


def _pairwise_deltas(
    df_wide: pd.DataFrame,
    variants: List[str],
    baseline: str,
    n_boot: int,
    seed: int,
) -> pd.DataFrame:
    rows = []
    for (market, line, side), group in df_wide.groupby(["market", "line", "side"]):
        y_true = group["y"].values
        p_base = group[f"p_{baseline}"].values
        for variant in variants:
            if variant == baseline:
                continue
            p_var = group[f"p_{variant}"].values
            delta_mean, ci_low, ci_high = bootstrap_logloss_delta(
                y_true, p_base, p_var, n_boot=n_boot, seed=seed
            )
            rows.append(
                {
                    "variant": variant,
                    "baseline_variant": baseline,
                    "market": market,
                    "line": line,
                    "side": side,
                    "delta_logloss": delta_mean,
                    "ci_low": ci_low,
                    "ci_high": ci_high,
                    "n": int(len(y_true)),
                }
            )
    return pd.DataFrame(rows)


def _init_overall_accumulators(variants: List[str]) -> Dict[str, Dict[str, object]]:
    acc = {}
    for variant in variants:
        acc[variant] = {
            "n": 0,
            "logloss_sum": 0.0,
            "brier_sum": 0.0,
            "bin_sums10": np.zeros(10, dtype=float),
            "bin_true10": np.zeros(10, dtype=float),
            "bin_total10": np.zeros(10, dtype=float),
            "bin_sums20": np.zeros(20, dtype=float),
            "bin_true20": np.zeros(20, dtype=float),
            "bin_total20": np.zeros(20, dtype=float),
        }
    return acc


def _update_overall_accumulators(
    acc: Dict[str, Dict[str, object]],
    variant: str,
    y: np.ndarray,
    p: np.ndarray,
) -> None:
    eps = 1e-15
    p_clamped = np.clip(p, eps, 1 - eps)
    acc[variant]["n"] += int(len(y))
    acc[variant]["logloss_sum"] += float(
        -(y * np.log(p_clamped) + (1 - y) * np.log(1 - p_clamped)).sum()
    )
    acc[variant]["brier_sum"] += float(np.square(y - p).sum())

    bins10 = np.linspace(0.0, 1.0 + 1e-8, 11)
    ids10 = np.digitize(p, bins10) - 1
    acc[variant]["bin_sums10"] += np.bincount(ids10, weights=p, minlength=10)
    acc[variant]["bin_true10"] += np.bincount(ids10, weights=y, minlength=10)
    acc[variant]["bin_total10"] += np.bincount(ids10, minlength=10)

    bins20 = np.linspace(0.0, 1.0 + 1e-8, 21)
    ids20 = np.digitize(p, bins20) - 1
    acc[variant]["bin_sums20"] += np.bincount(ids20, weights=p, minlength=20)
    acc[variant]["bin_true20"] += np.bincount(ids20, weights=y, minlength=20)
    acc[variant]["bin_total20"] += np.bincount(ids20, minlength=20)


def _tail_shift_line_half(
    df_test: pd.DataFrame,
    variant_cols: Dict[str, str],
) -> pd.DataFrame:
    rows = []
    sub = df_test[df_test["line"] == 1].copy()
    for market in MARKETS:
        m = sub[sub["market"] == market]
        if m.empty:
            continue
        base = m[variant_cols["production"]].values
        base_tail = float(np.mean(base > 0.5))
        rows.append(
            {
                "market": market,
                "variant": "production",
                "n": int(len(m)),
                "tail_rate_gt_0_5": base_tail,
                "tail_shift_vs_production": 0.0,
            }
        )
        for variant, col in variant_cols.items():
            if variant == "production":
                continue
            probs = m[col].values
            tail = float(np.mean(probs > 0.5))
            rows.append(
                {
                    "market": market,
                    "variant": variant,
                    "n": int(len(m)),
                    "tail_rate_gt_0_5": tail,
                    "tail_shift_vs_production": tail - base_tail,
                }
            )
    return pd.DataFrame(rows)


def _top200_ev_subset(
    con: duckdb.DuckDBPyConnection,
    date_min: Optional[str],
    date_max: Optional[str],
) -> pd.DataFrame:
    date_filter = ""
    if date_min:
        date_filter += f" AND o.game_date >= DATE '{date_min}'"
    if date_max:
        date_filter += f" AND o.game_date <= DATE '{date_max}'"
    markets_sql = ", ".join([f"'{m}'" for m in MARKETS])
    query = f"""
        SELECT
            o.game_date,
            o.player_id,
            o.player_name,
            o.market,
            o.line AS odds_line,
            o.side,
            o.odds_decimal,
            p.line AS prob_line,
            p.p_over,
            p.p_over_calibrated
        FROM fact_odds_props o
        JOIN fact_probabilities p
            ON o.player_id = p.player_id
            AND o.game_date = CAST(p.game_date AS DATE)
            AND o.market = p.market
            AND CAST(FLOOR(o.line) + 1 AS BIGINT) = p.line
        WHERE o.market IN ({markets_sql})
          AND lower(o.side) IN ('over', 'under')
          AND o.odds_decimal IS NOT NULL
          {date_filter}
    """
    df = con.execute(query).df()
    if df.empty:
        return df
    use_calib = (df["market"].isin(PROD_CALIB_MARKETS)) & (df["prob_line"] == 1)
    p_prod = np.where(use_calib & df["p_over_calibrated"].notna(), df["p_over_calibrated"], df["p_over"])
    model_prob = np.where(df["side"].str.upper() == "OVER", p_prod, 1 - p_prod)
    df["model_prob"] = model_prob
    df["ev"] = (df["model_prob"] * df["odds_decimal"]) - 1.0
    df = df.sort_values("ev", ascending=False).head(200).copy()
    df["line"] = df["prob_line"].astype(int)
    return df


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default="data/db/nhl_backtest.duckdb")
    parser.add_argument("--outdir", default="outputs/backtesting")
    parser.add_argument("--date-min", default=None)
    parser.add_argument("--date-max", default=None)
    parser.add_argument("--markets", default=",".join(MARKETS))
    parser.add_argument("--variants", default=None)
    parser.add_argument("--bootstrap-n", type=int, default=300)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--drop-home-or-away", action="store_true")
    parser.add_argument("--feature-parity-gate", action="store_true")
    parser.add_argument("--feature-parity-threshold", type=float, default=0.05)
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    con = _init_con(args.db_path)
    monitoring_notes: List[str] = []

    out_metrics = os.path.join(args.outdir, "model_compare_metrics_by_segment.csv")
    out_pairwise = os.path.join(args.outdir, "model_compare_pairwise_deltas.csv")
    out_bins = os.path.join(args.outdir, "model_compare_reliability_bins.parquet")
    out_md = os.path.join(args.outdir, "model_compare_summary.md")
    out_line1 = os.path.join(args.outdir, "model_compare_line1_predictions.parquet")

    variants = ["production", "raw", "calib_hgb_features", "calib_logreg_features"]
    if args.variants:
        allowed = {v.strip() for v in args.variants.split(",") if v.strip()}
        variants = [v for v in variants if v in allowed]

    markets = [m.strip().upper() for m in args.markets.split(",") if m.strip()]

    per_market_splits: Dict[str, Dict[str, pd.Timestamp]] = {}
    metrics_parts: List[pd.DataFrame] = []
    bins_parts: List[pd.DataFrame] = []
    pairwise_parts: List[pd.DataFrame] = []
    tail_parts: List[pd.DataFrame] = []
    market_summary_rows: List[Dict[str, object]] = []
    overall_acc = _init_overall_accumulators(variants)
    test_join_parts: List[pd.DataFrame] = []

    for market in markets:
        if args.verbose:
            print(f"[market] {market} fetch starting...", flush=True)
        df_market = _fetch_eval_df(con, args.date_min, args.date_max, markets=[market])
        if df_market.empty:
            if args.verbose:
                print(f"[market] {market} no rows.", flush=True)
            continue
        if args.verbose:
            print(f"[market] {market} rows={len(df_market):,}", flush=True)
        df_market["game_date"] = pd.to_datetime(df_market["game_date"])
        df_market = df_market.sort_values("game_date").reset_index(drop=True)

        train_end, val_end = _split_dates(df_market["game_date"].unique())
        per_market_splits[market] = {"train_end": train_end, "val_end": val_end}
        df_market["split"] = "train"
        df_market.loc[df_market["game_date"] > train_end, "split"] = "val"
        df_market.loc[df_market["game_date"] > val_end, "split"] = "test"

        df_market["p_production"] = _production_prob(df_market)
        df_market["p_raw"] = df_market["p_over"].values.astype(float)

        pred_cols = {
            "production": "p_production",
            "raw": "p_raw",
        }

        if any(v in variants for v in ["calib_hgb_features", "calib_logreg_features"]):
            if args.verbose:
                print(f"[market] {market} training feature calibrators...", flush=True)
            preds = _predict_feature_calibrators(
                df_market,
                train_end,
                val_end,
                args.seed,
                verbose=args.verbose,
                include_home_or_away=not args.drop_home_or_away,
                feature_parity_gate=args.feature_parity_gate,
                parity_threshold=args.feature_parity_threshold,
                monitoring_log=monitoring_notes,
            )
            for variant, pred in preds.items():
                df_market[f"p_{variant}"] = pred

        df_test = df_market[df_market["split"] == "test"].copy()
        if df_test.empty:
            if args.verbose:
                print(f"[market] {market} no test rows after split.", flush=True)
            continue

        for variant in ["calib_hgb_features", "calib_logreg_features"]:
            if variant not in variants:
                continue
            col = f"p_{variant}"
            if col not in df_test.columns:
                df_test[col] = df_test["p_raw"].values
                continue
            missing = df_test[col].isna()
            if missing.any():
                df_test.loc[missing, col] = df_test.loc[missing, "p_raw"]

        for variant in variants:
            pred_cols[variant] = f"p_{variant}"

        count_col = {
            "GOALS": "goals",
            "ASSISTS": "assists",
            "POINTS": "points",
            "SOG": "sog",
            "BLOCKS": "blocks",
        }[market]
        df_test["y_over"] = (df_test[count_col].values >= df_test["line"].values).astype(int)

        df_wide_over = df_test[["market", "line"]].copy()
        df_wide_over["side"] = "OVER"
        df_wide_over["y"] = df_test["y_over"].values
        for variant in variants:
            df_wide_over[f"p_{variant}"] = df_test[pred_cols[variant]].values

        df_wide_under = df_test[["market", "line"]].copy()
        df_wide_under["side"] = "UNDER"
        df_wide_under["y"] = 1 - df_test["y_over"].values
        for variant in variants:
            df_wide_under[f"p_{variant}"] = 1 - df_test[pred_cols[variant]].values

        df_wide = pd.concat([df_wide_over, df_wide_under], ignore_index=True)

        df_long_parts = []
        for variant in variants:
            tmp = df_wide[["market", "line", "side", "y"]].copy()
            tmp["variant"] = variant
            tmp["p_hat"] = df_wide[f"p_{variant}"].values
            df_long_parts.append(tmp)
        df_long = pd.concat(df_long_parts, ignore_index=True)

        metrics_df_m, bins_df_m = _segment_metrics(df_long)
        metrics_parts.append(metrics_df_m)
        if not bins_df_m.empty:
            bins_parts.append(bins_df_m)

        pairwise_parts.append(
            _pairwise_deltas(
                df_wide,
                variants=variants,
                baseline="production",
                n_boot=args.bootstrap_n,
                seed=args.seed,
            )
        )

        tail_parts.append(_tail_shift_line_half(df_test, pred_cols))

        for variant in variants:
            g = df_long[df_long["variant"] == variant]
            metrics = compute_metrics(g["y"].values, g["p_hat"].values)
            market_summary_rows.append(
                {
                    "variant": variant,
                    "market": market,
                    "n": metrics.n,
                    "logloss": metrics.logloss,
                    "brier": metrics.brier,
                    "ece10": metrics.ece10,
                    "ece20": metrics.ece20,
                }
            )

        for variant in variants:
            _update_overall_accumulators(
                overall_acc,
                variant,
                df_wide["y"].values,
                df_wide[f"p_{variant}"].values,
            )

        join_cols = [
            "game_date",
            "player_id",
            "market",
            "line",
            "player_name",
            *[pred_cols[v] for v in variants],
        ]
        test_join_parts.append(df_test[df_test["line"] == 1][join_cols].copy())
        if args.verbose:
            print(f"[market] {market} done.", flush=True)

    if not metrics_parts:
        print("No evaluation data found.")
        con.close()
        return

    metrics_df = pd.concat(metrics_parts, ignore_index=True)
    bins_df = pd.concat(bins_parts, ignore_index=True) if bins_parts else pd.DataFrame()
    pairwise_df = pd.concat(pairwise_parts, ignore_index=True) if pairwise_parts else pd.DataFrame()
    tail_df = pd.concat(tail_parts, ignore_index=True) if tail_parts else pd.DataFrame()
    df_test_join = pd.concat(test_join_parts, ignore_index=True) if test_join_parts else pd.DataFrame()
    if not df_test_join.empty:
        df_test_join.to_parquet(out_line1, index=False)

    pred_cols_all = {variant: f"p_{variant}" for variant in variants}
    top200_df = _top200_ev_subset(con, args.date_min, args.date_max)
    con.close()

    top200_tail = pd.DataFrame()
    if not top200_df.empty and not df_test_join.empty:
        top200_join = top200_df.merge(
            df_test_join[
                [
                    "game_date",
                    "player_id",
                    "market",
                    "line",
                    "player_name",
                    *[pred_cols_all[v] for v in variants],
                ]
            ],
            left_on=["game_date", "player_id", "market", "line"],
            right_on=["game_date", "player_id", "market", "line"],
            how="left",
        )
        top200_line = top200_join[top200_join["line"] == 1].copy()
        rows = []
        for market in MARKETS:
            sub = top200_line[top200_line["market"] == market]
            if sub.empty:
                continue
            base = sub[pred_cols_all["production"]]
            base_tail = float(np.mean(base > 0.5))
            rows.append(
                {
                    "market": market,
                    "variant": "production",
                    "n": int(len(sub)),
                    "tail_rate_gt_0_5": base_tail,
                    "tail_shift_vs_production": 0.0,
                }
            )
            for variant in variants:
                if variant == "production":
                    continue
                probs = sub[pred_cols_all[variant]]
                tail = float(np.mean(probs > 0.5))
                rows.append(
                    {
                        "market": market,
                        "variant": variant,
                        "n": int(len(sub)),
                        "tail_rate_gt_0_5": tail,
                        "tail_shift_vs_production": tail - base_tail,
                    }
                )
        top200_tail = pd.DataFrame(rows)

    metrics_df.to_csv(out_metrics, index=False)
    pairwise_df.to_csv(out_pairwise, index=False)
    if not bins_df.empty:
        bins_df.to_parquet(out_bins, index=False)

    # Guardrails: tail shift > +0.10 or near-1.0 in line 0.5
    guardrail_rows = []
    tail_flags = []
    if not tail_df.empty and not df_test_join.empty:
        for market in ["GOALS", "ASSISTS", "POINTS"]:
            base = tail_df[(tail_df["market"] == market) & (tail_df["variant"] == "production")]
            if base.empty:
                continue
            base_rate = float(base["tail_rate_gt_0_5"].iloc[0])
            for variant in variants:
                if variant == "production":
                    continue
                row = tail_df[(tail_df["market"] == market) & (tail_df["variant"] == variant)]
                if row.empty:
                    continue
                rate = float(row["tail_rate_gt_0_5"].iloc[0])
                shift = rate - base_rate
                if shift > 0.10 or rate >= 0.98:
                    tail_flags.append((market, variant, rate, shift))

        if tail_flags:
            flagged_variants = sorted({v for _, v, _, _ in tail_flags})
            for variant in flagged_variants:
                sub = df_test_join[(df_test_join["line"] == 1)]
                if sub.empty:
                    continue
                probs = sub[pred_cols_all[variant]]
                quantiles = probs.quantile([0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0])
                quantiles = quantiles.to_dict()
                guardrail_rows.append(
                    {
                        "variant": variant,
                        "quantiles": quantiles,
                    }
                )
            top_players = (
                df_test_join[df_test_join["line"] == 1][
                    ["player_name", "market", *[pred_cols_all[v] for v in variants]]
                ]
                .copy()
            )
            for market, variant, rate, shift in tail_flags:
                sub = top_players[top_players["market"] == market].copy()
                sub = sub[sub[pred_cols_all[variant]] > 0.5]
                player_stats = (
                    sub.groupby("player_name")[pred_cols_all[variant]]
                    .agg(["count", "mean"])
                    .reset_index()
                    .sort_values(["count", "mean"], ascending=False)
                    .head(15)
                )
                guardrail_rows.append(
                    {
                        "variant": variant,
                        "market": market,
                        "tail_rate": rate,
                        "tail_shift": shift,
                        "top_players": player_stats,
                    }
                )

    overall_rows = []
    for variant in variants:
        n = overall_acc[variant]["n"]
        if n == 0:
            continue
        logloss = overall_acc[variant]["logloss_sum"] / n
        brier = overall_acc[variant]["brier_sum"] / n
        bin_total10 = overall_acc[variant]["bin_total10"]
        bin_total20 = overall_acc[variant]["bin_total20"]
        ece10 = 0.0
        ece20 = 0.0
        if bin_total10.sum() > 0:
            avg_p10 = overall_acc[variant]["bin_sums10"] / np.where(bin_total10 == 0, 1, bin_total10)
            avg_y10 = overall_acc[variant]["bin_true10"] / np.where(bin_total10 == 0, 1, bin_total10)
            ece10 = float(np.sum(np.abs(avg_y10 - avg_p10) * bin_total10) / bin_total10.sum())
        if bin_total20.sum() > 0:
            avg_p20 = overall_acc[variant]["bin_sums20"] / np.where(bin_total20 == 0, 1, bin_total20)
            avg_y20 = overall_acc[variant]["bin_true20"] / np.where(bin_total20 == 0, 1, bin_total20)
            ece20 = float(np.sum(np.abs(avg_y20 - avg_p20) * bin_total20) / bin_total20.sum())
        overall_rows.append(
            {
                "variant": variant,
                "n": n,
                "logloss": float(logloss),
                "brier": float(brier),
                "ece10": float(ece10),
                "ece20": float(ece20),
            }
        )
    overall_summary = pd.DataFrame(overall_rows)

    market_summary = pd.DataFrame(market_summary_rows)

    # Conclusions: wins/losses vs production by segment
    wins = {}
    losses = {}
    for variant in variants:
        if variant == "production":
            continue
        sub = pairwise_df[pairwise_df["variant"] == variant]
        wins[variant] = int((sub["ci_high"] < 0).sum())
        losses[variant] = int((sub["ci_low"] > 0).sum())

    with open(out_md, "w", encoding="utf-8") as f:
        f.write("# Model Variant Backtest (Accuracy)\n\n")
        f.write(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n")
        f.write("## Data Sources + Join Logic\n")
        f.write(
            "- `fact_probabilities` + `fact_skater_game_all` joined on `(game_id, player_id)` for outcomes.\n"
        )
        f.write(
            "- `fact_player_game_features`, `fact_team_defense_features`, and deduped `fact_goalie_features` "
            "left-joined on `(game_id, player_id)` / `(game_id, opp_team)` for feature calibrators.\n"
        )
        f.write(
            "- Odds subset (top200 EV) uses `fact_odds_props` joined to probabilities on "
            "`(player_id, game_date, market, line=floor(odds_line)+1)`.\n\n"
        )

        f.write("## Variant Definitions\n")
        f.write("- **production**: uses calibrated probabilities for ASSISTS/POINTS line=1, raw elsewhere.\n")
        f.write("- **raw**: uses `p_over` for all markets/lines.\n")
        f.write("- **calib_hgb_features**: feature-based calibrator (HGB) trained chronologically.\n")
        f.write("- **calib_logreg_features**: feature-based calibrator (logistic) trained chronologically.\n\n")

        f.write("## Chronological Split\n")
        for market, splits in per_market_splits.items():
            f.write(
                f"- {market}: train_end={splits['train_end'].date()}, "
                f"val_end={splits['val_end'].date()} (test > val_end)\n"
            )
        f.write("\n")

        f.write("## Overall Metrics (All Markets/Lines/Sides)\n")
        f.write(overall_summary.to_markdown(index=False))
        f.write("\n\n")

        f.write("## Metrics by Market (All Lines/Sides)\n")
        f.write(market_summary.to_markdown(index=False))
        f.write("\n\n")

        f.write("## Metrics by Market/Line/Side\n")
        f.write(metrics_df.to_markdown(index=False))
        f.write("\n\n")

        f.write("## Pairwise Delta Log Loss vs Production (Bootstrap CI)\n")
        f.write(pairwise_df.to_markdown(index=False))
        f.write("\n\n")

        f.write("## Calibration Error (ECE)\n")
        ece_table = metrics_df[["variant", "market", "line", "side", "ece10", "ece20", "n"]]
        f.write(ece_table.to_markdown(index=False))
        f.write("\n\n")

        f.write("## Tail-Shift Diagnostics (Line 0.5 == line=1)\n")
        if not tail_df.empty:
            f.write(tail_df.to_markdown(index=False))
            f.write("\n\n")
        else:
            f.write("No line=1 rows available for tail-shift diagnostics.\n\n")

        f.write("## Tail-Shift Diagnostics (Top200 EV, Line 0.5)\n")
        if not top200_tail.empty:
            f.write(top200_tail.to_markdown(index=False))
            f.write("\n\n")
        else:
            f.write("Top200 EV subset could not be reconstructed from available odds data.\n\n")

        if tail_flags:
            f.write("## Guardrail Flags\n")
            for market, variant, rate, shift in tail_flags:
                f.write(
                    f"- **Likely misapplied calibration**: {variant} @ {market} "
                    f"(tail_rate={rate:.3f}, shift_vs_prod={shift:.3f}).\n"
                )
            f.write("\n")
            f.write("### Guardrail Diagnostics\n")
            for row in guardrail_rows:
                if "quantiles" in row:
                    f.write(f"- {row['variant']} quantiles: {row['quantiles']}\n")
                if "top_players" in row:
                    f.write(f"\n**Top Players (>{0.5} prob) for {row['variant']} {row['market']}**\n\n")
                    f.write(row["top_players"].to_markdown(index=False))
                    f.write("\n\n")

        f.write("## Conclusions\n")
        for variant in variants:
            if variant == "production":
                continue
            f.write(
                f"- {variant}: wins={wins.get(variant, 0)} segments, "
                f"losses={losses.get(variant, 0)} segments vs production (95% bootstrap CI).\n"
            )
        f.write("\n")
        f.write("Accuracy-only evaluation; ROI and betting selection logic were not modified.\n")

    print(f"Wrote metrics to {out_metrics}")
    print(f"Wrote pairwise deltas to {out_pairwise}")
    print(f"Wrote reliability bins to {out_bins}")
    print(f"Wrote report to {out_md}")
    if not df_test_join.empty:
        print(f"Wrote line=1 predictions to {out_line1}")

    if monitoring_notes:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        monitor_path = os.path.join("outputs", "monitoring", f"feature_parity_gate_{ts}.md")
        os.makedirs(os.path.dirname(monitor_path), exist_ok=True)
        with open(monitor_path, "w", encoding="utf-8") as f:
            f.write("# Feature Parity Gate (Research)\n\n")
            f.write(f"- drop_home_or_away: `{args.drop_home_or_away}`\n")
            f.write(f"- threshold: `{args.feature_parity_threshold}`\n\n")
            for line in monitoring_notes:
                f.write(f"- {line}\n")
        print(f"Wrote monitoring log to {monitor_path}")


if __name__ == "__main__":
    main()

import argparse
from datetime import datetime
from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
SRC_ROOT = REPO_ROOT / "src"
sys.path.insert(0, str(SRC_ROOT))

from src.research.eval_metrics import compute_metrics
from src.nhl_bets.projections.single_game_model import compute_game_probs


MARKETS = ["GOALS", "ASSISTS", "POINTS", "SOG", "BLOCKS"]
PROD_CALIB_MARKETS = {"ASSISTS", "POINTS"}

MARKET_LINES = {
    "GOALS": [1, 2, 3],
    "ASSISTS": [1, 2, 3],
    "POINTS": [1, 2, 3],
    "SOG": [1, 2, 3, 4, 5],
    "BLOCKS": [1, 2, 3, 4],
}

MARKET_PREFIX = {
    "GOALS": "G",
    "ASSISTS": "A",
    "POINTS": "PTS",
    "SOG": "SOG",
    "BLOCKS": "BLK",
}


def _init_con(db_path: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db_path)
    con.execute("SET memory_limit = '8GB';")
    con.execute("SET threads = 8;")
    con.execute("SET temp_directory = './duckdb_temp/';")
    return con


def _fetch_snapshot_inputs(con: duckdb.DuckDBPyConnection, start_date: str, end_date: str) -> pd.DataFrame:
    query = f"""
        WITH team_schedule AS (
            SELECT
                game_id,
                home_team AS team,
                game_date,
                season,
                LAG(game_date) OVER (PARTITION BY home_team ORDER BY game_date) AS prev_game_date,
                LAG(game_id) OVER (PARTITION BY home_team ORDER BY game_date) AS prev_game_id
            FROM dim_games
            UNION ALL
            SELECT
                game_id,
                away_team AS team,
                game_date,
                season,
                LAG(game_date) OVER (PARTITION BY away_team ORDER BY game_date) AS prev_game_date,
                LAG(game_id) OVER (PARTITION BY away_team ORDER BY game_date) AS prev_game_id
            FROM dim_games
        ),
        schedule_context AS (
            SELECT
                *,
                CASE WHEN date_diff('day', prev_game_date, game_date) = 1 THEN 1 ELSE 0 END AS is_b2b
            FROM team_schedule
        ),
        recent_roster AS (
            SELECT
                s.game_id,
                s.team,
                s.is_b2b,
                s.prev_game_id,
                gf.goalie_id,
                gf.sum_toi_L10,
                ROW_NUMBER() OVER (PARTITION BY s.game_id, s.team ORDER BY gf.sum_toi_L10 DESC) AS depth_rank
            FROM schedule_context s
            JOIN fact_goalie_features gf
                ON s.team = gf.team
                AND gf.game_date < s.game_date
                AND gf.game_date >= (s.game_date - INTERVAL 14 DAY)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY s.game_id, s.team, gf.goalie_id ORDER BY gf.game_date DESC) = 1
        ),
        prev_starter_info AS (
            SELECT
                s.game_id,
                s.team,
                pg.player_id AS prev_starter_id
            FROM schedule_context s
            JOIN fact_goalie_game_situation pg
                ON s.prev_game_id = pg.game_id
                AND s.team = pg.team
            QUALIFY ROW_NUMBER() OVER (PARTITION BY s.game_id, s.team ORDER BY pg.toi_seconds DESC) = 1
        ),
        primary_goalies AS (
            SELECT
                r.game_id,
                r.team,
                r.goalie_id,
                1 AS rn
            FROM recent_roster r
            LEFT JOIN prev_starter_info p
                ON r.game_id = p.game_id
                AND r.team = p.team
            WHERE
                CASE
                    WHEN r.is_b2b = 1 AND r.depth_rank = 1 AND r.goalie_id = p.prev_starter_id THEN 0
                    WHEN r.is_b2b = 1 AND r.depth_rank = 2 AND (p.prev_starter_id IS NULL OR p.prev_starter_id != r.goalie_id) THEN 1
                    WHEN r.is_b2b = 0 AND r.depth_rank = 1 THEN 1
                    ELSE 0
                END = 1
            QUALIFY ROW_NUMBER() OVER (PARTITION BY r.game_id, r.team ORDER BY r.depth_rank ASC) = 1
        )
        SELECT
            p.player_id,
            p.game_id,
            p.game_date::DATE AS game_date,
            p.season,
            dp.player_name AS Player,
            p.team AS Team,
            p.opp_team AS OppTeam,
            p.home_or_away,
            p.position AS Pos,
            p.xg_per_game_L10 AS G,
            p.goals_per_game_L10 AS G_realized,
            p.assists_per_game_L10 AS A,
            p.points_per_game_L10 AS PTS,
            p.sog_per_game_L10 AS SOG,
            p.blocks_per_game_L10 AS BLK,
            p.ev_ast_60_L20,
            p.pp_ast_60_L20,
            p.ev_pts_60_L20,
            p.pp_pts_60_L20,
            p.ev_toi_minutes_L20,
            p.pp_toi_minutes_L20,
            p.ev_on_ice_xg_60_L20,
            p.pp_on_ice_xg_60_L20,
            p.team_pp_xg_60_L20,
            p.ev_ipp_x_L20,
            p.pp_ipp_x_L20,
            p.primary_ast_ratio_L10,
            p.avg_toi_minutes_L10 AS TOI,
            p.avg_toi_minutes_L10 AS proj_toi,
            d.opp_sa60_L10 AS opp_sa60,
            d.opp_xga60_L10 AS opp_xga60,
            COALESCE(gf.goalie_gsax60_L10, 0.0) AS goalie_gsax60,
            CASE
                WHEN gf.sum_toi_L10 IS NULL OR gf.sum_toi_L10 = 0 THEN 0.0
                ELSE gf.sum_xga_L10 / (gf.sum_toi_L10 / 3600)
            END AS goalie_xga60,
            NULL AS implied_team_total,
            sc.is_b2b
        FROM fact_player_game_features p
        LEFT JOIN fact_team_defense_features d
            ON p.opp_team = d.team
            AND p.game_date = d.game_date
        LEFT JOIN dim_players dp
            ON p.player_id = dp.player_id
        LEFT JOIN schedule_context sc
            ON p.game_id = sc.game_id
            AND p.team = sc.team
        LEFT JOIN primary_goalies pg
            ON p.game_id = pg.game_id
            AND p.opp_team = pg.team
            AND pg.rn = 1
        LEFT JOIN fact_goalie_features gf
            ON pg.goalie_id = gf.goalie_id
            AND p.game_id = gf.game_id
        WHERE p.game_date::DATE >= DATE '{start_date}'
          AND p.game_date::DATE <= DATE '{end_date}'
          AND p.goals_per_game_L10 IS NOT NULL
    """
    return con.execute(query).df()


def _build_prob_snapshot(inputs_df: pd.DataFrame) -> pd.DataFrame:
    prob_records = []
    for row in inputs_df.itertuples(index=False):
        row_dict = row._asdict()
        context_data = {
            "opp_sa60": row.opp_sa60,
            "opp_xga60": row.opp_xga60,
            "goalie_gsax60": row.goalie_gsax60,
            "goalie_xga60": row.goalie_xga60,
            "implied_team_total": row.implied_team_total,
            "is_b2b": row.is_b2b,
            "proj_toi": row.proj_toi,
        }
        calcs = compute_game_probs(row_dict, context_data)

        def _round_or_nan(val, ndigits):
            if val is None or pd.isna(val):
                return np.nan
            return round(float(val), ndigits)

        rec = {
            "prob_snapshot_ts": row.game_date,
            "Date": row.game_date,
            "player_id": row.player_id,
            "game_id": row.game_id,
            "game_date": row.game_date,
            "season": row.season,
            "Player": row.Player,
            "Team": row.Team,
            "OppTeam": row.OppTeam,
            "mu_adj_G": _round_or_nan(calcs["mu_goals"], 4),
            "mu_adj_A": _round_or_nan(calcs["mu_assists"], 4),
            "mu_adj_PTS": _round_or_nan(calcs["mu_points"], 4),
            "mu_adj_SOG": _round_or_nan(calcs["mu_sog"], 4),
            "mu_adj_BLK": _round_or_nan(calcs["mu_blocks"], 4),
            "p_G_1plus": _round_or_nan(calcs["probs_goals"].get(1, np.nan), 4),
            "p_G_2plus": _round_or_nan(calcs["probs_goals"].get(2, np.nan), 4),
            "p_G_3plus": _round_or_nan(calcs["probs_goals"].get(3, np.nan), 4),
            "p_A_1plus": _round_or_nan(calcs["probs_assists"].get(1, np.nan), 4),
            "p_A_1plus_calibrated": _round_or_nan(calcs["probs_assists_calibrated"].get(1, np.nan), 4),
            "p_A_2plus": _round_or_nan(calcs["probs_assists"].get(2, np.nan), 4),
            "p_A_2plus_calibrated": _round_or_nan(calcs["probs_assists_calibrated"].get(2, np.nan), 4),
            "p_A_3plus": _round_or_nan(calcs["probs_assists"].get(3, np.nan), 4),
            "p_PTS_1plus": _round_or_nan(calcs["probs_points"].get(1, np.nan), 4),
            "p_PTS_1plus_calibrated": _round_or_nan(calcs["probs_points_calibrated"].get(1, np.nan), 4),
            "p_PTS_2plus": _round_or_nan(calcs["probs_points"].get(2, np.nan), 4),
            "p_PTS_2plus_calibrated": _round_or_nan(calcs["probs_points_calibrated"].get(2, np.nan), 4),
            "p_PTS_3plus": _round_or_nan(calcs["probs_points"].get(3, np.nan), 4),
            "p_SOG_1plus": _round_or_nan(calcs["probs_sog"].get(1, np.nan), 4),
            "p_SOG_2plus": _round_or_nan(calcs["probs_sog"].get(2, np.nan), 4),
            "p_SOG_3plus": _round_or_nan(calcs["probs_sog"].get(3, np.nan), 4),
            "p_SOG_4plus": _round_or_nan(calcs["probs_sog"].get(4, np.nan), 4),
            "p_SOG_5plus": _round_or_nan(calcs["probs_sog"].get(5, np.nan), 4),
            "p_BLK_1plus": _round_or_nan(calcs["probs_blocks"].get(1, np.nan), 4),
            "p_BLK_2plus": _round_or_nan(calcs["probs_blocks"].get(2, np.nan), 4),
            "p_BLK_3plus": _round_or_nan(calcs["probs_blocks"].get(3, np.nan), 4),
            "p_BLK_4plus": _round_or_nan(calcs["probs_blocks"].get(4, np.nan), 4),
            "mult_opp_sog": _round_or_nan(calcs["mult_opp_sog"], 3),
            "mult_opp_g": _round_or_nan(calcs["mult_opp_g"], 3),
            "mult_goalie": _round_or_nan(calcs["mult_goalie"], 3),
            "mult_itt": _round_or_nan(calcs["mult_itt"], 3),
            "mult_b2b": _round_or_nan(calcs["mult_b2b"], 3),
            "notes": "",
        }
        prob_records.append(rec)
    return pd.DataFrame(prob_records)


def _build_snapshot_long(prob_df: pd.DataFrame, base_df: pd.DataFrame, ctx_df: pd.DataFrame) -> pd.DataFrame:
    merged = prob_df.merge(
        base_df[
            [
                "player_id",
                "game_id",
                "game_date",
                "Player",
                "Team",
                "Pos",
                "TOI",
                "pp_toi_minutes_L20",
            ]
        ],
        on=["player_id", "game_id", "game_date", "Player", "Team"],
        how="left",
    ).merge(
        ctx_df[
            [
                "player_id",
                "game_id",
                "game_date",
                "Player",
                "Team",
                "OppTeam",
                "opp_sa60",
                "opp_xga60",
            ]
        ],
        on=["player_id", "game_id", "game_date", "Player", "Team", "OppTeam"],
        how="left",
    )

    merged = merged.rename(
        columns={
            "Pos": "position",
            "TOI": "avg_toi_minutes_L10",
            "opp_sa60": "opp_sa60_L10",
            "opp_xga60": "opp_xga60_L10",
        }
    )
    merged["home_or_away"] = pd.NA

    rows = []
    for market in MARKETS:
        prefix = MARKET_PREFIX[market]
        mu_col = f"mu_adj_{prefix}"
        for line in MARKET_LINES[market]:
            p_col = f"p_{prefix}_{line}plus"
            calib_col = f"p_{prefix}_{line}plus_calibrated"
            subset = merged.copy()
            subset["market"] = market
            subset["line"] = int(line)
            subset["p_over_baseline"] = pd.to_numeric(subset[p_col], errors="coerce")
            if calib_col in subset.columns:
                subset["p_over_calibrated"] = pd.to_numeric(subset[calib_col], errors="coerce")
            else:
                subset["p_over_calibrated"] = np.nan
            subset["mu_used"] = pd.to_numeric(subset[mu_col], errors="coerce")
            rows.append(
                subset[
                    [
                        "player_id",
                        "game_id",
                        "game_date",
                        "Player",
                        "Team",
                        "OppTeam",
                        "market",
                        "line",
                        "p_over_baseline",
                        "p_over_calibrated",
                        "mu_used",
                        "avg_toi_minutes_L10",
                        "pp_toi_minutes_L20",
                        "opp_sa60_L10",
                        "opp_xga60_L10",
                        "position",
                        "home_or_away",
                        "prob_snapshot_ts",
                    ]
                ]
            )
    return pd.concat(rows, ignore_index=True)


def _train_feature_calibrators(
    train_df: pd.DataFrame,
    seed: int,
    home_mode: str = "default",
) -> dict:
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    model_map = {}
    for (market, line), group in train_df.groupby(["market", "line"]):
        if group.empty:
            continue
        y = group["y"].values
        if len(np.unique(y)) < 2:
            model_map[(market, line)] = {"fallback": float(np.clip(y.mean(), 1e-6, 1 - 1e-6))}
            continue
        features = group[
            [
                "p_over_baseline",
                "mu_used",
                "avg_toi_minutes_L10",
                "pp_toi_minutes_L20",
                "opp_sa60_L10",
                "opp_xga60_L10",
                "home_or_away",
                "position",
            ]
        ].copy()
        if home_mode == "drop":
            features = features.drop(columns=["home_or_away"])
        elif home_mode == "impute_missing_indicator":
            features["home_or_away_missing"] = features["home_or_away"].isna().astype(int)
            features["home_or_away"] = features["home_or_away"].fillna("UNKNOWN")
        cat_cols = [c for c in ["home_or_away", "position"] if c in features.columns]
        missing_rates = {c: float(features[c].isna().mean()) for c in features.columns}
        X = pd.get_dummies(features, columns=cat_cols, dummy_na=True)
        model = Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler(with_mean=False)),
                ("logit", LogisticRegression(max_iter=1000, random_state=seed)),
            ]
        )
        model.fit(X, y)
        model_map[(market, line)] = {
            "model": model,
            "columns": X.columns.tolist(),
            "missingness_train": missing_rates,
        }
    return model_map


def _predict_feature_calibrators(
    model_map: dict,
    df: pd.DataFrame,
    home_mode: str = "default",
    feature_parity_gate: bool = False,
    parity_threshold: float = 0.05,
    fallback_col: str = "p_over_prod",
    monitoring_log: list | None = None,
) -> np.ndarray:
    preds = np.full(len(df), np.nan, dtype=float)
    for (market, line), group_idx in df.groupby(["market", "line"]).groups.items():
        idx = np.array(list(group_idx))
        model_info = model_map.get((market, line))
        if model_info is None:
            preds[idx] = df.loc[idx, "p_over_baseline"].values
            continue
        if "fallback" in model_info:
            preds[idx] = model_info["fallback"]
            continue
        features = df.loc[idx, [
            "p_over_baseline",
            "mu_used",
            "avg_toi_minutes_L10",
            "pp_toi_minutes_L20",
            "opp_sa60_L10",
            "opp_xga60_L10",
            "home_or_away",
            "position",
        ]].copy()
        if home_mode == "drop":
            features = features.drop(columns=["home_or_away"])
        elif home_mode == "impute_missing_indicator":
            features["home_or_away_missing"] = features["home_or_away"].isna().astype(int)
            features["home_or_away"] = features["home_or_away"].fillna("UNKNOWN")
        cat_cols = [c for c in ["home_or_away", "position"] if c in features.columns]
        if feature_parity_gate and "missingness_train" in model_info:
            train_rates = model_info["missingness_train"]
            test_rates = {c: float(features[c].isna().mean()) for c in features.columns}
            max_delta = 0.0
            worst_feature = None
            for key in train_rates:
                delta = abs(train_rates.get(key, 0.0) - test_rates.get(key, 0.0))
                if delta > max_delta:
                    max_delta = delta
                    worst_feature = key
            if monitoring_log is not None:
                monitoring_log.append(
                    f"Feature parity stats (market={market}, line={line}): "
                    f"max_delta={max_delta:.3f} on {worst_feature}. "
                    f"Train rates={train_rates}, test rates={test_rates}"
                )
            if max_delta > parity_threshold:
                preds[idx] = df.loc[idx, fallback_col].values
                continue
        X = pd.get_dummies(features, columns=cat_cols, dummy_na=True)
        X = X.reindex(columns=model_info["columns"], fill_value=0)
        preds[idx] = model_info["model"].predict_proba(X)[:, 1]
    return preds


def _normalize_book(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def _normalize_side(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def _normalize_market(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


DFS_TOKENS = ["underdog", "prizepicks", "sleeper", "pickem", "pick'em", "pick em"]
AMBIGUOUS_TOKENS = ["unknown", "tbd", "consensus"]


def _infer_book_type(book: str) -> str:
    name = _normalize_book(book)
    if not name:
        return "UNKNOWN"
    if any(token in name for token in AMBIGUOUS_TOKENS):
        return "UNKNOWN"
    if any(token in name for token in DFS_TOKENS):
        return "DFS_FIXED_PAYOUT"
    return "SPORTSBOOK"


def _attach_book_type(
    con: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    if df.empty:
        return df
    query = f"""
        SELECT
            date(event_start_time_utc) AS game_date,
            market_type,
            line,
            side,
            lower(trim(player_name_raw)) AS player_name_key,
            upper(trim(player_team)) AS player_team,
            book_name_raw,
            book_type
        FROM fact_prop_odds
        WHERE date(event_start_time_utc) >= DATE '{start_date}'
          AND date(event_start_time_utc) <= DATE '{end_date}'
          AND market_type IN ('GOALS', 'ASSISTS', 'POINTS', 'SOG', 'BLOCKS')
    """
    odds = con.execute(query).df()
    if odds.empty:
        df["book_type_final"] = "UNKNOWN"
        return df
    odds["market_key"] = odds["market_type"].apply(_normalize_market)
    odds["side_key"] = odds["side"].apply(_normalize_side)
    odds["line_key"] = pd.to_numeric(odds["line"], errors="coerce").round(3)

    enriched = df.copy()
    enriched["player_name_key"] = enriched["player_name"].str.strip().str.lower()
    enriched["market_key"] = enriched["market"].apply(_normalize_market)
    enriched["side_key"] = enriched["side"].apply(_normalize_side)
    enriched["line_key"] = pd.to_numeric(enriched["odds_line"], errors="coerce").round(3)

    merged = enriched.merge(
        odds[
            [
                "game_date",
                "market_key",
                "line_key",
                "side_key",
                "player_name_key",
                "player_team",
                "book_name_raw",
                "book_type",
            ]
        ],
        on=["game_date", "market_key", "line_key", "side_key", "player_name_key"],
        how="left",
    )
    team_match = (
        merged["player_team"].isna()
        | (merged["player_team"].str.upper() == merged["team"].str.upper())
    )
    merged = merged[team_match].copy()
    merged = merged.sort_values(
        ["game_date", "player_name_key", "market_key", "line_key", "side_key", "book_type"],
        na_position="last",
    )
    merged = merged.drop_duplicates(
        subset=["game_date", "player_name_key", "market_key", "line_key", "side_key"],
        keep="first",
    )
    merged["book_type_final"] = merged["book_type"]
    missing_mask = merged["book_type_final"].isna()
    merged.loc[missing_mask, "book_type_final"] = merged.loc[missing_mask, "book_name_raw"].apply(_infer_book_type)
    merged = merged.drop(columns=["market_key", "side_key", "line_key", "player_name_key"])
    return merged


def _fetch_odds_rows(
    con: duckdb.DuckDBPyConnection,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    query = f"""
        SELECT
            game_date,
            market,
            line,
            side,
            odds_decimal,
            player_id,
            player_name,
            team
        FROM fact_odds_props
        WHERE game_date >= DATE '{start_date}'
          AND game_date <= DATE '{end_date}'
          AND market IN ('GOALS', 'ASSISTS', 'POINTS', 'SOG', 'BLOCKS')
          AND side IS NOT NULL
          AND line IS NOT NULL
          AND player_id IS NOT NULL
          AND odds_decimal IS NOT NULL
    """
    df = con.execute(query).df()
    if df.empty:
        return df
    df["market"] = df["market"].str.upper()
    df["side"] = df["side"].str.upper()
    df["odds_line"] = pd.to_numeric(df["line"], errors="coerce")
    df["line_k"] = np.floor(df["odds_line"]).astype(int) + 1
    return df


def _fetch_outcomes(
    con: duckdb.DuckDBPyConnection,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    query = f"""
        SELECT
            game_id,
            player_id,
            game_date::DATE AS game_date,
            goals,
            assists,
            points,
            sog,
            blocks
        FROM fact_skater_game_all
        WHERE game_date::DATE >= DATE '{start_date}'
          AND game_date::DATE <= DATE '{end_date}'
    """
    return con.execute(query).df()


def _fetch_calibration_train(
    con: duckdb.DuckDBPyConnection,
    cutoff_date: str,
) -> pd.DataFrame:
    query = f"""
        SELECT
            market,
            line,
            p_over_baseline,
            mu_used,
            avg_toi_minutes_L10,
            pp_toi_minutes_L20,
            opp_sa60_L10,
            opp_xga60_L10,
            home_or_away,
            position,
            y,
            game_date::DATE AS game_date
        FROM fact_calibration_dataset
        WHERE game_date::DATE < DATE '{cutoff_date}'
          AND market IN ('GOALS', 'ASSISTS', 'POINTS', 'SOG', 'BLOCKS')
    """
    df = con.execute(query).df()
    if df.empty:
        return df
    df["market"] = df["market"].str.upper()
    df["line"] = df["line"].astype(int)
    return df


def _resolve_y(row):
    if row["market"] == "GOALS":
        return 1 if row["goals"] >= row["line_k"] else 0
    if row["market"] == "ASSISTS":
        return 1 if row["assists"] >= row["line_k"] else 0
    if row["market"] == "POINTS":
        return 1 if row["points"] >= row["line_k"] else 0
    if row["market"] == "SOG":
        return 1 if row["sog"] >= row["line_k"] else 0
    if row["market"] == "BLOCKS":
        return 1 if row["blocks"] >= row["line_k"] else 0
    return np.nan


def _metrics_by_variant(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (variant, market, line, side), group in df.groupby(
        ["variant", "market", "odds_line", "side"]
    ):
        metrics = compute_metrics(group["y"].values, group["p_hat"].values)
        rows.append(
            {
                "variant": variant,
                "market": market,
                "line": float(line),
                "side": side,
                "n": metrics.n,
                "logloss": metrics.logloss,
                "brier": metrics.brier,
                "ece10": metrics.ece10,
                "ece20": metrics.ece20,
                "slope": metrics.slope,
                "intercept": metrics.intercept,
            }
        )
    return pd.DataFrame(rows)


def _tail_mass(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    subset = df[df["odds_line"] == 0.5].copy()
    for market in sorted(subset["market"].unique()):
        sub = subset[subset["market"] == market]
        if sub.empty:
            continue
        for variant, col in [
            ("production", "p_over_prod"),
            ("raw", "p_over_raw"),
            ("calib_logreg_features", "p_over_feat"),
        ]:
            probs = sub[col].values
            rows.append(
                {
                    "market": market,
                    "variant": variant,
                    "n": int(len(probs)),
                    "p90": float(np.quantile(probs, 0.90)),
                    "p95": float(np.quantile(probs, 0.95)),
                    "p99": float(np.quantile(probs, 0.99)),
                    "share_p_ge_0_8": float(np.mean(probs >= 0.8)),
                    "share_p_ge_0_9": float(np.mean(probs >= 0.9)),
                }
            )
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb-path", default="data/db/nhl_backtest.duckdb")
    parser.add_argument("--start-date", default="2024-11-01")
    parser.add_argument("--end-date", default="2024-12-15")
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--drop-home-or-away", action="store_true")
    parser.add_argument("--feature-parity-gate", action="store_true")
    parser.add_argument("--feature-parity-threshold", type=float, default=0.05)
    args = parser.parse_args()

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir or f"outputs/backtesting/snapshot_replay_{ts}")
    out_dir.mkdir(parents=True, exist_ok=True)

    con = _init_con(args.duckdb_path)

    inputs_df = _fetch_snapshot_inputs(con, args.start_date, args.end_date)
    if inputs_df.empty:
        raise SystemExit("No snapshot inputs found for window.")

    prob_df = _build_prob_snapshot(inputs_df)
    base_df = inputs_df[
        [
            "player_id",
            "game_id",
            "game_date",
            "Player",
            "Team",
            "Pos",
            "TOI",
            "pp_toi_minutes_L20",
        ]
    ].copy()
    ctx_df = inputs_df[
        [
            "player_id",
            "game_id",
            "game_date",
            "Player",
            "Team",
            "OppTeam",
            "opp_sa60",
            "opp_xga60",
        ]
    ].copy()

    snapshot_long = _build_snapshot_long(prob_df, base_df, ctx_df)

    odds_df = _fetch_odds_rows(con, args.start_date, args.end_date)
    if odds_df.empty:
        raise SystemExit("No odds rows found for window.")

    outcomes_df = _fetch_outcomes(con, args.start_date, args.end_date)
    calib_train = _fetch_calibration_train(con, args.start_date)
    con.close()

    joined = odds_df.merge(
        snapshot_long,
        left_on=["player_id", "game_date", "market", "line_k"],
        right_on=["player_id", "game_date", "market", "line"],
        how="left",
    )

    if "line_x" in joined.columns:
        joined = joined.rename(columns={"line_x": "odds_line_raw"})
    if "line_y" in joined.columns:
        joined = joined.rename(columns={"line_y": "line_snapshot"})
    joined["line"] = joined["line_k"].astype(int)

    joined = joined.merge(
        outcomes_df,
        on=["player_id", "game_date"],
        how="left",
    )
    if "game_id_x" in joined.columns or "game_id_y" in joined.columns:
        joined["game_id"] = joined.get("game_id_x")
        if "game_id_y" in joined.columns:
            joined["game_id"] = joined["game_id"].fillna(joined["game_id_y"])

    joined["y"] = joined.apply(_resolve_y, axis=1)
    joined = joined[joined["y"].notna()].copy()
    joined["y"] = joined["y"].astype(int)

    joined["p_over_raw"] = joined["p_over_baseline"].astype(float)
    use_calib = joined["market"].isin(PROD_CALIB_MARKETS) & joined["p_over_calibrated"].notna()
    joined["p_over_prod"] = np.where(use_calib, joined["p_over_calibrated"], joined["p_over_raw"]).astype(float)

    if calib_train.empty:
        raise SystemExit("No calibration training rows available before window start.")

    monitoring_log: list[str] = []
    home_mode = "drop" if args.drop_home_or_away else "default"
    model_map = _train_feature_calibrators(calib_train, seed=args.seed, home_mode=home_mode)
    joined["p_over_feat"] = _predict_feature_calibrators(
        model_map,
        joined,
        home_mode=home_mode,
        feature_parity_gate=args.feature_parity_gate,
        parity_threshold=args.feature_parity_threshold,
        fallback_col="p_over_prod",
        monitoring_log=monitoring_log,
    )
    model_map_drop = _train_feature_calibrators(calib_train, seed=args.seed, home_mode="drop")
    joined["p_over_feat_drop_home"] = _predict_feature_calibrators(
        model_map_drop,
        joined,
        home_mode="drop",
        feature_parity_gate=args.feature_parity_gate,
        parity_threshold=args.feature_parity_threshold,
        fallback_col="p_over_prod",
        monitoring_log=monitoring_log,
    )
    model_map_impute = _train_feature_calibrators(
        calib_train, seed=args.seed, home_mode="impute_missing_indicator"
    )
    joined["p_over_feat_impute_home_missing"] = _predict_feature_calibrators(
        model_map_impute,
        joined,
        home_mode="impute_missing_indicator",
        feature_parity_gate=args.feature_parity_gate,
        parity_threshold=args.feature_parity_threshold,
        fallback_col="p_over_prod",
        monitoring_log=monitoring_log,
    )
    joined["p_over_feat"] = joined["p_over_feat"].fillna(joined["p_over_raw"])
    joined["p_over_feat_drop_home"] = joined["p_over_feat_drop_home"].fillna(joined["p_over_raw"])
    joined["p_over_feat_impute_home_missing"] = joined["p_over_feat_impute_home_missing"].fillna(joined["p_over_raw"])
    joined["p_over_prod"] = joined["p_over_prod"].fillna(joined["p_over_raw"])

    joined = joined[joined["p_over_raw"].notna()].copy()

    joined["p_raw"] = np.where(
        joined["side"] == "OVER",
        joined["p_over_raw"],
        1.0 - joined["p_over_raw"],
    )
    joined["p_prod"] = np.where(
        joined["side"] == "OVER",
        joined["p_over_prod"],
        1.0 - joined["p_over_prod"],
    )
    joined["p_feat"] = np.where(
        joined["side"] == "OVER",
        joined["p_over_feat"],
        1.0 - joined["p_over_feat"],
    )
    joined["p_feat_drop_home"] = np.where(
        joined["side"] == "OVER",
        joined["p_over_feat_drop_home"],
        1.0 - joined["p_over_feat_drop_home"],
    )
    joined["p_feat_impute_home_missing"] = np.where(
        joined["side"] == "OVER",
        joined["p_over_feat_impute_home_missing"],
        1.0 - joined["p_over_feat_impute_home_missing"],
    )

    variant_rows = []
    for variant, col in [
        ("production", "p_prod"),
        ("raw", "p_raw"),
        ("calib_logreg_features", "p_feat"),
        ("calib_logreg_features_drop_home", "p_feat_drop_home"),
        ("calib_logreg_features_impute_home_missing", "p_feat_impute_home_missing"),
    ]:
        tmp = joined.copy()
        tmp["variant"] = variant
        tmp["p_hat"] = tmp[col].astype(float)
        variant_rows.append(tmp)
    eval_df = pd.concat(variant_rows, ignore_index=True)

    metrics_df = _metrics_by_variant(eval_df)
    tail_df = _tail_mass(joined)
    tail_ablation_rows = []
    for market in sorted(joined["market"].unique()):
        sub = joined[(joined["market"] == market) & (joined["odds_line"] == 0.5)]
        if sub.empty:
            continue
        for variant, col in [
            ("calib_logreg_features_drop_home", "p_over_feat_drop_home"),
            ("calib_logreg_features_impute_home_missing", "p_over_feat_impute_home_missing"),
        ]:
            probs = sub[col].values
            tail_ablation_rows.append(
                {
                    "market": market,
                    "variant": variant,
                    "n": int(len(probs)),
                    "p90": float(np.quantile(probs, 0.90)),
                    "p95": float(np.quantile(probs, 0.95)),
                    "p99": float(np.quantile(probs, 0.99)),
                    "share_p_ge_0_8": float(np.mean(probs >= 0.8)),
                    "share_p_ge_0_9": float(np.mean(probs >= 0.9)),
                }
            )
    tail_ablation_df = pd.DataFrame(tail_ablation_rows)
    if not tail_ablation_df.empty:
        tail_df = pd.concat([tail_df, tail_ablation_df], ignore_index=True)

    sample_cols = [
        "game_id",
        "player_id",
        "player_name",
        "market",
        "odds_line",
        "side",
        "odds_decimal",
        "p_over_prod",
        "p_over_raw",
        "p_over_feat",
        "p_over_feat_drop_home",
        "p_over_feat_impute_home_missing",
        "p_prod",
        "p_raw",
        "p_feat",
        "p_feat_drop_home",
        "p_feat_impute_home_missing",
        "y",
    ]
    sample_df = joined[sample_cols].sample(
        n=min(50, len(joined)), random_state=args.seed
    )
    con = _init_con(args.duckdb_path)
    sample_df = _attach_book_type(con, sample_df, args.start_date, args.end_date)
    con.close()

    missing_features = [
        "p_over_baseline",
        "mu_used",
        "avg_toi_minutes_L10",
        "pp_toi_minutes_L20",
        "opp_sa60_L10",
        "opp_xga60_L10",
        "position",
        "home_or_away",
    ]
    missing_rates = {}
    for col in missing_features:
        missing_rates[col] = float(joined[col].isna().mean()) if col in joined.columns else np.nan

    missing_any = joined[missing_features].isna().any(axis=1).mean()
    missing_features_no_home = [c for c in missing_features if c != "home_or_away"]
    missing_any_no_home = joined[missing_features_no_home].isna().any(axis=1).mean()

    calib_missing_rates = {}
    for col in missing_features:
        calib_missing_rates[col] = float(calib_train[col].isna().mean()) if col in calib_train.columns else np.nan
    calib_missing_any = calib_train[missing_features].isna().any(axis=1).mean()

    row_checks = [
        {"check": "window_start", "value": args.start_date},
        {"check": "window_end", "value": args.end_date},
        {"check": "odds_rows", "value": int(len(odds_df))},
        {"check": "snapshot_rows", "value": int(len(snapshot_long))},
        {"check": "joined_rows_after_outcomes", "value": int(len(joined))},
        {"check": "rows_variant_production", "value": int((~joined["p_prod"].isna()).sum())},
        {"check": "rows_variant_raw", "value": int((~joined["p_raw"].isna()).sum())},
        {"check": "rows_variant_calib_logreg_features", "value": int((~joined["p_feat"].isna()).sum())},
        {"check": "missing_any_feature_rate_snapshot", "value": float(missing_any)},
        {"check": "missing_any_feature_rate_snapshot_ex_home_or_away", "value": float(missing_any_no_home)},
        {"check": "missing_any_feature_rate_calibration_train", "value": float(calib_missing_any)},
    ]
    for col, rate in missing_rates.items():
        row_checks.append({"check": f"missing_rate_snapshot_{col}", "value": rate})
    for col, rate in calib_missing_rates.items():
        row_checks.append({"check": f"missing_rate_calibration_train_{col}", "value": rate})

    row_checks_df = pd.DataFrame(row_checks)

    metrics_df.to_csv(out_dir / "metrics_by_variant.csv", index=False)
    tail_df.to_csv(out_dir / "tail_mass_by_variant.csv", index=False)
    row_checks_df.to_csv(out_dir / "row_consistency_checks.csv", index=False)
    sample_df.to_csv(out_dir / "sample_rows.csv", index=False)

    count_table = (
        sample_df.groupby(["market", "odds_line", "side", "book_type_final"])
        .size()
        .reset_index(name="n")
    )
    count_table.to_csv(out_dir / "ev_counts_by_bucket.csv", index=False)

    ev_df = sample_df.copy()
    ev_df["p_used_prod"] = np.where(ev_df["side"] == "OVER", ev_df["p_over_prod"], 1 - ev_df["p_over_prod"])
    ev_df["p_used_feat"] = np.where(ev_df["side"] == "OVER", ev_df["p_over_feat"], 1 - ev_df["p_over_feat"])
    ev_df["ev_prod"] = ev_df["p_used_prod"] * ev_df["odds_decimal"] - 1.0
    ev_df["ev_feat"] = ev_df["p_used_feat"] * ev_df["odds_decimal"] - 1.0
    ev_df.to_csv(out_dir / "ev_reconciliation_table.csv", index=False)

    counts_match = (
        row_checks_df[row_checks_df["check"] == "rows_variant_production"]["value"].iloc[0]
        == row_checks_df[row_checks_df["check"] == "rows_variant_raw"]["value"].iloc[0]
        == row_checks_df[row_checks_df["check"] == "rows_variant_calib_logreg_features"]["value"].iloc[0]
    )
    missing_ok = missing_any_no_home <= 0.05
    conclusion = "SNAPSHOT_PATH_ALIGNED" if counts_match and missing_ok else "SNAPSHOT_PATH_MISMATCHED"

    report_path = out_dir / "report.md"
    with report_path.open("w", encoding="utf-8") as f:
        f.write("# Snapshot Replay Diagnostic\n\n")
        f.write("## Window\n")
        f.write(f"- start_date: `{args.start_date}`\n")
        f.write(f"- end_date: `{args.end_date}`\n\n")
        f.write("## Row Consistency\n")
        f.write(row_checks_df.to_string(index=False))
        f.write("\n\n")
        f.write("## Metrics by Variant (market/line/side)\n")
        f.write(metrics_df.to_string(index=False))
        f.write("\n\n")
        f.write("## Tail Mass (line 0.5)\n")
        if tail_df.empty:
            f.write("No line=0.5 rows available for tail mass diagnostics.\n\n")
        else:
            f.write(tail_df.to_string(index=False))
            f.write("\n\n")
        f.write("## EV Reconciliation (sample rows)\n")
        f.write(f"- counts table: `{out_dir / 'ev_counts_by_bucket.csv'}`\n")
        f.write(f"- reconciliation table: `{out_dir / 'ev_reconciliation_table.csv'}`\n\n")
        f.write(f"## Conclusion\n{conclusion}\n")

    print(f"Wrote report to {report_path}")
    print(f"Wrote metrics to {out_dir / 'metrics_by_variant.csv'}")
    print(f"Wrote tail mass to {out_dir / 'tail_mass_by_variant.csv'}")
    print(f"Wrote row checks to {out_dir / 'row_consistency_checks.csv'}")
    print(f"Wrote sample rows to {out_dir / 'sample_rows.csv'}")
    print(f"Wrote EV counts to {out_dir / 'ev_counts_by_bucket.csv'}")
    print(f"Wrote EV reconciliation table to {out_dir / 'ev_reconciliation_table.csv'}")

    if monitoring_log:
        monitor_path = Path("outputs/monitoring") / f"snapshot_feature_parity_gate_{ts}.md"
        monitor_path.parent.mkdir(parents=True, exist_ok=True)
        with monitor_path.open("w", encoding="utf-8") as f:
            f.write("# Snapshot Feature Parity Gate (Research)\n\n")
            f.write(f"- drop_home_or_away: `{args.drop_home_or_away}`\n")
            f.write(f"- threshold: `{args.feature_parity_threshold}`\n\n")
            for line in monitoring_log:
                f.write(f"- {line}\n")
        print(f"Wrote monitoring log to {monitor_path}")


if __name__ == "__main__":
    main()

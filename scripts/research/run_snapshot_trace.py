import argparse
from datetime import datetime
from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from src.nhl_bets.projections.single_game_model import compute_game_probs


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


def _fetch_snapshot_row(con: duckdb.DuckDBPyConnection, player_id: int, game_date: str) -> pd.DataFrame:
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
        WHERE p.player_id = {player_id}
          AND p.game_date::DATE = DATE '{game_date}'
    """
    return con.execute(query).df()


def _prob_for_line(calcs: dict, market: str, line: float) -> float:
    prefix = MARKET_PREFIX[market]
    k = int(np.floor(float(line)) + 1)
    key = f"probs_{market.lower()}"
    probs = calcs.get(key, {})
    return float(probs.get(k, np.nan))


def _calibrated_prob_for_line(calcs: dict, market: str, line: float) -> float:
    if market not in {"ASSISTS", "POINTS"}:
        return np.nan
    prefix = MARKET_PREFIX[market]
    k = int(np.floor(float(line)) + 1)
    if market == "ASSISTS":
        probs = calcs.get("probs_assists_calibrated", {})
    else:
        probs = calcs.get("probs_points_calibrated", {})
    return float(probs.get(k, np.nan))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb-path", default="data/db/nhl_backtest.duckdb")
    parser.add_argument("--player-id", type=int, required=True)
    parser.add_argument("--game-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--market", required=True, choices=["GOALS", "ASSISTS", "POINTS", "SOG", "BLOCKS"])
    parser.add_argument("--line", type=float, required=True)
    parser.add_argument("--side", required=True, choices=["OVER", "UNDER"])
    parser.add_argument("--odds-decimal", type=float, required=True)
    parser.add_argument("--out-dir", default=None)
    args = parser.parse_args()

    con = _init_con(args.duckdb_path)
    df = _fetch_snapshot_row(con, args.player_id, args.game_date)
    con.close()

    if df.empty:
        raise SystemExit("No snapshot row found for player/date.")

    row = df.iloc[0].to_dict()
    context = {
        "opp_sa60": row.get("opp_sa60"),
        "opp_xga60": row.get("opp_xga60"),
        "goalie_gsax60": row.get("goalie_gsax60"),
        "goalie_xga60": row.get("goalie_xga60"),
        "implied_team_total": None,
        "is_b2b": row.get("is_b2b"),
        "proj_toi": row.get("proj_toi"),
    }
    calcs = compute_game_probs(row, context)

    p_over_raw = _prob_for_line(calcs, args.market, args.line)
    p_over_calib = _calibrated_prob_for_line(calcs, args.market, args.line)
    use_calib = args.market in {"ASSISTS", "POINTS"} and int(np.floor(args.line) + 1) == 1
    p_over_prod = p_over_calib if use_calib and not np.isnan(p_over_calib) else p_over_raw

    if args.side == "OVER":
        p_used_raw = p_over_raw
        p_used_prod = p_over_prod
        p_used_calib = p_over_calib
    else:
        p_used_raw = 1 - p_over_raw
        p_used_prod = 1 - p_over_prod
        p_used_calib = 1 - p_over_calib if not np.isnan(p_over_calib) else np.nan

    ev_raw = p_used_raw * args.odds_decimal - 1.0
    ev_prod = p_used_prod * args.odds_decimal - 1.0
    ev_calib = p_used_calib * args.odds_decimal - 1.0 if not np.isnan(p_used_calib) else np.nan

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir or "outputs/backtesting")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"snapshot_trace_{ts}.md"

    with out_path.open("w", encoding="utf-8") as f:
        f.write("# Snapshot Projection Trace\n\n")
        f.write("## Inputs\n")
        f.write(f"- player_id: `{args.player_id}`\n")
        f.write(f"- player: `{row.get('Player')}`\n")
        f.write(f"- team: `{row.get('Team')}` vs `{row.get('OppTeam')}`\n")
        f.write(f"- game_date: `{args.game_date}`\n")
        f.write(f"- market: `{args.market}`\n")
        f.write(f"- line: `{args.line}`\n")
        f.write(f"- side: `{args.side}`\n")
        f.write(f"- odds_decimal: `{args.odds_decimal}`\n\n")
        f.write("## Base Rates\n")
        for key in ["G", "A", "PTS", "SOG", "BLK", "TOI", "proj_toi"]:
            f.write(f"- {key}: `{row.get(key)}`\n")
        f.write("\n## Process Features\n")
        for key in [
            "ev_ast_60_L20",
            "pp_ast_60_L20",
            "ev_pts_60_L20",
            "pp_pts_60_L20",
            "ev_toi_minutes_L20",
            "pp_toi_minutes_L20",
            "ev_on_ice_xg_60_L20",
            "pp_on_ice_xg_60_L20",
            "team_pp_xg_60_L20",
            "ev_ipp_x_L20",
            "pp_ipp_x_L20",
            "primary_ast_ratio_L10",
        ]:
            f.write(f"- {key}: `{row.get(key)}`\n")
        f.write("\n## Context\n")
        for key in ["opp_sa60", "opp_xga60", "goalie_gsax60", "goalie_xga60", "is_b2b"]:
            f.write(f"- {key}: `{row.get(key)}`\n")

        f.write("\n## Multipliers + Mu\n")
        for key in ["mult_opp_sog", "mult_opp_g", "mult_goalie", "mult_itt", "mult_b2b", "toi_factor"]:
            f.write(f"- {key}: `{calcs.get(key)}`\n")
        for key in ["mu_goals", "mu_assists", "mu_points", "mu_sog", "mu_blocks"]:
            f.write(f"- {key}: `{calcs.get(key)}`\n")

        f.write("\n## Probabilities\n")
        f.write(f"- p_over_raw: `{p_over_raw}`\n")
        f.write(f"- p_over_calibrated: `{p_over_calib}`\n")
        f.write(f"- p_over_prod: `{p_over_prod}`\n")
        f.write(f"- p_used_raw: `{p_used_raw}`\n")
        f.write(f"- p_used_prod: `{p_used_prod}`\n")
        f.write(f"- p_used_calibrated: `{p_used_calib}`\n")

        f.write("\n## EV Decomposition\n")
        f.write(f"- ev_raw: `{ev_raw}`\n")
        f.write(f"- ev_prod: `{ev_prod}`\n")
        f.write(f"- ev_calibrated: `{ev_calib}`\n")

    print(f"Wrote trace to {out_path}")


if __name__ == "__main__":
    main()

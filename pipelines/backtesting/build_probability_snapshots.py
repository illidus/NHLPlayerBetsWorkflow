import duckdb
import pandas as pd
import sys
import argparse
import os
import numpy as np
import json
from datetime import datetime

# Add project root and src to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
src_dir = os.path.join(project_root, 'src')
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from nhl_bets.projections.single_game_model import compute_game_probs
    from nhl_bets.projections.config import ALPHAS, LG_SA60, LG_XGA60, LG_PACE
except ImportError as e:
    print(f"Error importing nhl_bets package: {e}")
    sys.exit(1)

def build_snapshots(db_path, start_date=None, end_date=None, model_version="full_v1", calibration_mode="segmented", use_interactions=False, variance_mode="off", output_table=None):
    conn = duckdb.connect(db_path)
    
    # Enable performance pragmas
    conn.execute("SET memory_limit = '8GB';")
    conn.execute("SET threads = 8;")
    conn.execute("SET temp_directory = './duckdb_temp/';")

    # Generate Output Table Name
    if not output_table:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_table = f"fact_probabilities_{model_version}_{timestamp}"
        
    print(f"Building Probability Snapshots (Model: {model_version}, Calib: {calibration_mode}, Interactions: {use_interactions}, Variance: {variance_mode})...")
    print(f"Output Table: {output_table}")
    
    date_filter = ""
    if start_date:
        date_filter += f" AND p.game_date >= '{start_date}'"
    if end_date:
        date_filter += f" AND p.game_date <= '{end_date}'"

    query = f"""
    WITH primary_goalies AS (
        SELECT 
            game_id, 
            team, 
            player_id as goalie_id,
            ROW_NUMBER() OVER (PARTITION BY game_id, team ORDER BY toi_seconds DESC) as rn
        FROM fact_goalie_game_situation
        WHERE situation = 'all'
    ),
    league_rolling_stats AS (
        SELECT 
            game_date,
            team,
            AVG(opp_sa60_L10) OVER (ORDER BY game_date ROWS BETWEEN 900 PRECEDING AND 1 PRECEDING) as lg_mu_sog,
            STDDEV(opp_sa60_L10) OVER (ORDER BY game_date ROWS BETWEEN 900 PRECEDING AND 1 PRECEDING) as lg_sigma_sog,
            AVG(opp_xga60_L10) OVER (ORDER BY game_date ROWS BETWEEN 900 PRECEDING AND 1 PRECEDING) as lg_mu_xga,
            STDDEV(opp_xga60_L10) OVER (ORDER BY game_date ROWS BETWEEN 900 PRECEDING AND 1 PRECEDING) as lg_sigma_xga,
            {LG_PACE} as lg_mu_pace,
            3.0 as lg_sigma_pace
        FROM fact_team_defense_features
    )
    SELECT 
        p.player_id,
        p.game_id,
        p.game_date,
        p.season,
        dp.player_name as Player,
        p.team as Team,
        p.opp_team as OppTeam,
        p.position as Pos,
        p.shooter_cluster,
        
        -- Base Stats (L10)
        p.xg_per_game_L10 as G,
        p.goals_per_game_L10 as G_realized,
        p.assists_per_game_L10 as A,
        p.points_per_game_L10 as PTS,
        p.sog_per_game_L10 as SOG,
        p.blocks_per_game_L10 as BLK,
        
        -- TOI
        p.avg_toi_minutes_L10 as TOI,
        p.avg_toi_minutes_L10 as proj_toi,
        
        -- Context (Opponent)
        d.opp_sa60_L10 as opp_sa60,
        d.opp_xga60_L10 as opp_xga60,
        
        -- Goalie Features
        COALESCE(gf.goalie_gsax60_L10, 0.0) as goalie_gsax60,
        gf.goalie_cluster,
        CASE 
            WHEN gf.sum_toi_L10 IS NULL OR gf.sum_toi_L10 = 0 THEN 0.0 
            ELSE gf.sum_xga_L10 / (gf.sum_toi_L10 / 3600)
        END as goalie_xga60,
        
        -- B2B
        CASE WHEN date_diff('day', LAG(p.game_date) OVER (PARTITION BY p.team ORDER BY p.game_date), p.game_date) = 1 THEN 1 ELSE 0 END as is_b2b,
        
        -- Derived Deltas
        ln(COALESCE(d.opp_sa60_L10, {LG_SA60}) / {LG_SA60}) as delta_opp_sog,
        ln(COALESCE(d.opp_xga60_L10, {LG_XGA60}) / {LG_XGA60}) as delta_opp_xga,
        COALESCE(gf.goalie_gsax60_L10, 0.0) as delta_goalie,
        
        -- Z-Scores
        (d.opp_sa60_L10 - lrs.lg_mu_sog) / NULLIF(lrs.lg_sigma_sog, 0) as z_opp_sog,
        (d.opp_xga60_L10 - lrs.lg_mu_xga) / NULLIF(lrs.lg_sigma_xga, 0) as z_opp_xga,
        (COALESCE(gf.goalie_gsax60_L10, 0.0) - 0.0) / 0.5 as z_goalie_gsax,
        
        -- Rolling L20 Features
        SUM(p.primary_assists) OVER (PARTITION BY p.player_id ORDER BY p.game_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as sum_pa_L20,
        SUM(p.assists) OVER (PARTITION BY p.player_id ORDER BY p.game_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as sum_a_L20,
        SUM(p.points) OVER (PARTITION BY p.player_id ORDER BY p.game_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as sum_pts_L20,
        SUM(p.toi_minutes) OVER (PARTITION BY p.player_id ORDER BY p.game_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as sum_toi_L20,
        STDDEV(p.sog) OVER (PARTITION BY p.player_id ORDER BY p.game_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as sog_std_L20
        
    FROM fact_player_game_features p
    LEFT JOIN dim_players dp ON p.player_id = dp.player_id
    LEFT JOIN fact_team_defense_features d ON p.opp_team = d.team AND p.game_date = d.game_date
    LEFT JOIN league_rolling_stats lrs ON p.opp_team = lrs.team AND p.game_date = lrs.game_date
    LEFT JOIN primary_goalies pg ON p.game_id = pg.game_id AND p.opp_team = pg.team AND pg.rn = 1
    LEFT JOIN fact_goalie_features gf ON pg.goalie_id = gf.goalie_id AND p.game_id = gf.game_id
    WHERE 1=1 {date_filter}
    AND p.goals_per_game_L10 IS NOT NULL
    """
    
    print("Executing query...")
    try:
        df = conn.execute(query).df()
        print(f"Loaded {len(df)} rows.")
    except Exception as e:
        print(f"Error executing query: {e}")
        conn.close()
        sys.exit(1)

    print(f"Computing probabilities for {model_version}...")
    prob_records = []
    
    for row in df.itertuples(index=False):
        row_dict = row._asdict()
        
        # Cluster logic
        db_cluster = getattr(row, 'shooter_cluster', None)
        if db_cluster:
            row_dict['cluster_id'] = db_cluster
        else:
            sog_60 = (row.SOG / row.TOI) * 60.0 if row.TOI > 0 else 0
            if sog_60 >= 10.0: row_dict['cluster_id'] = 'volume_shooter'
            elif sog_60 >= 6.0: row_dict['cluster_id'] = 'average_shooter'
            else: row_dict['cluster_id'] = 'low_volume'
        
        row_dict['goalie_cluster'] = getattr(row, 'goalie_cluster', 'average_goalie')
        
        toi_20 = getattr(row, 'sum_toi_L20', 0.0) or 0.0
        assist_cluster = 'unclustered'
        if toi_20 > 60:
            sum_pa = getattr(row, 'sum_pa_L20', 0.0) or 0.0
            sum_a = getattr(row, 'sum_a_L20', 0.0) or 0.0
            sum_pts = getattr(row, 'sum_pts_L20', 0.0) or 0.0
            pa_rate = (sum_pa / toi_20) * 60.0
            sa_rate = ((sum_a - sum_pa) / toi_20) * 60.0
            assist_share = (sum_a / sum_pts) if sum_pts > 0 else 0
            if pa_rate >= 0.8 and assist_share >= 0.5: assist_cluster = 'creator'
            elif sa_rate >= 0.7: assist_cluster = 'connector'
            else: assist_cluster = 'support'
        row_dict['assist_cluster'] = assist_cluster
        row_dict['sog_std_L20'] = getattr(row, 'sog_std_L20', 1.5)
        
        row_dict['calibration_mode'] = calibration_mode
        row_dict['use_interactions'] = use_interactions
        row_dict['variance_mode'] = variance_mode
        
        try:
            res = compute_game_probs(row_dict, row_dict)
        except Exception:
            continue
            
        def add_probs(market, probs_dict, probs_cal_dict, mu_val, dist):
            for line, p_val in probs_dict.items():
                p_cal = probs_cal_dict.get(line, p_val)
                prob_records.append({
                    'asof_ts': row.game_date,
                    'game_id': row.game_id,
                    'game_date': row.game_date,
                    'season': row.season,
                    'player_id': row.player_id,
                    'player_name': row.Player,
                    'team': row.Team,
                    'opp_team': row.OppTeam,
                    'market': market,
                    'line': line,
                    'p_over': p_val,
                    'p_over_calibrated': p_cal,
                    'mu_used': mu_val,
                    'dist_type': dist,
                    'model_version': model_version,
                    'feature_window': 'L10',
                    'is_calibrated': 1 if calibration_mode != 'none' else 0,
                    'matchup_type': res.get('matchup_key', 'none'),
                    'assist_cluster': assist_cluster
                })

        add_probs('GOALS', res['probs_goals'], {}, res['mu_goals'], 'poisson')
        add_probs('ASSISTS', res['probs_assists'], res['probs_assists_calibrated'], res['mu_assists'], 'poisson')
        add_probs('POINTS', res['probs_points'], res['probs_points_calibrated'], res['mu_points'], 'poisson')
        add_probs('SOG', res['probs_sog'], res['probs_sog_calibrated'], res['mu_sog'], 'negbin')
        add_probs('BLOCKS', res['probs_blocks'], res['probs_blocks_calibrated'], res['mu_blocks'], 'negbin')

    if not prob_records:
        print("No records generated.")
        conn.close()
        return

    df_probs = pd.DataFrame(prob_records)
    conn.execute(f"CREATE OR REPLACE TABLE {output_table} AS SELECT * FROM df_probs")
    print(f"Written {len(df_probs)} rows to {output_table}")
    conn.close()
    
    manifest = {
        "timestamp": datetime.now().isoformat(),
        "output_table": output_table,
        "model_version": model_version,
        "calibration_mode": calibration_mode,
        "use_interactions": use_interactions,
        "variance_mode": variance_mode,
        "start_date": start_date,
        "end_date": end_date,
        "scoring_alphas": {
            "GOALS": ALPHAS.get('GOALS'),
            "ASSISTS": ALPHAS.get('ASSISTS'),
            "POINTS": ALPHAS.get('POINTS')
        },
        "scoring_alpha_override_path": os.environ.get('NHL_BETS_SCORING_ALPHA_OVERRIDE_PATH')
    }
    
    os.makedirs("outputs/runs", exist_ok=True)
    manifest_path = os.path.join("outputs/runs", f"run_manifest_{output_table}.json")
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=4)
    print(f"Manifest saved to {manifest_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", type=str, default="2025-10-01")
    parser.add_argument("--end_date", type=str, default="2025-11-01")
    parser.add_argument("--duckdb-path", default="data/db/nhl_backtest.duckdb")
    parser.add_argument("--model-version", default="full_v1")
    parser.add_argument("--calibration", default="segmented")
    parser.add_argument("--use_interactions", action="store_true")
    parser.add_argument("--variance_mode", default="off", choices=['off', 'nb_dynamic', 'all_nb'])
    parser.add_argument("--output_table", type=str, default=None)
    args = parser.parse_args()
    build_snapshots(args.duckdb_path, args.start_date, args.end_date, args.model_version, args.calibration, args.use_interactions, args.variance_mode, args.output_table)
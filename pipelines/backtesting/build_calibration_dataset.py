import duckdb
import pandas as pd
import sys
import os

DB_PATH = 'data/db/nhl_backtest.duckdb'

def main():
    con = duckdb.connect(DB_PATH)
    
    print("Building fact_calibration_dataset...")
    
    # We join:
    # fact_probabilities (base)
    # fact_skater_game_all (outcomes)
    # fact_player_game_features (player feats)
    # fact_team_defense_features (opp defense feats)
    
    # Markets mapping to actual columns in fact_skater_game_all
    # goals -> goals
    # assists -> assists
    # points -> points
    # shots -> sog
    # blocks -> blocks
    
    query = """
    CREATE OR REPLACE TABLE fact_calibration_dataset AS
    WITH outcomes AS (
        SELECT 
            player_id, 
            game_id, 
            goals, 
            assists, 
            points, 
            sog, 
            blocks
        FROM fact_skater_game_all
    ),
    player_feats AS (
        SELECT
            player_id,
            game_id,
            home_or_away,
            position,
            avg_toi_minutes_L10,
            pp_toi_minutes_L20
        FROM fact_player_game_features
    ),
    defense_feats AS (
        SELECT
            team as opp_team_key,
            game_id,
            opp_sa60_L10,
            opp_xga60_L10
        FROM fact_team_defense_features
    )
    SELECT
        p.player_id,
        p.game_id,
        p.game_date,
        p.season,
        p.market,
        p.line,
        p.p_over as p_over_baseline,
        p.mu_used,
        p.dist_type,
        p.feature_window,
        
        -- outcome y
        CASE 
            WHEN p.market = 'GOALS' AND o.goals >= p.line THEN 1
            WHEN p.market = 'ASSISTS' AND o.assists >= p.line THEN 1
            WHEN p.market = 'POINTS' AND o.points >= p.line THEN 1
            WHEN p.market = 'SOG' AND o.sog >= p.line THEN 1
            WHEN p.market = 'BLOCKS' AND o.blocks >= p.line THEN 1
            ELSE 0
        END as y,
        
        -- features
        pf.home_or_away,
        pf.position,
        pf.avg_toi_minutes_L10,
        pf.pp_toi_minutes_L20,
        df.opp_sa60_L10,
        df.opp_xga60_L10
        
    FROM fact_probabilities p
    JOIN outcomes o ON p.player_id = o.player_id AND p.game_id = o.game_id
    LEFT JOIN player_feats pf ON p.player_id = pf.player_id AND p.game_id = pf.game_id
    LEFT JOIN defense_feats df ON p.opp_team = df.opp_team_key AND p.game_id = df.game_id
    WHERE p.market IN ('GOALS', 'ASSISTS', 'POINTS', 'SOG', 'BLOCKS')
    """
    
    try:
        con.execute(query)
        print("Success: fact_calibration_dataset created.")
        
        # Validation
        cnt = con.execute("SELECT count(*) FROM fact_calibration_dataset").fetchone()[0]
        print(f"Total rows in calibration dataset: {cnt}")
        
        sample = con.execute("SELECT * FROM fact_calibration_dataset LIMIT 5").df()
        print(sample)
        
    except Exception as e:
        print(f"Error building calibration dataset: {e}")
        sys.exit(1)
    finally:
        con.close()

if __name__ == "__main__":
    main()

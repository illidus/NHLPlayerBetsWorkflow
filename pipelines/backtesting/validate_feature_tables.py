import duckdb
import pandas as pd
import sys
from pathlib import Path

def validate_features(db_path):
    conn = duckdb.connect(db_path)
    
    report_data = []

    tables = [
        "fact_player_game_features",
        "fact_team_defense_features",
        "fact_goalie_features"
    ]

    for table in tables:
        print(f"Validating {table}...")
        
        # 1. Row Count
        count = conn.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        report_data.append({"Table": table, "Metric": "Row Count", "Value": count})
        
        if count == 0:
            print(f"CRITICAL: {table} is empty!")
            continue

        # 2. Null Checks (First Game Logic)
        # Check if first game for a player/team has NULL rolling stats
        # We'll check L10 columns as a proxy
        
        if table == "fact_player_game_features":
            entity_col = "player_id"
            metric_col = "points_per_game_L10"
        elif table == "fact_team_defense_features":
            entity_col = "team"
            metric_col = "opp_goals_against_L10"
        elif table == "fact_goalie_features":
            entity_col = "goalie_id"
            metric_col = "goalie_goals_against_per_game_L10"
        
        # Find entities with at least 1 game
        # Check the very first game for each entity
        
        query_first_game = f"""
        WITH ranked AS (
            SELECT 
                {entity_col}, 
                {metric_col}, 
                ROW_NUMBER() OVER (PARTITION BY {entity_col} ORDER BY game_date) as rn
            FROM {table}
        )
        SELECT COUNT(*) 
        FROM ranked 
        WHERE rn = 1 AND {metric_col} IS NOT NULL
        """
        
        leaky_first_games = conn.sql(query_first_game).fetchone()[0]
        report_data.append({"Table": table, "Metric": "Leaky First Games (Should be 0)", "Value": leaky_first_games})
        
        if leaky_first_games > 0:
             print(f"WARNING: {leaky_first_games} entities have values in {metric_col} on their first game. Possible Leakage!")

        # 3. Warm-up check
        # Check percentage of rows with NULLs (should be high for early games, low for later)
        null_count = conn.sql(f"SELECT COUNT(*) FROM {table} WHERE {metric_col} IS NULL").fetchone()[0]
        pct_null = (null_count / count) * 100
        report_data.append({"Table": table, "Metric": "Null % (Warm-up)", "Value": round(pct_null, 2)})

    # Export Report
    report_df = pd.DataFrame(report_data)
    report_path = Path("outputs/backtest_reports/feature_inventory.csv")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_df.to_csv(report_path, index=False)
    print(f"Report saved to {report_path}")

    conn.close()

if __name__ == "__main__":
    db_path = "data/db/nhl_backtest.duckdb"
    validate_features(db_path)

import pandas as pd
import duckdb
import os

DB_PATH = 'data/db/nhl_backtest.duckdb'
V1_PATH = 'outputs/backtest_reports/backtest_bets_v1_leaked.csv'
V2_PATH = 'outputs/backtest_reports/fact_backtest_v2_clean.csv'

def analyze():
    print("Loading datasets...")
    if not os.path.exists(V1_PATH) or not os.path.exists(V2_PATH):
        print(f"Missing V1 or V2 files. Checked: {V1_PATH}, {V2_PATH}")
        return

    df_v1 = pd.read_csv(V1_PATH)
    df_v2 = pd.read_csv(V2_PATH)

    print(f"V1 Bets: {len(df_v1)}")
    print(f"V2 Bets: {len(df_v2)}")

    # 1. EV Decay (Common Bets)
    common = pd.merge(df_v1, df_v2, on='bet_id', suffixes=('_v1', '_v2'))
    if not common.empty:
        ev_drop = common['ev_v1'].mean() - common['ev_v2'].mean()
        print(f"\n--- EV Decay Analysis (Common Bets: {len(common)}) ---")
        print(f"Avg EV V1: {common['ev_v1'].mean():.4f}")
        print(f"Avg EV V2: {common['ev_v2'].mean():.4f}")
        print(f"EV Decay: {ev_drop:.4f}")
    else:
        print("\n--- EV Decay Analysis ---")
        print("No common bets found.")

    # 2. Win Rate Delta (Assists/Points OVERs)
    print("\n--- Win Rate Delta (Assists/Points OVERs) ---")
    markets = ['ASSISTS', 'POINTS']
    
    def get_wr(df, label):
        subset = df[
            (df['market'].isin(markets)) & 
            (df['side'].str.lower() == 'over')
        ]
        if subset.empty:
            return 0.0, 0
        wins = subset[subset['result'] == 'WIN']
        wr = len(wins) / len(subset)
        return wr, len(subset)

    wr_v1, n_v1 = get_wr(df_v1, "V1")
    wr_v2, n_v2 = get_wr(df_v2, "V2")
    
    print(f"V1 Win Rate: {wr_v1:.2%} (n={n_v1})")
    print(f"V2 Win Rate: {wr_v2:.2%} (n={n_v2})")
    delta = wr_v2 - wr_v1
    print(f"Delta: {delta:.2%}")

    # 3. Backup Signal (Opponent B2B Games)
    # The heuristic triggers when OPPONENT is B2B.
    print("\n--- Backup Signal Analysis (Opponent B2B) ---")
    
    con = duckdb.connect(DB_PATH)
    con.register('df_v2', df_v2)
    
    # We need to find games where OPPONENT is B2B
    query = """
    WITH games_aug AS (
        SELECT 
            CAST(game_date AS DATE) as game_date,
            home_team,
            away_team
        FROM dim_games
    ),
    schedule AS (
        SELECT 
            team,
            game_date,
             CASE WHEN date_diff('day', LAG(game_date) OVER (PARTITION BY team ORDER BY game_date), game_date) = 1 THEN 1 ELSE 0 END as is_b2b
        FROM (
            SELECT home_team as team, game_date FROM games_aug
            UNION ALL
            SELECT away_team as team, game_date FROM games_aug
        )
    ),
    bets_aug AS (
        SELECT 
            b.*,
            CAST(b.game_date AS DATE) as g_date_dt,
            -- Determine Opponent
            CASE 
                WHEN b.team = g.home_team THEN g.away_team
                ELSE g.home_team
            END as opp_team
        FROM df_v2 b
        JOIN games_aug g ON CAST(b.game_date AS DATE) = g.game_date 
            AND (b.team = g.home_team OR b.team = g.away_team)
    )
    SELECT 
        b.*
    FROM bets_aug b
    JOIN schedule s ON b.opp_team = s.team AND b.g_date_dt = s.game_date
    WHERE s.is_b2b = 1
    """
    
    b2b_bets = con.execute(query).df()
    
    if not b2b_bets.empty:
        profit = b2b_bets['profit'].sum()
        roi = profit / b2b_bets['stake'].sum()
        wins = len(b2b_bets[b2b_bets['result'] == 'WIN'])
        wr = wins / len(b2b_bets)
        
        print(f"Opponent B2B Bets Found: {len(b2b_bets)}")
        print(f"Profit: {profit:.2f}")
        print(f"ROI: {roi:.2%}")
        print(f"Win Rate: {wr:.2%}")
        print(b2b_bets.groupby('market')[['profit', 'stake']].sum())
    else:
        print("No Opponent B2B bets found.")
        
    con.close()

if __name__ == "__main__":
    analyze()

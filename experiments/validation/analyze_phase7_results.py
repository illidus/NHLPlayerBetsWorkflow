import pandas as pd
import numpy as np

def load_data():
    df = pd.read_csv('outputs/backtest_reports/backtest_bets_calibrated.csv')
    df['game_date'] = pd.to_datetime(df['game_date'])
    return df

def analyze_october(df):
    print("\n--- Task 1: The October Problem ---")
    # Identify unique seasons (approximate by year-month clusters if needed, or just look at date range)
    # Since NHL season starts in Oct, we can group by Year-Month.
    
    monthly_stats = df.groupby(df['game_date'].dt.to_period('M')).agg({
        'profit': 'sum',
        'stake': 'sum',
        'bet_id': 'count'
    }).reset_index()
    monthly_stats['roi'] = monthly_stats['profit'] / monthly_stats['stake']
    
    print("Monthly Breakdown:")
    print(monthly_stats)
    
    # Filter out October bets (Month == 10)
    non_oct_df = df[df['game_date'].dt.month != 10]
    
    if len(non_oct_df) > 0:
        profit_no_oct = non_oct_df['profit'].sum()
        stake_no_oct = non_oct_df['stake'].sum()
        roi_no_oct = profit_no_oct / stake_no_oct
        bets_no_oct = len(non_oct_df)
        print(f"\nSimulation: Excluding October")
        print(f"Bets: {bets_no_oct} (Removed {len(df) - bets_no_oct})")
        print(f"Profit: {profit_no_oct:.2f}")
        print(f"ROI: {roi_no_oct:.2%}")
    else:
        print("No bets remaining after removing October.")

    # Alternative: Skip first 15 days of the season? 
    # Let's just stick to the Month exclusion as requested/implied by "October Problem".

def analyze_assists(df):
    print("\n--- Task 2: Assist Market Deep-Dive ---")
    assist_df = df[df['market'] == 'ASSISTS'].copy()
    
    if len(assist_df) == 0:
        print("No ASSISTS bets found.")
        return

    print(f"Total Assist Bets: {len(assist_df)}")
    print(f"Overall Assist ROI: {assist_df['profit'].sum() / assist_df['stake'].sum():.2%}")

    # Analyze by Line (0.5 vs 1.5 etc)
    print("\nBy Line:")
    by_line = assist_df.groupby('line').agg({
        'profit': 'sum', 
        'stake': 'sum', 
        'bet_id': 'count'
    })
    by_line['roi'] = by_line['profit'] / by_line['stake']
    print(by_line)

    # Analyze by EV Threshold
    print("\nBy EV Threshold (Cumulative > X):")
    thresholds = [0.05, 0.10, 0.15, 0.20, 0.25]
    for t in thresholds:
        subset = assist_df[assist_df['ev'] > t]
        if len(subset) > 0:
            roi = subset['profit'].sum() / subset['stake'].sum()
            print(f"EV > {t:.2f}: Bets={len(subset)}, ROI={roi:.2%}, Profit={subset['profit'].sum():.2f}")
        else:
            print(f"EV > {t:.2f}: No bets")

    # Combine Line and EV if useful
    print("\nSegment Analysis (Line 0.5 only):")
    subset_05 = assist_df[assist_df['line'] == 0.5]
    if len(subset_05) > 0:
         for t in thresholds:
            sub = subset_05[subset_05['ev'] > t]
            if len(sub) > 0:
                roi = sub['profit'].sum() / sub['stake'].sum()
                print(f"Line 0.5, EV > {t:.2f}: Bets={len(sub)}, ROI={roi:.2%}")

if __name__ == "__main__":
    try:
        df = load_data()
        analyze_october(df)
        analyze_assists(df)
    except Exception as e:
        print(f"Error: {e}")

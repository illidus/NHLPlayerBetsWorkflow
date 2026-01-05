import pandas as pd
import glob
import os

def get_latest_audit_csv():
    audit_dir = "outputs/audits"
    files = glob.glob(os.path.join(audit_dir, "ev_prob_audit_*.csv"))
    if not files:
        return None
    return max(files, key=os.path.getmtime)

def main():
    audit_file = get_latest_audit_csv()
    if not audit_file:
        print("No audit file found.")
        return
    
    print(f"Reading {audit_file}...")
    df = pd.read_csv(audit_file)
    
    # Apply variance-aware filtering
    # 1. EV% >= +2.0%
    mask = df['ev_percent'] >= 2.0
    
    # 2. p_model_used_in_ev >= 0.05
    mask &= df['p_model_used_in_ev'] >= 0.05
    
    # 3. If odds_decimal > 15.0, require EV% >= +10.0
    high_odds_mask = df['odds_decimal'] > 15.0
    mask &= ~(high_odds_mask & (df['ev_percent'] < 10.0))
    
    df_filtered = df[mask].copy()
    
    # Market Grouping for Priority
    def get_market_priority(market):
        if 'Goals' in market: return 1
        if 'Assists' in market: return 2
        if 'Points' in market: return 3
        return 4

    df_filtered['market_priority'] = df_filtered['market_key'].apply(get_market_priority)
    
    # Secondary rank within market by EV% descending
    df_ranked = df_filtered.sort_values(['market_priority', 'ev_percent'], ascending=[True, False])
    
    print("\nTop 10 Overall Candidates (By Market Priority):")
    top_cols = ['player_name', 'market_key', 'odds_decimal', 'implied_prob', 'p_model_used_in_ev', 'ev_percent', 'ProbSource', 'source_prob_column']
    top_10 = df_ranked.head(10)
    print(top_10[top_cols].to_markdown(index=False))
    
    print("\nNext 10 Honorable Mentions:")
    honorable = df_ranked.iloc[10:20]
    if not honorable.empty:
        print(honorable[top_cols].to_markdown(index=False))
    
    print("\nMarket Distribution Counts:")
    # Extract simplified market name
    def simplify_market(m):
        if 'Goals' in m: return 'GOALS'
        if 'Assists' in m: return 'ASSISTS'
        if 'Points' in m: return 'POINTS'
        return 'OTHER'
    
    print(df_ranked['market_key'].apply(simplify_market).value_counts())
    
    print("\nPolicy Verification Examples:")
    examples = []
    # 1. Goals Raw
    ex_g = df_ranked[df_ranked['market_key'].str.contains('Goals')].head(1)
    if not ex_g.empty: examples.append(ex_g)
    # 2. Assists Calibrated
    ex_a = df_ranked[df_ranked['market_key'].str.contains('Assists')].head(1)
    if not ex_a.empty: examples.append(ex_a)
    # 3. Points Calibrated
    ex_p = df_ranked[df_ranked['market_key'].str.contains('Points')].head(1)
    if not ex_p.empty: examples.append(ex_p)
    
    if examples:
        df_ex = pd.concat(examples)
        print(df_ex[['player_name', 'market_key', 'ProbSource', 'source_prob_column', 'ev_percent']].to_markdown(index=False))

    # Save to Excel
    out_xlsx = "outputs/ev_analysis/BestCandidatesFiltered.xlsx"
    try:
        df_ranked.to_excel(out_xlsx, index=False)
        print(f"\nSaved Best Candidates to: {out_xlsx}")
    except Exception as e:
        print(f"\nCould not save Excel: {e}")

if __name__ == "__main__":
    main()

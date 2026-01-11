import pandas as pd

def export_to_excel(bets, output_path):
    """
    Exports bets to Excel with multiple tabs.
    """
    # Convert list of Bet objects to DataFrame
    data = []
    for b in bets:
        row = {
            'Date': b.game_date,
            'Game': b.game_slug,
            'Player': b.player_raw,
            'Player_Matched': b.player_matched,
            'Team': b.team_matched,
            'Market': b.market_raw,
            'Stat': b.stat_type,
            'Line': b.line_value,
            'Side': b.side,
            'Book': 'PlayNow', # Default for now
            'Odds': b.odds_decimal,
            'Imp_Prob': b.implied_prob_raw,
            'Imp_Prob_NoVig': b.implied_prob_novig,
            'Model_Mean': b.model_mean,
            'Model_Prob': b.model_prob,
            'EV': b.ev,
            'Edge': b.edge,
            'Supported': b.supported,
            'Reason': b.reason
        }
        data.append(row)
        
    df = pd.DataFrame(data)
    
    # Filter for Ranked Bets
    df_ranked = df[
        (df['Supported'] == True) & 
        (df['EV'] > 0)
    ].sort_values(by='EV', ascending=False)
    
    # All Bets
    df_all = df[df['Supported'] == True]
    
    # Unsupported
    df_unsupported = df[df['Supported'] == False]
    
    # QA Summary (Simple aggregation)
    qa_data = {
        'Total Rows': [len(df)],
        'Supported': [len(df_all)],
        'Unsupported': [len(df_unsupported)],
        'Positive EV': [len(df_ranked)]
    }
    df_qa = pd.DataFrame(qa_data)
    
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_ranked.to_excel(writer, sheet_name='Ranked Bets', index=False)
            df_all.to_excel(writer, sheet_name='All Bets', index=False)
            df_unsupported.to_excel(writer, sheet_name='Unsupported', index=False)
            df_qa.to_excel(writer, sheet_name='QA Summary', index=False)
        print(f"Successfully exported results to {output_path}")
    except PermissionError:
        print(f"WARNING: Permission denied when writing to {output_path}. Is the file open? Skipping Excel export.")
    except Exception as e:
        print(f"WARNING: Unexpected error during Excel export: {e}. Skipping.")

def export_to_csv(bets, output_path):
    """Exports raw flat CSV."""
    data = []
    for b in bets:
        row = {
            'Date': b.game_date,
            'Game': b.game_slug,
            'Player': b.player_raw,
            'Player_Matched': b.player_matched,
            'Market': b.market_raw,
            'Stat': b.stat_type,
            'Line': b.line_value,
            'Side': b.side,
            'Book': 'PlayNow',
            'Odds': b.odds_decimal,
            'EV': b.ev,
            'Supported': b.supported,
            'Reason': b.reason
        }
        data.append(row)
    pd.DataFrame(data).to_csv(output_path, index=False)

import duckdb
import pandas as pd
from collections import Counter

def match_phase11_rows(con, phase11_table="fact_odds_historical_phase11", game_table_candidates=None, game_table_override=None):
    """
    Attempts to match historical odds rows to games using match_key_code.
    Returns a structured metrics dictionary.
    """
    if game_table_candidates is None:
        game_table_candidates = ["dim_games", "fact_games", "dim_game", "fact_game", "fact_game_schedule", "dim_game_schedule"]
        
    metrics = {
        "status": "pending",
        "game_table_selected": None,
        "match_rate": 0.0,
        "matched_count": 0,
        "total_phase11_rows": 0,
        "rows_with_match_key": 0,
        "unmatched_reasons_breakdown": {},
        "unmatched_sample": [],
        "detected_columns": {},
        "required_columns": ['game_id', 'date', 'home', 'away'],
        "daily_summary": [],
        "notes": []
    }
    
    # 1. Discover Game Table
    selected_table = None
    cols_map = {} # 'date', 'home', 'away', 'game_id'
    
    candidates = [game_table_override] if game_table_override else game_table_candidates
    
    for table in candidates:
        try:
            # Check existence
            res = con.execute(f"SELECT count(*) FROM {table}").fetchall()
            if not res:
                continue
                
            # Inspect columns
            cols = [c[1] for c in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
            metrics['detected_columns'][table] = cols
            
            # Heuristic mapping
            mapping = {}
            if 'game_id' in cols: mapping['game_id'] = 'game_id'
            elif 'id' in cols: mapping['game_id'] = 'id'
            
            if 'game_date' in cols: mapping['date'] = 'game_date'
            elif 'date' in cols: mapping['date'] = 'date'
            elif 'start_time' in cols: mapping['date'] = 'start_time'
            
            # Prefer codes
            if 'home_team_code' in cols: mapping['home'] = 'home_team_code'
            elif 'home_team' in cols: mapping['home'] = 'home_team'
            elif 'home_code' in cols: mapping['home'] = 'home_code'
            
            if 'away_team_code' in cols: mapping['away'] = 'away_team_code'
            elif 'away_team' in cols: mapping['away'] = 'away_team'
            elif 'away_code' in cols: mapping['away'] = 'away_code'
            
            if len(mapping) >= 4:
                selected_table = table
                cols_map = mapping
                break
            else:
                metrics['notes'].append(f"Table '{table}' exists but missing required columns. Found: {list(mapping.keys())}")
                
        except Exception:
            continue
            
    if not selected_table:
        if game_table_override:
             metrics['status'] = "missing_required_columns" # Or no_game_table if override failed existence
        else:
             metrics['status'] = "no_game_table"
        metrics['notes'].append("No suitable game schedule table found in candidates.")
        return metrics
        
    metrics['game_table_selected'] = selected_table
    metrics['columns_used'] = cols_map
    
    # 2. Build Game Match Keys (Join Logic)
    game_key_expr = f"CAST({cols_map['date']} AS DATE) || '|' || {cols_map['away']} || '|' || {cols_map['home']}"
    
    query = f"""
    WITH games_w_keys AS (
        SELECT 
            {cols_map['game_id']} as game_id,
            {game_key_expr} as game_match_key
        FROM {selected_table}
    ),
    matched AS (
        SELECT 
            p.row_id,
            p.match_key_code,
            p.game_date,
            g.game_id,
            CASE 
                WHEN p.match_key_code IS NULL THEN 'null_match_key_code'
                WHEN g.game_id IS NOT NULL THEN 'Matched'
                ELSE 'no_key_match'
            END as status
        FROM {phase11_table} p
        LEFT JOIN games_w_keys g ON p.match_key_code = g.game_match_key
    )
    SELECT 
        *
    FROM matched
    """
    
    try:
        df_res = con.execute(query).df()
        
        # --- Aggregation Metrics ---
        summary = df_res.groupby('status').size().to_dict()
        
        total = len(df_res)
        matched = summary.get('Matched', 0)
        null_keys = summary.get('null_match_key_code', 0)
        
        metrics['total_phase11_rows'] = int(total)
        metrics['matched_count'] = int(matched)
        metrics['match_rate'] = matched / total if total > 0 else 0.0
        metrics['rows_with_match_key'] = int(total - null_keys)
        
        if total == 0:
            metrics['status'] = "no_rows"
        elif matched > 0:
            metrics['status'] = "success"
        elif metrics['rows_with_match_key'] == 0:
            metrics['status'] = "null_match_key_code" # All are null
        else:
            metrics['status'] = "no_key_match" # Has keys but no matches
        
        metrics['unmatched_reasons_breakdown'] = {k: int(v) for k, v in summary.items() if k != 'Matched'}
        
        # Sample Keys
        unmatched_mask = df_res['status'] != 'Matched'
        if unmatched_mask.any():
            sample_df = df_res[unmatched_mask].head(20)
            for _, row in sample_df.iterrows():
                k = row.get('match_key_code')
                if k:
                    metrics['unmatched_sample'].append(f"{row['status']}: {k}")
                    
        # --- Daily Summary ---
        # Group by Date
        if 'game_date' in df_res.columns:
            # Pandas default groupby excludes Nulls unless dropna=False
            daily_groups = df_res.groupby('game_date', dropna=False)
            
            for date_val, group in daily_groups:
                d_total = len(group)
                d_matched = len(group[group['status'] == 'Matched'])
                d_null = len(group[group['status'] == 'null_match_key_code'])
                d_rate = d_matched / d_total if d_total > 0 else 0.0
                
                # Top reason
                top_reason = "Matched"
                status_counts = group['status'].value_counts()
                if not status_counts.empty:
                    top_reason = status_counts.index[0]
                    
                metrics['daily_summary'].append({
                    "date": str(date_val),
                    "total": int(d_total),
                    "with_key": int(d_total - d_null),
                    "matched": int(d_matched),
                    "match_rate": float(d_rate),
                    "top_status": str(top_reason)
                })
            
            # Sort by date (handles None/NaT if any)
            metrics['daily_summary'].sort(key=lambda x: x['date'] if x['date'] != 'None' else '0000-00-00')
                
    except Exception as e:
        metrics['status'] = "error"
        metrics['error_type'] = type(e).__name__
        metrics['error_message'] = str(e)
        metrics['notes'].append(f"Matching query failed: {e}")
        
    return metrics

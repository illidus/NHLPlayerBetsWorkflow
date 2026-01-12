import duckdb
import pandas as pd
from collections import Counter

def match_phase11_rows(con, phase11_table="fact_odds_historical_phase11", game_table_candidates=None):
    """
    Attempts to match historical odds rows to games using match_key_code.
    Returns a dictionary of metrics and samples.
    """
    if game_table_candidates is None:
        game_table_candidates = ["dim_games", "fact_games", "dim_game", "fact_game", "fact_game_schedule", "dim_game_schedule"]
        
    metrics = {
        "status": "failed",
        "game_table_selected": None,
        "match_rate": 0.0,
        "matched_count": 0,
        "total_phase11_rows": 0,
        "unmatched_reasons": {},
        "unmatched_sample": [],
        "notes": []
    }
    
    # 1. Discover Game Table
    selected_table = None
    cols_map = {} # 'date', 'home', 'away', 'game_id'
    
    for table in game_table_candidates:
        try:
            # Check existence
            res = con.execute(f"SELECT count(*) FROM {table}").fetchall()
            if not res:
                continue
                
            # Inspect columns
            cols = [c[1] for c in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
            
            # Heuristic mapping
            mapping = {}
            if 'game_id' in cols: mapping['game_id'] = 'game_id'
            elif 'id' in cols: mapping['game_id'] = 'id'
            
            if 'game_date' in cols: mapping['date'] = 'game_date'
            elif 'date' in cols: mapping['date'] = 'date'
            elif 'start_time' in cols: mapping['date'] = 'start_time'
            
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
        except Exception:
            continue
            
    if not selected_table:
        metrics['status'] = "no_game_table"
        metrics['notes'].append("No suitable game schedule table found in candidates.")
        return metrics
        
    metrics['game_table_selected'] = selected_table
    metrics['columns_used'] = cols_map
    
    # 2. Build Game Match Keys
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
            g.game_id,
            CASE 
                WHEN p.match_key_code IS NULL THEN 'Missing Match Key'
                WHEN g.game_id IS NOT NULL THEN 'Matched'
                ELSE 'No Match Found'
            END as status
        FROM {phase11_table} p
        LEFT JOIN games_w_keys g ON p.match_key_code = g.game_match_key
    )
    SELECT 
        status, 
        COUNT(*) as count,
        list(match_key_code) FILTER (match_key_code IS NOT NULL) as keys
    FROM matched
    GROUP BY status
    """
    
    try:
        df_res = con.execute(query).df()
        
        total = df_res['count'].sum()
        matched = df_res[df_res['status'] == 'Matched']['count'].sum() if not df_res.empty else 0
        
        metrics['status'] = "success"
        metrics['total_phase11_rows'] = int(total)
        metrics['matched_count'] = int(matched)
        metrics['match_rate'] = matched / total if total > 0 else 0.0
        
        # Unmatched analysis
        if not df_res.empty:
            unmatched_rows = df_res[df_res['status'] != 'Matched']
            for _, row in unmatched_rows.iterrows():
                status = row['status']
                count = row['count']
                metrics['unmatched_reasons'][status] = int(count)
                
                # Sample keys
                keys = row['keys']
                # Hardened check for iterability (NAType is not iterable)
                if keys is not None and hasattr(keys, '__iter__') and not isinstance(keys, (str, bytes)):
                    try:
                        sample = list(keys)[:5]
                        metrics['unmatched_sample'].extend([f"{status}: {k}" for k in sample])
                    except Exception:
                        pass
                
    except Exception as e:
        metrics['status'] = "error"
        metrics['notes'].append(f"Matching query failed: {e}")
        
    return metrics

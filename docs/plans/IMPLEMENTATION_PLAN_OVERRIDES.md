# Implementation Plan: Manual Lineup Overrides

## 1. Overview
This plan integrates a `manual_lineup_overrides.csv` file into the projection pipeline. This allows human operators or external feeds to override `projected_toi` and `pp_unit` for specific players, enabling the model to react to lineup changes (e.g., a promotion to PP1) that historical L20 averages cannot capture.

## 2. File Contract
**File Path:** `data/overrides/manual_lineup_overrides.csv` (Git-ignored)
**Schema:**
- `player_name` (String, required): Must match `dim_players` name.
- `team` (String, optional): For disambiguation.
- `projected_toi` (Float, optional): New total TOI projection in minutes (e.g., 18.5).
- `line_number` (Int, optional): 1-4 (Metadata only, for now).
- `pp_unit` (Int, optional): 1 or 2. Used to adjust PP split ratios.

---

## 3. Module Changes

### A. `produce_game_context.py`
**Goal:** Load the CSV and merge values into the context row `ctx`.

**New Function:**
```python
def load_lineup_overrides():
    """
    Loads manual overrides from data/overrides/manual_lineup_overrides.csv.
    Returns a dictionary keyed by Player Name (normalized) -> dict of overrides.
    """
    path = os.path.join(project_root, 'data', 'overrides', 'manual_lineup_overrides.csv')
    if not os.path.exists(path):
        return {}
        
    try:
        df = pd.read_csv(path)
        # Normalize keys
        overrides = {}
        for _, row in df.iterrows():
            p_name = row['player_name'] # Normalize if needed
            overrides[p_name] = {
                'proj_toi': row.get('projected_toi'),
                'pp_unit': row.get('pp_unit'),
                'line_number': row.get('line_number')
            }
        return overrides
    except Exception as e:
        print(f"Warning: Failed to load overrides: {e}")
        return {}
```

**Integration Point:**
Inside `main()`, before iterating `df_base`:
1. Call `overrides = load_lineup_overrides()`.
2. Inside the loop `for _, row in df_base.iterrows()`:
   ```python
   # ... existing context lookup ...
   if team in team_context_cache:
       ctx = team_context_cache[team].copy() # Copy to avoid polluting shared team cache
       
       # CHECK OVERRIDES
       if player in overrides:
           ovr = overrides[player]
           if pd.notna(ovr['proj_toi']):
               ctx['proj_toi'] = float(ovr['proj_toi'])
               ctx['is_manual_toi'] = 1 # Flag for audit
           if pd.notna(ovr['pp_unit']):
               ctx['pp_unit'] = int(ovr['pp_unit'])
               
       final_rows.append({
           # ... existing fields ...
           'proj_toi': ctx.get('proj_toi', -1.0),
           'pp_unit': ctx.get('pp_unit', -1),
           'is_manual_toi': ctx.get('is_manual_toi', 0)
       })
   ```

### B. `single_game_model.py`
**Goal:** Use `pp_unit` to rescue the PP Ratio if historical L20 data says "0% PP Time".

**Logic Update in `compute_game_probs`:**
Current logic calculates `pp_ratio` solely from L20 history. We will inject a heuristic block *before* the ratio is applied.

```python
    # ... existing TOI extraction ...
    
    # 3. PP Split Logic (Enhanced)
    pp_ratio = 0.0
    if (ev_toi_L20 + pp_toi_L20) > 0:
        pp_ratio = pp_toi_L20 / (ev_toi_L20 + pp_toi_L20)
        
    # --- OVERRIDE INJECTION ---
    pp_unit = get_val(context_data if context_data else {}, 'pp_unit', -1)
    
    # Heuristic: If PP1 and historical ratio is low (< 30%), boost it.
    # PP1 usually gets ~60-70% of PP time, equating to ~15-20% of Total TOI depending on penalties.
    # Simpler: Set a floor for PP ratio if promoted.
    
    if pp_unit == 1:
        # If historical PP ratio is < 0.40 (e.g. was PP2 or none), force a floor of 0.50 (conservative PP1 share)
        # This ensures proj_pp_toi becomes non-zero.
        if pp_ratio < 0.40:
            pp_ratio = 0.50 
    elif pp_unit == 2:
        # PP2 floor (e.g. 0.20)
        if pp_ratio < 0.10:
            pp_ratio = 0.25

    # Recalculate splits with new ratio
    proj_pp_toi = proj_toi * pp_ratio
    proj_ev_toi = proj_toi - proj_pp_toi
    
    # ... proceed to IPP/Rate calcs ...
```

### C. `single_game_probs.py`
**Goal:** Ensure new columns pass through `load_data` and merge.

**Changes:**
1. **`process_base_projections`:** No changes needed (it handles base stats).
2. **`load_data` context merging:** 
   Add `'proj_toi', 'pp_unit', 'is_manual_toi'` to `ctx_cols` list so they aren't dropped during the merge.
   ```python
   ctx_cols = [..., 'proj_toi', 'pp_unit', 'is_manual_toi']
   ```
3. **`main` loop:**
   Ensure `context_data` passed to `compute_game_probs` includes these keys. (Existing code passes the whole row, so this is likely covered, but explicit mapping is safer).

## 4. Validation & Safety

1.  **Type Safety:** Ensure `proj_toi` is cast to float and `pp_unit` to int. Invalid types (strings in CSV) should be caught or ignored.
2.  **Audit Column:** Add `is_manual_toi` (boolean/int) to the final `SingleGamePropProbabilities.csv` output. This allows backtesting to filter in/out manual intervention games.
3.  **Logging:** `produce_game_context.py` should print a summary:
    `"Loaded 12 overrides. Applied 10 matches."`
4.  **Fallback:** If `manual_lineup_overrides.csv` is missing, the function returns empty dict, and pipeline proceeds with standard L10/L20 logic.

## 5. Execution Steps
1.  Create `data/overrides/` directory.
2.  Create dummy `manual_lineup_overrides.csv` for testing.
3.  Apply code changes.
4.  Run `produce_game_context.py` -> Verify `GameContext.csv` has `proj_toi`.
5.  Run `single_game_probs.py` -> Verify `SingleGamePropProbabilities.csv` output reflects the TOI change in `mu`.

---

## 6. Usage Guide (How to Override)

**Objective:** Adjust specific player projections for injury returns, line promotions, or benchings.

1.  **Edit the CSV:**
    Open `data/overrides/manual_lineup_overrides.csv` (or create it if missing).

    ```csv
    player_name,team,projected_toi,line_number,pp_unit
    Connor McDavid,EDM,23.5,1,1
    Evan Bouchard,EDM,24.0,,1
    New Guy,CHI,15.0,2,2
    ```

    *   `projected_toi`: The new total minutes (e.g., 20.5). If blank, uses model default (L20 avg).
    *   `pp_unit`: `1` (Primary), `2` (Secondary), or blank.
        *   **Effect:** If `1`, ensures at least 50% PP share even if history is zero. If `2`, ensures 25% floor.

2.  **Run the Pipeline:**
    Execute the standard projection workflow. The overrides are applied during the "Game Context" step.
    
    ```bash
    python pipelines/production/run_production_pipeline.py
    ```

3.  **Verify:**
    Check `outputs/projections/SingleGamePropProbabilities.csv`.
    *   Look for the `is_manual_toi` column (1 = overridden).
    *   Confirm `mu_adj_...` values have shifted compared to a run without the file.
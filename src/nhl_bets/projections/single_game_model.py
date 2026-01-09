import numpy as np
import pandas as pd
import logging
from .config import BETAS, ALPHAS, LG_SA60, LG_XGA60, ITT_BASE, LG_PACE
from .distributions import calculate_poisson_probs, calculate_nbinom_probs
import joblib
import os
from scipy.special import logit

def apply_posthoc_calibration(prob, market, context_data=None, model_dir="data/models/calibrators_posthoc/"):
    """
    Applies a pre-trained post-hoc calibrator to a raw probability.
    Supports 'segmented' mode if context_data provides 'calibration_mode'='segmented' and 'position'.
    """
    if market not in ['ASSISTS', 'POINTS']:
        return prob
    
    # Determine Model Filename
    mode = 'global'
    segment = ''
    if context_data:
        mode = context_data.get('calibration_mode', 'global')
        if mode == 'segmented':
            # Segment by Position (F/D)
            # We need position passed in context_data or player_data.
            # Usually player_data has position. 
            # Context data is passed here. We should ensure 'position' is in context if we want to use it.
            # Actually, compute_game_probs has player_data. Let's pass player_data['Pos'] to this func?
            # Or assume context_data has it.
            pos = context_data.get('position', 'F') # Default F
            if pos == 'D':
                segment = '_D'
            else:
                segment = '_F'
    
    # Filename: calib_posthoc_{MARKET}{_SEGMENT}.joblib
    # Global: calib_posthoc_ASSISTS.joblib
    # Segmented: calib_posthoc_ASSISTS_D.joblib
    
    filename = f"calib_posthoc_{market.upper()}{segment}.joblib"
    model_path = os.path.join(model_dir, filename)
    
    # Fallback to global if segmented missing
    if not os.path.exists(model_path) and mode == 'segmented':
        model_path = os.path.join(model_dir, f"calib_posthoc_{market.upper()}.joblib")

    if not os.path.exists(model_path):
        return prob
        
    try:
        calib_data = joblib.load(model_path)
        method = calib_data['method']
        model = calib_data['model']
        
        if method == 'Isotonic':
            p_calib = model.transform([prob])[0]
        elif method == 'Platt':
            eps = 1e-10
            p_clamped = np.clip(prob, eps, 1-eps)
            l = logit(p_clamped).reshape(-1, 1)
            p_calib = model.predict_proba(l)[0, 1]
        else:
            return prob
            
        return np.clip(p_calib, 1e-6, 1 - 1e-6)
    except Exception:
        return prob

def calculate_adjusted_mu(base_stat, multiplier, toi_factor=1.0):
    """
    Calculate final mu based on base stat, environment multiplier, and TOI adjustment.
    base_stat: The base expected value (e.g. per game average or projection)
    multiplier: The combined environmental multiplier
    toi_factor: Ratio of Projected TOI / Base TOI (if base_stat is not already TOI adjusted)
    """
    return base_stat * multiplier * toi_factor

def compute_game_probs(
    player_data, 
    context_data=None, 
    use_defaults=True
):
    """
    Computes probabilities for a single player-game.
    """
    
    # 1. Extract Base Stats
    # Helper to safe get float
    def get_val(d, k, default=0.0):
        val = d.get(k)
        if val is None or (isinstance(val, float) and np.isnan(val)) or pd.isna(val):
            return default
        return val

    mu_base_goals = get_val(player_data, 'G', 0)
    mu_realized_goals = get_val(player_data, 'G_realized', -1.0)
    mu_base_assists = get_val(player_data, 'A', 0)
    mu_base_points = get_val(player_data, 'PTS', 0)
    
    # 2. TOI Handling (Moved Up)
    base_toi = get_val(player_data, 'TOI', 15.0)
    if base_toi == 0: base_toi = 15.0
    
    # Try to get proj_toi from context_data or player_data (row from merge)
    proj_toi = get_val(context_data if context_data else {}, 'proj_toi', -1.0)
    if proj_toi < 0:
        proj_toi = get_val(player_data, 'proj_toi', base_toi)
    
    toi_factor = proj_toi / base_toi if base_toi > 0 else 1.0

    # --- ENHANCED ASSISTS/POINTS MU_BASE (Process-Driven) ---
    # Try to use splits if available
    # Prefer L40 for Rates (Stability)
    ev_ast_60 = get_val(player_data, 'ev_ast_60_L40', -1.0)
    if ev_ast_60 < 0: ev_ast_60 = get_val(player_data, 'ev_ast_60_L20', -1.0)

    pp_ast_60 = get_val(player_data, 'pp_ast_60_L40', -1.0)
    if pp_ast_60 < 0: pp_ast_60 = get_val(player_data, 'pp_ast_60_L20', -1.0)
    
    ev_pts_60 = get_val(player_data, 'ev_pts_60_L40', -1.0)
    if ev_pts_60 < 0: ev_pts_60 = get_val(player_data, 'ev_pts_60_L20', -1.0)

    pp_pts_60 = get_val(player_data, 'pp_pts_60_L40', -1.0)
    if pp_pts_60 < 0: pp_pts_60 = get_val(player_data, 'pp_pts_60_L20', -1.0)
    
    # Keep L20 for TOI (Recency matters for usage)
    ev_toi_L20 = get_val(player_data, 'ev_toi_minutes_L20', 0.0)
    pp_toi_L20 = get_val(player_data, 'pp_toi_minutes_L20', 0.0)
    
    if ev_ast_60 >= 0 and (ev_toi_L20 + pp_toi_L20) > 0:
        # Split proj_toi based on historical ratio
        pp_ratio = pp_toi_L20 / (ev_toi_L20 + pp_toi_L20)
        
        # --- PP UNIT OVERRIDE LOGIC ---
        # If manual overrides provided pp_unit, adjust ratio if history is stale
        pp_unit = get_val(context_data if context_data else {}, 'pp_unit', -1)
        if pp_unit == 1:
            # If promoted to PP1 but historical ratio is low (e.g. was PP2/None), boost it.
            if pp_ratio < 0.40: 
                pp_ratio = 0.50 
        elif pp_unit == 2:
            # If PP2, ensure some floor
            if pp_ratio < 0.10:
                pp_ratio = 0.25
        
        proj_pp_toi = proj_toi * pp_ratio
        proj_ev_toi = proj_toi - proj_pp_toi
        
        # Use IPP * OnIceXG if available for even more process-driven Mu
        # Prefer L40 IPP/xG
        ev_ipp = get_val(player_data, 'ev_ipp_x_L40', -1.0)
        if ev_ipp < 0: ev_ipp = get_val(player_data, 'ev_ipp_x_L20', 0.0)
        
        ev_on_ice_xg_60 = get_val(player_data, 'ev_on_ice_xg_60_L40', -1.0)
        if ev_on_ice_xg_60 < 0: ev_on_ice_xg_60 = get_val(player_data, 'ev_on_ice_xg_60_L20', 0.0)

        pp_ipp = get_val(player_data, 'pp_ipp_x_L40', -1.0)
        if pp_ipp < 0: pp_ipp = get_val(player_data, 'pp_ipp_x_L20', 0.0)

        pp_on_ice_xg_60 = get_val(player_data, 'pp_on_ice_xg_60_L40', -1.0)
        if pp_on_ice_xg_60 < 0: pp_on_ice_xg_60 = get_val(player_data, 'pp_on_ice_xg_60_L20', 0.0)
        
        if ev_ipp > 0 and ev_on_ice_xg_60 > 0:
             # POINTS
             mu_ev_pts = (ev_ipp * ev_on_ice_xg_60) * (proj_ev_toi / 60)
             mu_pp_pts = (pp_ipp * pp_on_ice_xg_60) * (proj_pp_toi / 60)
             mu_base_points = mu_ev_pts + mu_pp_pts
             
             # ASSISTS
             # We use the same split but with the assist rate component
             # Or just use the total assists rate if IPP for assists is not provided
             # Check for explicit IPP Assist keys (injected by experiment runner)
             ev_ipp_ast = get_val(player_data, 'ev_ipp_ast', -1.0)
             ev_oig_60 = get_val(player_data, 'ev_on_ice_goals_60', -1.0)
             
             if ev_ipp_ast >= 0 and ev_oig_60 >= 0:
                 # IPP-based Assist Model
                 mu_ev_ast = (ev_ipp_ast * ev_oig_60) * (proj_ev_toi / 60)
                 
                 pp_ipp_ast = get_val(player_data, 'pp_ipp_ast', 0.0)
                 pp_oig_60 = get_val(player_data, 'pp_on_ice_goals_60', 0.0)
                 mu_pp_ast = (pp_ipp_ast * pp_oig_60) * (proj_pp_toi / 60)
             else:
                 # Standard Rate Model
                 mu_ev_ast = ev_ast_60 * (proj_ev_toi / 60)
                 mu_pp_ast = pp_ast_60 * (proj_pp_toi / 60)
             
             mu_base_assists = mu_ev_ast + mu_pp_ast
             
             # Apply primary assist ratio if available to potentially scale (Optional)
             # But the goal is accuracy, and total assists is what we bet on.
        else:
             # Fallback to basic split if IPP logic fails
             mu_base_assists = (ev_ast_60 * proj_ev_toi + pp_ast_60 * proj_pp_toi) / 60
             mu_base_points = (ev_pts_60 * proj_ev_toi + pp_pts_60 * proj_pp_toi) / 60

    # --- SOG PROJECTION (Corsi-Enhanced) ---
    # Theory: Mu = (Corsi_L20_Rate * Thru_Pct_L40) * TOI
    corsi_60 = get_val(player_data, 'corsi_per_60_L20', -1.0)
    thru_pct = get_val(player_data, 'thru_pct_L40', -1.0)
    
    if corsi_60 >= 0 and thru_pct >= 0:
        mu_base_sog = (corsi_60 * thru_pct) * (proj_toi / 60.0)
    else:
        mu_base_sog = get_val(player_data, 'SOG', 0)

    mu_base_blocks = get_val(player_data, 'BLK', 0)
    
    # --- THEORY ENFORCEMENT GUARDS (MODEL_PROJECTION_THEORY.md) ---
    if mu_base_goals > 0:
        logger = logging.getLogger(__name__)
        # Guard A: No Banned Inputs
        if abs(mu_base_goals - mu_realized_goals) < 1e-9 and mu_realized_goals >= 0:
             if not hasattr(compute_game_probs, "_warned_guard_a"):
                 logger.warning(
                     f"THEORY WARNING (Guard A): mu_base_goals ({mu_base_goals}) matches realized goals exactly. "
                     "Low-frequency events (GOALS) must be process-based (xG), not outcome-based. "
                     "(Further instances of this warning suppressed for this session). "
                     "Check docs/MODEL_PROJECTION_THEORY.md."
                 )
                 compute_game_probs._warned_guard_a = True

        # Guard B: No Discretization
        is_discrete = abs(mu_base_goals * 10 - round(mu_base_goals * 10)) < 1e-7
        if is_discrete:
             if not hasattr(compute_game_probs, "_warned_guard_b"):
                 logger.warning(
                     f"THEORY WARNING (Guard B): mu_base_goals ({mu_base_goals}) appears discretized. "
                     "Expected continuous xG-based intensity. "
                     "(Further instances of this warning suppressed for this session). "
                      "Reference docs/MODEL_PROJECTION_THEORY.md."
                 )
                 compute_game_probs._warned_guard_b = True

        # Guard C: Precision Preservation
        if abs(mu_base_goals - round(mu_base_goals, 5)) < 1e-9 and not is_discrete:
            if not hasattr(compute_game_probs, "_warned_guard_c"):
                logger.warning(
                    f"PRECISION WARNING (Guard C): mu_base_goals ({mu_base_goals}) has < 6 decimal precision. "
                    "Ensure float_format='%.6f' is used during export. "
                    "(Further instances of this warning suppressed for this session). "
                     "Reference docs/MODEL_PROJECTION_THEORY.md."
                )
                compute_game_probs._warned_guard_c = True

    # (TOI handling removed from here as it was moved up)
    
    # 2. Calculate Multipliers
    mult_opp_sog = 1.0
    mult_opp_g = 1.0
    mult_goalie = 1.0
    mult_itt = 1.0
    mult_b2b = 1.0
    mult_pace = 1.0
    
    def is_valid(val):
        return val is not None and not (isinstance(val, float) and np.isnan(val)) and not pd.isna(val)

    if context_data:
        # --- Delta-Based Multipliers (New Logic) ---
        # Prioritize pre-computed deltas if available
        
        # 1. Opponent SOG (Defensive Strength vs Shots)
        if 'delta_opp_sog' in context_data:
            mult_opp_sog = np.exp(BETAS['opp_sog'] * context_data['delta_opp_sog'])
        else:
            # Fallback to Raw
            val = context_data.get('opp_sa60')
            if is_valid(val):
                 mult_opp_sog = (val / LG_SA60) ** BETAS['opp_sog']
        
        # 2. Opponent Scoring (Defensive Strength vs Goals)
        if 'delta_opp_xga' in context_data:
            mult_opp_g = np.exp(BETAS['opp_g'] * context_data['delta_opp_xga'])
        else:
            val = context_data.get('opp_xga60')
            if is_valid(val):
                 mult_opp_g = (val / LG_XGA60) ** BETAS['opp_g']
             
        # 3. Goalie (Impact on Goals)
        # Delta is GSAx/60 (positive is good). Beta is positive magnitude.
        # Effect should be negative (Good Goalie -> Lower Goals).
        if 'delta_goalie' in context_data:
            # GSAx is already additive relative to expected. 
            # We treat it as exponential suppression.
            mult_goalie = np.exp(-BETAS['goalie'] * context_data['delta_goalie'])
            mult_goalie = max(0.5, min(1.5, mult_goalie))
        else:
            val = context_data.get('goalie_gsax60')
            if is_valid(val):
                g_xga = context_data.get('goalie_xga60')
                if not is_valid(g_xga): g_xga = LG_XGA60
                if g_xga > 0:
                    raw_m = 1 - (val / g_xga)
                    raw_m = max(0.1, raw_m) 
                    mult_goalie = raw_m ** BETAS['goalie']
                    mult_goalie = max(0.5, min(1.5, mult_goalie))
        
        # 4. Pace (New)
        if 'delta_pace' in context_data:
            mult_pace = np.exp(BETAS['pace'] * context_data['delta_pace'])
                
        # 5. ITT (Implied Team Total)
        val = context_data.get('implied_team_total')
        if is_valid(val):
             mult_itt = (val / ITT_BASE) ** BETAS['itt']
             
        # 6. B2B
        val = context_data.get('is_b2b')
        if is_valid(val) and val:
             # Check if true-like
             if val in [1, '1', True]:
                 mult_b2b = np.exp(BETAS['b2b'])

    # 3. Calculate Adjusted Mu
    # Pace affects EVERYTHING (more events -> more stats)
    
    # SOG / BLK: Affected by Opp SOG, B2B, Pace
    mu_adj_sog = calculate_adjusted_mu(mu_base_sog, mult_opp_sog * mult_b2b * mult_pace, toi_factor)
    mu_adj_blocks = calculate_adjusted_mu(mu_base_blocks, mult_opp_sog * mult_b2b * mult_pace, toi_factor)
    
    # Scoring: Affected by Opp G, Goalie, ITT, B2B, Pace
    scoring_mult = mult_opp_g * mult_goalie * mult_itt * mult_b2b * mult_pace
    mu_adj_goals = calculate_adjusted_mu(mu_base_goals, scoring_mult, toi_factor)
    
    # If enhanced logic was used, mu_base already includes proj_toi adjustment
    toi_factor_ast_pts = 1.0 if (ev_ast_60 >= 0 and (ev_toi_L20 + pp_toi_L20) > 0) else toi_factor
    
    mu_adj_assists = calculate_adjusted_mu(mu_base_assists, scoring_mult, toi_factor_ast_pts)
    mu_adj_points = calculate_adjusted_mu(mu_base_points, scoring_mult, toi_factor_ast_pts)
    
    # 4. Calculate Probabilities
    
    # --- CLUSTER ADJUSTMENTS (Alpha/Variance) ---
    alpha_sog = ALPHAS['SOG']
    if context_data:
        cluster = context_data.get('cluster_id', 'default')
        if cluster == 'volume_shooter':
            # Volume shooters are more consistent -> Lower Dispersion
            alpha_sog *= 0.8
        elif cluster == 'low_volume':
            # Low volume are more volatile -> Higher Dispersion
            alpha_sog *= 1.2
            
    probs_goals = calculate_poisson_probs(mu_adj_goals, max_k=3)
    probs_assists = calculate_poisson_probs(mu_adj_assists, max_k=3)
    probs_points = calculate_poisson_probs(mu_adj_points, max_k=3)
    
    probs_sog = calculate_nbinom_probs(mu_adj_sog, alpha_sog, max_k=5)
    probs_blocks = calculate_nbinom_probs(mu_adj_blocks, ALPHAS['BLK'], max_k=4)
    
    # --- POST-HOC CALIBRATION (Integration Phase 8) ---
    # Prepare data for segmentation (pass position from player_data)
    if context_data and 'position' not in context_data:
        # Inject position from player_data if missing
        context_data['position'] = player_data.get('Pos', 'F')
        
    probs_assists_calib = {k: apply_posthoc_calibration(v, 'ASSISTS', context_data) for k, v in probs_assists.items()}
    probs_points_calib = {k: apply_posthoc_calibration(v, 'POINTS', context_data) for k, v in probs_points.items()}

    # 5. Pack Results
    result = {
        # Mus
        'mu_goals': mu_adj_goals,
        'mu_assists': mu_adj_assists,
        'mu_points': mu_adj_points,
        'mu_sog': mu_adj_sog,
        'mu_blocks': mu_adj_blocks,
        
        # Probs
        'probs_goals': probs_goals,
        'probs_assists': probs_assists,
        'probs_points': probs_points,
        'probs_assists_calibrated': probs_assists_calib,
        'probs_points_calibrated': probs_points_calib,
        'probs_sog': probs_sog,
        'probs_blocks': probs_blocks,
        
        # Multipliers (for debugging/reporting)
        'mult_opp_sog': mult_opp_sog,
        'mult_opp_g': mult_opp_g,
        'mult_goalie': mult_goalie,
        'mult_itt': mult_itt,
        'mult_b2b': mult_b2b,
        'mult_pace': mult_pace,
        'toi_factor': toi_factor
    }
    
    return result

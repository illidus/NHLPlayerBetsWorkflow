import numpy as np
import pandas as pd
import logging
from .config import BETAS, ALPHAS, LG_SA60, LG_XGA60, ITT_BASE
from .distributions import calculate_poisson_probs, calculate_nbinom_probs
import joblib
import os
from scipy.special import logit

def apply_posthoc_calibration(prob, market, model_dir="data/models/calibrators_posthoc/"):
    """
    Applies a pre-trained post-hoc calibrator to a raw probability.
    """
    if market not in ['ASSISTS', 'POINTS']:
        return prob
        
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
    
    player_data: dict or series containing:
        - Player, Team, Position (metadata)
        - G, A, PTS, SOG, BLK (base stats, expected to be per-game averages or projections)
        - TOI (base toi) - optional
        - proj_toi (projected toi) - optional
        
    context_data: dict or series containing:
        - opp_sa60, opp_xga60 (metrics)
        - goalie_gsax60, goalie_xga60 (metrics)
        - implied_team_total
        - is_b2b
        
    Returns:
        dict containing 'mu' values and 'p_over' values for all markets.
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
    
    def is_valid(val):
        return val is not None and not (isinstance(val, float) and np.isnan(val)) and not pd.isna(val)

    if context_data:
        # Opponent SOG/BLK
        val = context_data.get('opp_sa60')
        if is_valid(val):
             mult_opp_sog = (val / LG_SA60) ** BETAS['opp_sog']
        
        # Opponent Scoring (Goals/Points)
        val = context_data.get('opp_xga60')
        if is_valid(val):
             mult_opp_g = (val / LG_XGA60) ** BETAS['opp_g']
             
        # Goalie
        val = context_data.get('goalie_gsax60')
        if is_valid(val):
            g_xga = context_data.get('goalie_xga60')
            if not is_valid(g_xga): g_xga = LG_XGA60
            
            if g_xga > 0:
                # Theory: Mult_goalie = (1 - (gsax60 / xga60)) ** beta_goalie
                # Clamp result to [0.5, 1.5]
                raw_m = 1 - (val / g_xga)
                # Avoid negative base for power if gsax60 > xga60 significantly (rare but possible)
                raw_m = max(0.1, raw_m) 
                mult_goalie = raw_m ** BETAS['goalie']
                mult_goalie = max(0.5, min(1.5, mult_goalie))
                
        # ITT
        val = context_data.get('implied_team_total')
        if is_valid(val):
             mult_itt = (val / ITT_BASE) ** BETAS['itt']
             
        # B2B
        val = context_data.get('is_b2b')
        if is_valid(val) and val:
             # Check if true-like
             if val in [1, '1', True]:
                 mult_b2b = np.exp(BETAS['b2b'])

    # 3. Calculate Adjusted Mu
    # SOG / BLK: Affected by Opp SOG, B2B
    mu_adj_sog = calculate_adjusted_mu(mu_base_sog, mult_opp_sog * mult_b2b, toi_factor)
    mu_adj_blocks = calculate_adjusted_mu(mu_base_blocks, mult_opp_sog * mult_b2b, toi_factor)
    
    # Scoring: Affected by Opp G, Goalie, ITT, B2B
    scoring_mult = mult_opp_g * mult_goalie * mult_itt * mult_b2b
    mu_adj_goals = calculate_adjusted_mu(mu_base_goals, scoring_mult, toi_factor)
    
    # If enhanced logic was used, mu_base already includes proj_toi adjustment
    toi_factor_ast_pts = 1.0 if (ev_ast_60 >= 0 and (ev_toi_L20 + pp_toi_L20) > 0) else toi_factor
    
    mu_adj_assists = calculate_adjusted_mu(mu_base_assists, scoring_mult, toi_factor_ast_pts)
    mu_adj_points = calculate_adjusted_mu(mu_base_points, scoring_mult, toi_factor_ast_pts)
    
    # 4. Calculate Probabilities
    probs_goals = calculate_poisson_probs(mu_adj_goals, max_k=3)
    probs_assists = calculate_poisson_probs(mu_adj_assists, max_k=3)
    probs_points = calculate_poisson_probs(mu_adj_points, max_k=3)
    
    probs_sog = calculate_nbinom_probs(mu_adj_sog, ALPHAS['SOG'], max_k=5)
    probs_blocks = calculate_nbinom_probs(mu_adj_blocks, ALPHAS['BLK'], max_k=4)
    
    # --- POST-HOC CALIBRATION (Integration Phase 8) ---
    probs_assists_calib = {k: apply_posthoc_calibration(v, 'ASSISTS') for k, v in probs_assists.items()}
    probs_points_calib = {k: apply_posthoc_calibration(v, 'POINTS') for k, v in probs_points.items()}

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
        'toi_factor': toi_factor
    }
    
    return result

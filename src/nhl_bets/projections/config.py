# Default Adjustment Betas (Power)
BETAS = {
    'opp_sog': 0.15,      # Opponent SOG/BLK impact
    'opp_g': 0.15,        # Opponent Goals/Points impact
    'goalie': 0.20,       # Goalie impact on Goals
    'itt': 0.50,          # Implied Team Total impact
    'b2b': -0.05          # Back-to-back negative impact (log-space coeff or linear multiplier power)
}

# Negative Binomial Dispersion (Alpha) defaults
# Variance = mu + alpha * mu^2
ALPHAS = {
    'SOG': 0.35,
    'BLK': 0.60
}

# League Baselines (Approximate 2023-24 values, can be overridden)
LG_SA60 = 30.0
LG_XGA60 = 2.8
ITT_BASE = 3.0

import os

# Production Probability Policy
# Determines which probability column to use for EV calculations
MARKET_POLICY = {
    'ASSISTS': 'p_over_calibrated',
    'POINTS': 'p_over_calibrated',
    'GOALS': 'p_over',
    'SOG': 'p_over',
    'BLOCKS': 'p_over'
}

def get_prob_column_name(stat_type, line, variant='p_over'):
    """
    Constructs the column name used in SingleGamePropProbabilities.csv
    stat_type: 'goals', 'assists', 'points', 'sog', 'blocks'
    line: float (e.g. 0.5, 1.5)
    variant: 'p_over' or 'p_over_calibrated'
    """
    import numpy as np
    stat_map = {
        'goals': 'G',
        'assists': 'A',
        'points': 'PTS',
        'sog': 'SOG',
        'blocks': 'BLK',
        'blk': 'BLK'
    }
    s = stat_map.get(stat_type.lower(), 'UNK')
    # Line 0.5 -> 1plus, Line 1.5 -> 2plus
    k = int(np.floor(float(line)) + 1)
    
    suffix = ""
    if variant == 'p_over_calibrated':
        suffix = "_calibrated"
        
    return f"p_{s}_{k}plus{suffix}"

def get_production_prob_column(market, line, available_columns):
    """
    Returns the column name to use for the given market and line based on policy and availability.
    """
    # 1. Check for manual override
    force_raw = os.environ.get('DISABLE_CALIBRATION', '0') == '1'
    
    # 2. Determine target variant from policy
    if force_raw:
        target_variant = 'p_over'
    else:
        target_variant = MARKET_POLICY.get(market.upper(), 'p_over')
    
    # 3. Construct column name
    target_col = get_prob_column_name(market, line, target_variant)
    
    # 4. Fallback if not available
    if target_col in available_columns:
        return target_col
    
    # Try raw fallback if we wanted calibrated
    if target_variant == 'p_over_calibrated' or force_raw:
        raw_col = get_prob_column_name(market, line, 'p_over')
        if raw_col in available_columns:
            return raw_col
            
    return None

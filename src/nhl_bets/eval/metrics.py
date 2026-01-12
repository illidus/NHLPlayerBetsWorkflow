import numpy as np
from sklearn.metrics import log_loss, brier_score_loss

def compute_log_loss(y_true, y_prob, clip_eps=1e-15):
    """
    Standardized Log Loss calculation with clipping.
    """
    if len(y_true) == 0:
        return np.nan
    
    # Clip probabilities to avoid log(0)
    y_prob_clamped = np.clip(y_prob, clip_eps, 1 - clip_eps)
    
    # Calculate Log Loss
    # labels=[0, 1] ensures consistent handling even if y_true only contains 0s or 1s
    return log_loss(y_true, y_prob_clamped, labels=[0, 1])

def compute_brier_score(y_true, y_prob):
    """
    Standardized Brier Score calculation.
    """
    if len(y_true) == 0:
        return np.nan
    
    return brier_score_loss(y_true, y_prob)
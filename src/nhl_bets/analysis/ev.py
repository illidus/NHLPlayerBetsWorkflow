def decimal_to_implied(odds):
    """Calculates implied probability from decimal odds."""
    if odds <= 0: return 0
    return 1 / odds

def remove_vig(p1, p2):
    """
    Normalizes two probabilities to sum to 1.
    """
    total = p1 + p2
    if total == 0:
        return 0, 0
    return p1 / total, p2 / total

def calculate_ev(prob_win, decimal_odds):
    """
    Calculates Expected Value.
    EV = (Probability * Odds) - 1
    """
    return (prob_win * decimal_odds) - 1

def kelly_criterion(prob_win, decimal_odds, fraction=1.0):
    """
    Calculates Kelly Fraction.
    f* = (bp - q) / b
    b = decimal_odds - 1
    p = prob_win
    q = 1 - p
    """
    if decimal_odds <= 1:
        return 0
    
    b = decimal_odds - 1
    p = prob_win
    q = 1 - p
    
    f = (b * p - q) / b
    return max(0, f) * fraction

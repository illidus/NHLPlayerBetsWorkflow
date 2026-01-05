import numpy as np
from scipy.stats import poisson, nbinom
import math

def poisson_probability(k, lam, side='over'):
    """
    Calculates probability for a Poisson distribution.
    P(X >= k) if side='over', P(X <= k) if side='under'.
    """
    if lam <= 0:
        return 1e-6 if side == 'over' else 1.0 - 1e-6

    if side == 'over':
        # P(X >= k) = 1 - P(X <= k-1)
        res = 1 - poisson.cdf(k - 1, lam)
    elif side == 'under':
        # P(X <= k)
        res = poisson.cdf(k, lam)
    else:
        return 1e-6

    return np.clip(res, 1e-6, 1 - 1e-6)

def nbinom_probability(k, mu, alpha, side='over'):
    """
    Calculates probability for a Negative Binomial distribution.
    Parametrization:
      p = 1 / (1 + alpha * mu)
      n = 1 / alpha
    """
    if mu <= 0:
        return 1e-6 if side == 'over' else 1.0 - 1e-6
        
    if alpha is None or alpha <= 0:
        return poisson_probability(k, mu, side)
    
    n = 1.0 / alpha
    p = 1.0 / (1.0 + alpha * mu)
    
    if side == 'over':
        # P(X >= k) = 1 - P(X <= k-1)
        res = 1 - nbinom.cdf(k - 1, n, p)
    elif side == 'under':
        # P(X <= k)
        res = nbinom.cdf(k, n, p)
    else:
        return 1e-6
        
    return np.clip(res, 1e-6, 1 - 1e-6)

def calculate_poisson_probs(mu, max_k=3):
    """
    Returns dictionary {k: P(X >= k)} for k in 1..max_k.
    """
    probs = {}
    for k in range(1, max_k + 1):
        probs[k] = poisson_probability(k, mu, side='over')
    return probs

def calculate_nbinom_probs(mu, alpha, max_k=5):
    """
    Returns dictionary {k: P(X >= k)} for k in 1..max_k.
    """
    probs = {}
    for k in range(1, max_k + 1):
        probs[k] = nbinom_probability(k, mu, alpha, side='over')
    return probs

def calc_prob_from_line(line, mean, side, stat_type, alphas_dict=None):
    """
    Handles fractional line logic and distribution selection.
    alphas_dict: dict like {'SOG': 0.35, 'BLK': 0.60}
    """
    # Determine Threshold K based on Line
    if line % 1 != 0:
        if side == 'over':
            k = math.ceil(line)
        else:
            k = math.floor(line)
    else:
        k = int(line)

    st = stat_type.lower()
    
    if 'sog' in st or 'shots' in st:
        alpha = alphas_dict.get('SOG', 0.35) if alphas_dict else 0.35
        return nbinom_probability(k, mean, alpha, side)
    elif 'blk' in st or 'blocks' in st:
        alpha = alphas_dict.get('BLK', 0.60) if alphas_dict else 0.60
        return nbinom_probability(k, mean, alpha, side)
    else:
        return poisson_probability(k, mean, side)

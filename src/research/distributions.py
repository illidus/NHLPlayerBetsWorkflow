import math
from typing import Tuple

import numpy as np


def poisson_sf(k: int, mu: float) -> float:
    if mu <= 0:
        return 0.0 if k > 0 else 1.0
    cdf = 0.0
    for i in range(0, k):
        cdf += math.exp(-mu + i * math.log(mu) - math.lgamma(i + 1))
    return max(0.0, 1.0 - cdf)


def negbin_sf(k: int, mu: float, alpha: float) -> float:
    if mu <= 0:
        return 0.0 if k > 0 else 1.0
    if alpha <= 0:
        return poisson_sf(k, mu)
    r = 1.0 / alpha
    p = r / (r + mu)
    cdf = 0.0
    for i in range(0, k):
        log_pmf = (
            math.lgamma(i + r)
            - math.lgamma(r)
            - math.lgamma(i + 1)
            + r * math.log(p)
            + i * math.log(1 - p)
        )
        cdf += math.exp(log_pmf)
    return max(0.0, 1.0 - cdf)


def zero_inflated_sf(k: int, mu: float, pi: float, dist: str, alpha: float = 0.0) -> float:
    if k <= 0:
        return 1.0
    base_sf = poisson_sf(k, mu) if dist == "poisson" else negbin_sf(k, mu, alpha)
    return (1 - pi) * base_sf


def hurdle_sf(k: int, mu: float, p0: float, dist: str, alpha: float = 0.0) -> float:
    if k <= 0:
        return 1.0
    if dist == "poisson":
        base_zero = math.exp(-mu)
        base_sf = poisson_sf(k, mu)
    else:
        base_zero = negbin_pmf_zero(mu, alpha)
        base_sf = negbin_sf(k, mu, alpha)
    if base_zero >= 1.0:
        return 0.0
    truncated_sf = base_sf / (1.0 - base_zero)
    return (1 - p0) * truncated_sf


def negbin_pmf_zero(mu: float, alpha: float) -> float:
    if mu <= 0:
        return 1.0
    r = 1.0 / alpha
    p = r / (r + mu)
    return p**r


def estimate_zero_inflation_p(
    y: np.ndarray, mu: np.ndarray, dist: str, alpha: float = 0.0
) -> float:
    if len(y) == 0:
        return 0.0
    p0_obs = float(np.mean(y == 0))
    if dist == "poisson":
        p0_model = float(np.mean(np.exp(-mu)))
    else:
        p0_model = float(np.mean([negbin_pmf_zero(m, alpha) for m in mu]))
    denom = max(1e-8, 1 - p0_model)
    pi = max(0.0, min(0.8, (p0_obs - p0_model) / denom))
    return pi


def estimate_nb_alpha(y: np.ndarray) -> float:
    if len(y) == 0:
        return 0.0
    mu = float(np.mean(y))
    var = float(np.var(y, ddof=1)) if len(y) > 1 else 0.0
    if mu <= 0 or var <= mu:
        return 0.0
    return max(0.0, (var - mu) / (mu * mu))

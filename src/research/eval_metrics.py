import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.linear_model import LogisticRegression


@dataclass
class Metrics:
    n: int
    logloss: float
    brier: float
    ece10: float
    ece20: float
    slope: float
    intercept: float


def _clip_prob(p: np.ndarray) -> np.ndarray:
    eps = 1e-15
    return np.clip(p, eps, 1.0 - eps)


def _logit(p: np.ndarray) -> np.ndarray:
    p = _clip_prob(p)
    return np.log(p / (1 - p))


def ece_score(y_true: np.ndarray, y_prob: np.ndarray, n_bins: int) -> float:
    if y_true.size == 0:
        return 0.0
    bins = np.linspace(0.0, 1.0 + 1e-8, n_bins + 1)
    binids = np.digitize(y_prob, bins) - 1
    bin_sums = np.bincount(binids, weights=y_prob, minlength=n_bins)
    bin_true = np.bincount(binids, weights=y_true, minlength=n_bins)
    bin_total = np.bincount(binids, minlength=n_bins)
    nonzero = bin_total > 0
    if not np.any(nonzero):
        return 0.0
    avg_p = bin_sums[nonzero] / bin_total[nonzero]
    avg_y = bin_true[nonzero] / bin_total[nonzero]
    ece = np.sum(np.abs(avg_y - avg_p) * bin_total[nonzero]) / np.sum(bin_total)
    return float(ece)


def calibration_slope_intercept(y_true: np.ndarray, y_prob: np.ndarray) -> Tuple[float, float]:
    if len(np.unique(y_true)) < 2:
        return float("nan"), float("nan")
    x = _logit(y_prob).reshape(-1, 1)
    model = LogisticRegression(solver="lbfgs")
    model.fit(x, y_true)
    return float(model.coef_[0][0]), float(model.intercept_[0])


def compute_metrics(y_true: np.ndarray, y_prob: np.ndarray) -> Metrics:
    y_prob = _clip_prob(y_prob)
    ll = log_loss(y_true, y_prob, labels=[0, 1])
    brier = brier_score_loss(y_true, y_prob)
    ece10 = ece_score(y_true, y_prob, 10)
    ece20 = ece_score(y_true, y_prob, 20)
    slope, intercept = calibration_slope_intercept(y_true, y_prob)
    return Metrics(
        n=int(len(y_true)),
        logloss=float(ll),
        brier=float(brier),
        ece10=float(ece10),
        ece20=float(ece20),
        slope=float(slope),
        intercept=float(intercept),
    )


def reliability_bins(
    y_true: np.ndarray, y_prob: np.ndarray, n_bins: int, variant: str
) -> List[Dict[str, float]]:
    bins = np.linspace(0.0, 1.0 + 1e-8, n_bins + 1)
    binids = np.digitize(y_prob, bins) - 1
    rows: List[Dict[str, float]] = []
    for i in range(n_bins):
        mask = binids == i
        if not np.any(mask):
            continue
        count = int(np.sum(mask))
        avg_p = float(np.mean(y_prob[mask]))
        actual = float(np.mean(y_true[mask]))
        rows.append(
            {
                "bin": i,
                "bin_label": f"{bins[i]:.2f}-{bins[i+1]:.2f}",
                "bin_lo": float(bins[i]),
                "bin_hi": float(bins[i + 1]),
                "count": count,
                "variant": variant,
                "avg_p": avg_p,
                "actual_rate": actual,
                "gap": actual - avg_p,
            }
        )
    return rows


def bootstrap_logloss_delta(
    y_true: np.ndarray,
    p_a: np.ndarray,
    p_b: np.ndarray,
    n_boot: int = 200,
    seed: int = 7,
) -> Tuple[float, float, float]:
    rng = np.random.default_rng(seed)
    idx = np.arange(len(y_true))
    deltas = []
    for _ in range(n_boot):
        sample = rng.choice(idx, size=len(idx), replace=True)
        ll_a = log_loss(y_true[sample], _clip_prob(p_a[sample]), labels=[0, 1])
        ll_b = log_loss(y_true[sample], _clip_prob(p_b[sample]), labels=[0, 1])
        deltas.append(ll_b - ll_a)
    deltas = np.array(deltas)
    return float(deltas.mean()), float(np.percentile(deltas, 5)), float(np.percentile(deltas, 95))

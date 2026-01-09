
from scipy.stats import nbinom

alpha = 0.35
k = 2.0
n = 1.0 / alpha
mu = 0.4937051343950387 # from CSV mu_used
# p = 1 / (1 + alpha * mu)
p = 1.0 / (1.0 + alpha * mu)

print(f"n: {n}")
print(f"p: {p}")
print(f"k: {k}")

# P(X >= k) = 1 - P(X <= k-1)
# k-1 = 1
cdf_val = nbinom.cdf(k - 1, n, p)
prob = 1 - cdf_val

print(f"CDF(1): {cdf_val}")
print(f"Prob (1-CDF): {prob}")
print(f"CSV p_over_raw: 0.09883627035149278")

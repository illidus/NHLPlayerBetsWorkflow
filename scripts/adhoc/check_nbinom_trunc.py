
from scipy.stats import nbinom

alpha = 0.35
k = 2.0
n = 1.0 / alpha
mu = 0.4937051343950387 
p = 1.0 / (1.0 + alpha * mu)

print(f"Original n: {n}")
print(f"Original p: {p}")
print(f"Original Prob: {1 - nbinom.cdf(k - 1, n, p)}")

# Excel truncation check
n_trunc = int(n)
print(f"Truncated n: {n_trunc}")
print(f"Truncated Prob: {1 - nbinom.cdf(k - 1, n_trunc, p)}")

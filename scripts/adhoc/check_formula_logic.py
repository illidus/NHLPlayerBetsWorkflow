
import math
from scipy.special import gammaln

def excel_gammaln(x):
    return gammaln(x)

def check_excel_formula_logic():
    alpha = 0.35
    k_line = 2 # nbinom_k
    n = 1.0 / alpha
    mu = 0.4937051343950387
    p = 1.0 / (1.0 + alpha * mu)
    
    # We want 1 - CDF(k_line - 1)
    # Sum i from 0 to k_line - 1
    
    limit = k_line - 1
    cdf = 0
    for i in range(limit + 1):
        # Term(i) = EXP(GAMMALN(n+i) - GAMMALN(n) - GAMMALN(i+1)) * p^n * (1-p)^i
        term = math.exp(excel_gammaln(n+i) - excel_gammaln(n) - excel_gammaln(i+1)) * (p**n) * ((1-p)**i)
        print(f"Term {i}: {term}")
        cdf += term
        
    print(f"CDF sum: {cdf}")
    print(f"Result (1-CDF): {1-cdf}")
    print(f"Expected: 0.09883627...")

if __name__ == "__main__":
    check_excel_formula_logic()

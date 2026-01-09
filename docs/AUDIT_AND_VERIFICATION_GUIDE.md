# Auditing & Verification Best Practices

## 1. Introduction
This guide establishes the standard for validating model outputs in the NHL Player Bets workflow. It emphasizes "White-Box Verification"—ensuring that every number in the final output can be mathematically traced back to its raw data inputs using independent, external tools (like Excel or manual calculation).

## 2. The Core Philosophy: "Right-to-Left" Auditing
When auditing a bet (e.g., in `BestCandidatesFiltered.xlsx`), always work backwards from the result to the source.

**The Chain of Trust:**
1.  **Expected Value (EV):** Is it `(Prob * Decimal Odds) - 1`?
2.  **Probability (Model):** Is it derived correctly from the distribution (Poisson/Negative Binomial)?
3.  **Distribution Parameters ($n, p$ or $\mu$):** Are they calculated correctly from the Alpha and Adjusted Mean?
4.  **Adjusted Mean (`mu_used`):** Is it `Base Mean * Multipliers`?
5.  **Base Mean (`base_mu`):** Does it match the raw historical data (L10/L20/L40 averages or Corsi logic)?

## 3. Recommended Tooling: Forensic Audit Script
We have developed a specialized forensic tool (`scripts/forensics/run_topx_forensic_audit.py`) that generates a "Self-Verifying Spreadsheet".

### How to Run
```bash
python scripts/forensics/run_topx_forensic_audit.py --top-x 50
```

### What It Generates
A file named `combined_summary_with_formulas.xlsx` containing:
- **Value Columns:** The static values stored in the database.
- **Formula Columns:** Dynamic Excel formulas that re-calculate the value from its predecessors.
- **Delta Columns:** `Formula - Value`. These should always be near zero ($< 1e-9$).

### Key Formulas to Verify (Best Practices)

| Step | Component | Formula Logic | Excel Implementation Note |
| :--- | :--- | :--- | :--- |
| **1** | **Base Input** | L10 Average or Corsi-Split | `=AVERAGE(raw_log_1...raw_log_10)` |
| **2** | **Multipliers** | Apply Context Adjustments | `=base_mu * mult_opp * mult_goalie * ...` |
| **3** | **Params ($n$)** | NegBinom $n$ | `=1 / alpha` |
| **4** | **Params ($p$)** | NegBinom $p$ | `=1 / (1 + alpha * mu)` |
| **5** | **Probability** | Cumulative Distribution | Use **Gamma Summation** (see below) instead of `NEGBINOM.DIST` to handle float parameters. |
| **6** | **EV** | Expected Value | `=(Prob * Odds) - 1` |

## 4. Handling Negative Binomial in Excel (The "Gamma Trick")
Excel's standard `NEGBINOM.DIST` function **truncates** parameters to integers, which breaks accuracy for advanced models.

**Best Practice:** Use the expanded Gamma function summation for the Probability Density Function (PDF) and sum them manually for the CDF.

**Formula for $P(X=k)$:**
```excel
EXP(GAMMALN(n+k) - GAMMALN(n) - GAMMALN(k+1)) * p^n * (1-p)^k
```
*Where $n$ and $p$ are the distribution parameters derived in Step 3 & 4.*

## 5. Automated "Diff Lens"
When reviewing an audit report, look for the following flags:
- **PROJECTION_INPUT_ISSUE:** The Base Mu doesn't match the historical average. (Check: Is it a Corsi-based market? Is data missing?)
- **ODDS_JOIN_ISSUE:** The implied odds don't match the book odds. (Check: Time zone mismatch? Line movement?)
- **PROB_MATH_MATCH:** False means the stored probability deviates from the formula. (Check: Rounding errors? Model version mismatch?)

## 6. Model Governance
- **Never** manually adjust a projection without a code change.
- **Always** run the forensic audit after deploying a logic change (e.g., changing from L20 to L40 window) to confirm the chain of calculation remains unbroken.

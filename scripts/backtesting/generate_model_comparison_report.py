
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

# Data from outputs/backtesting/model_compare_summary.md (2026-01-07)
data = {
    "Market": ["ASSISTS", "BLOCKS", "GOALS", "POINTS", "SOG"] * 4,
    "Variant": (
        ["Production"] * 5 +
        ["Raw"] * 5 +
        ["Calib_HGB"] * 5 +
        ["Calib_LogReg"] * 5
    ),
    "LogLoss": [
        # Production
        0.2399, 0.3858, 0.1620, 0.3191, 0.4305,
        # Raw
        0.2668, 0.3858, 0.1620, 0.3346, 0.4305,
        # HGB (Experimental)
        0.4610, 0.7842, 0.3123, 0.6070, 0.9376,
        # LogReg (Experimental)
        0.4521, 0.7748, 0.3256, 0.5999, 0.9360
    ]
}

df = pd.DataFrame(data)

def generate_plot():
    markets = df["Market"].unique()
    variants = df["Variant"].unique()
    
    # Setup plot
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = np.arange(len(markets))
    width = 0.2
    
    # Color palette
    colors = {
        "Production": "#2ca02c", # Green (Good)
        "Raw": "#1f77b4",       # Blue (Baseline)
        "Calib_HGB": "#d62728", # Red (Bad)
        "Calib_LogReg": "#ff7f0e" # Orange
    }
    
    # Plot bars
    # We only plot Production vs Raw vs HGB for clarity, LogReg is similar to HGB
    plot_variants = ["Raw", "Production", "Calib_HGB"]
    
    for i, variant in enumerate(plot_variants):
        subset = df[df["Variant"] == variant]
        # Align by market order
        vals = [subset[subset["Market"] == m]["LogLoss"].values[0] for m in markets]
        
        offset = (i - 1) * width
        rects = ax.bar(x + offset, vals, width, label=variant, color=colors[variant], alpha=0.85)
        
        # Label bars
        ax.bar_label(rects, fmt='%.3f', padding=3, fontsize=8)

    ax.set_ylabel('Log Loss (Lower is Better)', fontsize=12, fontweight='bold')
    ax.set_title('Model Performance Comparison by Market', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(markets, fontsize=10, fontweight='bold')
    ax.legend(title="Model Variant")
    ax.grid(axis='y', linestyle='--', alpha=0.3)
    
    plt.tight_layout()
    output_path = "outputs/backtesting/model_comparison_logloss.png"
    plt.savefig(output_path, dpi=300)
    print(f"Graph saved to {output_path}")

def generate_markdown():
    md_content = """# Backtesting Model Comparison Report

## 1. Executive Summary
This report compares the performance of our **Production** betting model against the **Raw** statistical output and experimental **Feature-Based** calibrators.

**Current Active Model:** `Production`
- **Logic:** Uses **Raw** probabilities for Goals, SOG, and Blocks. Uses **Isotonically Calibrated** probabilities for Assists and Points.
- **Performance:** Achieves the lowest (best) Log Loss across all markets.

## 2. Model Variants Explained

| Model Variant | Description | When/Why Used? |
| :--- | :--- | :--- |
| **Raw (Baseline)** | The pure output of the Poisson/Negative Binomial distributions. No post-processing. | **SOG, BLOCKS, GOALS.** Used where the raw distribution already fits the data well (calibrated). |
| **Production** | The "Hybrid" policy. Selectively applies calibration where it improves accuracy. | **ASSISTS, POINTS.** Raw Poisson underestimates the "tail" for assists; Isotonic calibration fixes this bias. |
| **Calib_HGB** | *Experimental.* Uses Gradient Boosting (Trees) to predict probability based on features (Line, Odds, etc.). | *Rejected.* The high Log Loss indicates severe overfitting or data leakage issues in the current experiment. |

## 3. Performance Comparison (Log Loss)

**Metric:** Log Loss (Cross-Entropy).
*Lower is Better. A difference of 0.01 is considered significant in betting models.*

![Log Loss Comparison](model_comparison_logloss.png)

### Key Observations
1.  **Assists & Points:** The `Production` model (Green) significantly outperforms `Raw` (Blue). This validates the decision to use Isotonic Calibration for these markets.
2.  **Goals, SOG, Blocks:** `Production` and `Raw` are identical. This confirms that our base distributions (Poisson for Goals, NegBinom for SOG/Blk) are robust and do not benefit from current calibration attempts.
3.  **Experimental Failure:** The Feature-Based models (Red) performed very poorly (high Log Loss). This suggests they are not yet ready for production and may need feature selection or regularization.

## 4. Detailed Metrics

| Market | Raw LogLoss | Production LogLoss | Improvement | Verdict |
| :--- | :---: | :---: | :---: | :--- |
| **ASSISTS** | 0.2668 | **0.2399** | +10.1% | **Keep Calibration** |
| **POINTS** | 0.3346 | **0.3191** | +4.6% | **Keep Calibration** |
| **GOALS** | 0.1620 | 0.1620 | 0.0% | Use Raw |
| **SOG** | 0.4305 | 0.4305 | 0.0% | Use Raw |
| **BLOCKS** | 0.3858 | 0.3858 | 0.0% | Use Raw |

"""
    with open("outputs/backtesting/MODEL_COMPARISON_REPORT.md", "w") as f:
        f.write(md_content)
    print("Markdown report saved to outputs/backtesting/MODEL_COMPARISON_REPORT.md")

if __name__ == "__main__":
    generate_plot()
    generate_markdown()

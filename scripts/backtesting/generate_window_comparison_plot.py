
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Data from docs/experiments/2026-01-05_L40_validation.md
# Metric: Log Loss (Lower is Better)
data = {
    "Experiment": ["L40 (Stability)", "Weighted (50/50)", "Baseline (L20)", "Weighted (75/25)", "Season Long", "L5 (Recency)"],
    "LogLoss": [0.6015, 0.6679, 0.7345, 0.8053, 1.2125, 1.8793]
}

df = pd.DataFrame(data)

def generate_plot():
    # Setup plot
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Colors: Green for winner, Grey for mid, Red for loser
    colors = ['#2ca02c'] + ['#7f7f7f'] * 4 + ['#d62728']
    
    rects = ax.bar(df["Experiment"], df["LogLoss"], color=colors, alpha=0.85)
    
    # Label bars
    ax.bar_label(rects, fmt='%.4f', padding=3, fontsize=10, fontweight='bold')

    ax.set_ylabel('Log Loss (Lower is Better)', fontsize=12, fontweight='bold')
    ax.set_title('Impact of Rolling Window Size on Accuracy (Assists)', fontsize=14, fontweight='bold')
    plt.xticks(rotation=45, ha='right', fontsize=10)
    
    # Add annotation for improvement
    improvement = (1 - (0.6015 / 0.7345)) * 100
    ax.text(0, 0.50, f"-{improvement:.1f}% Error\nvs Baseline", ha='center', color='white', fontweight='bold')

    ax.grid(axis='y', linestyle='--', alpha=0.3)
    
    plt.tight_layout()
    output_path = "outputs/backtesting/window_comparison_logloss.png"
    plt.savefig(output_path, dpi=300)
    print(f"Graph saved to {output_path}")

if __name__ == "__main__":
    generate_plot()

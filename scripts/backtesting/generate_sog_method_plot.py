
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Data from docs/experiments/2026-01-05_L40_validation.md (Section 5)
# Metric: Log Loss (Lower is Better)
# Target: SOG > 2.5
data = {
    "Experiment": ["Corsi Split (L20*L40)", "Weighted (50/50)", "Baseline (SOG L20)", "Corsi * LgAvg"],
    "LogLoss": [0.4764, 0.4777, 0.4808, 0.4910]
}

df = pd.DataFrame(data)

def generate_plot():
    # Setup plot
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Colors: Green for winner, Grey for mid
    colors = ['#2ca02c'] + ['#7f7f7f'] * 3
    
    rects = ax.bar(df["Experiment"], df["LogLoss"], color=colors, alpha=0.85)
    
    # Label bars
    ax.bar_label(rects, fmt='%.4f', padding=3, fontsize=10, fontweight='bold')

    ax.set_ylabel('Log Loss (Lower is Better)', fontsize=12, fontweight='bold')
    ax.set_title('Impact of Projection Method on Accuracy (Shots on Goal)', fontsize=14, fontweight='bold')
    plt.xticks(rotation=45, ha='right', fontsize=10)
    
    # Zoom in on the y-axis to show the difference (since range is small 0.47-0.49)
    ax.set_ylim(0.470, 0.495)
    
    # Annotation
    improvement = (1 - (0.4764 / 0.4808)) * 100
    ax.text(0, 0.472, f"-{improvement:.2f}% Error\nvs Baseline", ha='center', color='white', fontweight='bold', bbox=dict(facecolor='black', alpha=0.5))

    ax.grid(axis='y', linestyle='--', alpha=0.3)
    
    plt.tight_layout()
    output_path = "outputs/backtesting/sog_method_comparison_logloss.png"
    plt.savefig(output_path, dpi=300)
    print(f"Graph saved to {output_path}")

if __name__ == "__main__":
    generate_plot()

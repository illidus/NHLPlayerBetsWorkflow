# Custom command script: /backtest:compare
# Runs baseline backtest, calibrated backtest, and then the comparison report.

Write-Host "Starting Backtest Comparison Workflow..."

# 1. Run Baseline Backtest
python pipelines/backtesting/run_ev_backtest.py --prob-source baseline --ev-threshold 0.05

# 2. Run Calibrated Backtest
python pipelines/backtesting/run_ev_backtest.py --prob-source calibrated --ev-threshold 0.05

# 3. Compare Results
python experiments/metrics/compare_baseline_vs_calibrated.py

Write-Host "Workflow Complete. Check outputs/backtest_reports/ for outputs."

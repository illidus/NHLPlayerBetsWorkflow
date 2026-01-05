# NHL Player Prop Bets Workflow

This project identifies +EV (Expected Value) NHL player prop bets by comparing bookmaker odds against enhanced statistical projections (Phase 10 Production).

## Automated Pipeline

The system is orchestrated by `pipelines/production/run_production_pipeline.py`, which performs a daily run including data syncing, feature engineering, scraping, and EV analysis.

### Core Stages:
1.  **Sync & Features**: Updates MoneyPuck data in DuckDB and rebuilds player/team/goalie features.
2.  **API Scraper**: Fetches current NHL player props directly from the PlayNow content-service JSON API.
3.  **Projection Enhancement**: Runs the Phase 8+ "Brain" model to adjust base player stats for Goalie Quality (GSAx), Team Defense (xGA60/SA60), and Game Context (B2B, Home/Away).
4.  **EV Analysis**: 
    *   Matches live props to enhanced projections.
    *   Calculates probabilities using market-appropriate distributions (**Poisson** for Goals/Assists/Points, **Negative Binomial** for SOG/Blocks).
    *   Identifies and ranks all +EV positions.

## Usage

### Prerequisites
*   Python 3.12+
*   Dependencies: `pip install -r requirements_backtesting.txt`
*   Libraries: `requests`, `pandas`, `duckdb`, `tenacity`, `openpyxl`, `scipy`.

### Running the Workflow
Execute the master pipeline from the project root:

```powershell
python pipelines/production/run_production_pipeline.py
```

### Reviewing Results
The system provides triple-channel reporting:
1.  **Console Summary**: Prints a live list of all +EV bets found.
2.  **Spreadsheet Output**: A detailed audit file is created at `outputs/ev_analysis/ev_bets_ranked.xlsx`.
3.  **Best Bets**: A filtered, prioritized list is generated at `outputs/ev_analysis/BestCandidatesFiltered.xlsx`.

## Advanced Configuration

### Accuracy Backtesting
To run accuracy metrics (Log Loss, Brier, ECE) after the main run:
```powershell
$env:RUN_ACCURACY_BACKTEST = "1"
python pipelines/production/run_production_pipeline.py
```

### Scraper Fallback
If the API scraper fails or you want to use the legacy browser-based scraper:
```powershell
$env:USE_SELENIUM_SCRAPER = "1"
python pipelines/production/run_production_pipeline.py
```

## Operational Strategy

### The "October Rule"
**WARNING:** Be extremely cautious during the first 30 days of the season. Early-season data is volatile. Consider skipping or using 0.25x unit sizes.

### Mathematical Accuracy
*   **Poisson Markets:** GOALS, ASSISTS, POINTS.
*   **Negative Binomial Markets:** SOG, BLOCKS (Alphas: SOG=0.35, BLK=0.60).
*   **Rolling Windows:** Assists/Points (L40), Goals (L10), Usage (L20).
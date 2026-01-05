import duckdb
import pandas as pd
from pathlib import Path
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

DB_PATH = "data/db/nhl_backtest.duckdb"
REPORT_DIR = "outputs/backtest_reports"

def validate_db():
    if not Path(DB_PATH).exists():
        logger.error(f"Database not found at {DB_PATH}")
        return

    con = duckdb.connect(DB_PATH)
    Path(REPORT_DIR).mkdir(parents=True, exist_ok=True)
    
    try:
        # 1. Row Counts
        logger.info("Checking row counts...")
        tables = con.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        
        row_counts = []
        for t in table_names:
            count = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            row_counts.append({"table_name": t, "row_count": count})
            logger.info(f"{t}: {count}")
            
        pd.DataFrame(row_counts).to_csv(Path(REPORT_DIR) / "duckdb_table_rowcounts.csv", index=False)
        
        # 2. Season Range
        logger.info("Checking season ranges...")
        ranges = []
        if 'fact_skater_game_situation' in table_names:
            min_s, max_s = con.execute("SELECT MIN(season), MAX(season) FROM fact_skater_game_situation").fetchone()
            ranges.append({"table": "fact_skater_game_situation", "min_season": min_s, "max_season": max_s})
        
        if 'fact_goalie_game_situation' in table_names:
            min_s, max_s = con.execute("SELECT MIN(season), MAX(season) FROM fact_goalie_game_situation").fetchone()
            ranges.append({"table": "fact_goalie_game_situation", "min_season": min_s, "max_season": max_s})
            
        # 3. Null Checks (fact_skater_game_all)
        null_metrics = {}
        if 'fact_skater_game_all' in table_names:
            total_rows = con.execute("SELECT count(*) FROM fact_skater_game_all").fetchone()[0]
            for col in ['player_id', 'game_id', 'game_date', 'goals', 'assists', 'points', 'toi_seconds']:
                null_count = con.execute(f"SELECT count(*) FROM fact_skater_game_all WHERE {col} IS NULL").fetchone()[0]
                null_metrics[f"null_{col}_pct"] = (null_count / total_rows) * 100 if total_rows > 0 else 0
        
        # 4. Cardinality Checks
        cardinality_metrics = {}
        if 'fact_skater_game_all' in table_names:
            unique_pk = con.execute("SELECT count(*) FROM (SELECT DISTINCT player_id, game_id FROM fact_skater_game_all)").fetchone()[0]
            total_rows = con.execute("SELECT count(*) FROM fact_skater_game_all").fetchone()[0]
            cardinality_metrics['skater_game_all_pk_unique_pct'] = (unique_pk / total_rows) * 100 if total_rows > 0 else 0
            
            if unique_pk != total_rows:
                logger.warning("fact_skater_game_all is not unique on (player_id, game_id)!")

        # Summary Report
        summary_data = {
            "timestamp": pd.Timestamp.now(),
            **{f"range_{r['table']}": f"{r['min_season']}-{r['max_season']}" for r in ranges},
            **null_metrics,
            **cardinality_metrics
        }
        
        # Flatten summary for CSV
        summary_list = [{"metric": k, "value": v} for k, v in summary_data.items()]
        pd.DataFrame(summary_list).to_csv(Path(REPORT_DIR) / "duckdb_ingest_summary.csv", index=False)
        logger.info("Validation reports generated.")
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        raise
    finally:
        con.close()

if __name__ == "__main__":
    validate_db()

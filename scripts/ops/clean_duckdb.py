import duckdb
from odds_archive import config

def main():
    con = duckdb.connect(str(config.ODDS_ARCHIVE_DB_PATH))
    tables = ["fact_odds_archive_props", "fact_odds_archive_pages", "fact_odds_archive_url_lake"]
    
    for table in tables:
        try:
            con.execute(f"DROP TABLE IF EXISTS {table}")
            print(f"Dropped {table}")
        except Exception as e:
            print(f"Error dropping {table}: {e}")
    
    con.close()

if __name__ == "__main__":
    main()

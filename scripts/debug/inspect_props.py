import pandas as pd
from odds_archive import config

def main():
    try:
        df = pd.read_parquet(config.PROPS_PARQUET_PATH)
        print(f"Total props: {len(df)}")
        print(df.head())
        print(df.columns)
    except Exception as e:
        print(f"Error reading parquet: {e}")

if __name__ == "__main__":
    main()

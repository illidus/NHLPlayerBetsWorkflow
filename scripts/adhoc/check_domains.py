import pandas as pd
from odds_archive import io

pd.set_option('display.max_rows', None)

def main():
    df = io.load_url_lake()
    print(df['source'].value_counts())
    print("\nStatus by Source:")
    print(df.groupby(['source', 'status']).size())

if __name__ == "__main__":
    main()


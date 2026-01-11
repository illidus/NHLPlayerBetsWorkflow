import pandas as pd\nfrom pathlib import Path\npath=Path('outputs/projections/BaseSingleGameProjections.csv')\ndf=pd.read_csv(path)\nprint(df.columns.tolist())\nprint(df.head(1).T)\n

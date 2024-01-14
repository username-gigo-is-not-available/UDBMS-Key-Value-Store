import pandas as pd

from settings import OUTPUT_PATH
from tables import TABLES_MAP
if __name__ == '__main__':

    df = pd.read_csv(OUTPUT_PATH + "output.csv")
    for table_name, table_columns in TABLES_MAP.items():
        table_records = df[table_columns].drop_duplicates()
        table_records.to_csv(OUTPUT_PATH + table_name + ".csv", index=False, header=True)

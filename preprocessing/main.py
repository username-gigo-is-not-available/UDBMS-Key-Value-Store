import logging
import time
import pandas as pd

from load import load_csv, pollutants
from transform import transform_measurement_dict
from preprocessing.extract import extract_date, extract_address
from settings import PATH_MEASUREMENT_INFO_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

pd.set_option('display.max_columns', None)

if __name__ == '__main__':
    start_time = time.time()

    result: list[dict] = []

    records = sorted(list(load_csv(path=PATH_MEASUREMENT_INFO_PATH, to_dict=True).values()),
                     key=lambda item: (item['Measurement date'], item['Station code'])
                     )

    total_records = len(records)
    batch_size = len(pollutants)

    for index in range(0, total_records - batch_size + 1, len(pollutants)):
        result.append(transform_measurement_dict(records[index:index + len(pollutants)]))
        processed_records = index + len(pollutants)
        logger.info(f"Processed {processed_records}/{total_records} records")

    df: pd.DataFrame = pd.DataFrame(result)
    logger.info("Extracting date and address columns")
    df = df.join(df['Measurement date'].apply(extract_date).apply(pd.Series)) \
        .join(df["Address"].apply(extract_address).apply(pd.Series))
    logger.info("Finished extracting date and address columns")
    end_time = time.time()
    total_time_elapsed = end_time - start_time
    logger.info(f"Total time elapsed: {total_time_elapsed:.2f} seconds")

    logger.info("Columns in DataFrame:")
    logger.info(df.columns)
    logger.info("\nDataFrame:\n")
    logger.info(df)

    df.to_csv("./data/AirPollutionSeoul/preprocessed.csv")

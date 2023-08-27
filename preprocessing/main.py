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

    records = sorted(list(load_csv(path=PATH_MEASUREMENT_INFO_PATH, to_dict=True, n_rows=200).values()),
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

    redundant_columns = ['Measurement date', 'Minute', 'Address', 'City', 'Country', 'Station name(district)']
    logger.info(f"Dropping columns: {redundant_columns}")
    df.drop(columns=redundant_columns, inplace=True)
    column_order = ['Timestamp', 'Year', 'Month', 'Day', 'Hour', 'Station code', 'House number', 'Street name',
                    'Station name/District', 'Latitude', 'Longitude',
                    'SO2', 'SO2 instrument status code', 'SO2 instrument status', 'SO2 pollutant code',
                    'SO2 class code', 'SO2 class', 'SO2 unit of measurement id', 'SO2 unit of measurement',
                    'NO2', 'NO2 instrument status code', 'NO2 instrument status', 'NO2 pollutant code',
                    'NO2 class code', 'NO2 class', 'NO2 unit of measurement id', 'NO2 unit of measurement',
                    'O3', 'O3 instrument status code', 'O3 instrument status', 'O3 pollutant code', 'O3 class code',
                    'O3 class', 'O3 unit of measurement id', 'O3 unit of measurement',
                    'CO', 'CO instrument status code', 'CO instrument status', 'CO pollutant code', 'CO class code',
                    'CO class', 'CO unit of measurement id', 'CO unit of measurement',
                    'PM10', 'PM10 instrument status code', 'PM10 instrument status', 'PM10 pollutant code',
                    'PM10 class code', 'PM10 class', 'PM10 unit of measurement id', 'PM10 unit of measurement',
                    'PM2.5', 'PM2.5 instrument status code', 'PM2.5 instrument status', 'PM2.5 pollutant code',
                    'PM2.5 class code', 'PM2.5 class', 'PM2.5 unit of measurement id', 'PM2.5 unit of measurement']
    logger.info(f"Ordering columns: ")
    df = df[column_order]
    end_time = time.time()
    total_time_elapsed = end_time - start_time
    logger.info(f"Total time elapsed: {total_time_elapsed:.2f} seconds")

    logger.info("Columns in DataFrame:")
    logger.info(list(df.columns))
    logger.info("\nDataFrame:\n")
    logger.info(df)

    # df.to_csv("./data/AirPollutionSeoul/preprocessed.csv")

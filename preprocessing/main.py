import logging
import os
import time

import pandas as pd

from preprocessing.etl import etl_measurement_info, etl_measurement_item_info, etl_measurement_station_info
from preprocessing.transform import classify_pollutant_row
from preprocessing.log import get_logger
from preprocessing.static import COLUMN_ORDER
from settings import OUTPUT_PATH

pd.set_option('display.max_columns', None)

if __name__ == '__main__':
    logger = get_logger('main')
    start_time = time.time()
    df_station_info, df_item_info, df_info = etl_measurement_station_info(), etl_measurement_item_info(), etl_measurement_info()
    logger.info("Joining measurement info dataset with station info and item info")
    result = pd.merge(df_info, df_station_info, on='Station code', how='inner').merge(df_item_info, on='Pollutant code', how='inner')
    logger.info("Classifying pollutant values")
    result['Pollutant class'] = result.apply(classify_pollutant_row, axis=1)
    logger.info("Reordering columns")
    result = result[COLUMN_ORDER]
    logger.info("Finished joining measurement info dataset with station info and item info")
    logger.info(f"Finished in {round(time.time() - start_time, 5)} seconds")
    logger.info("Writing result to csv")
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    result.to_csv(OUTPUT_PATH, index=False, header=True)
    logger.info("Finished writing result to csv")

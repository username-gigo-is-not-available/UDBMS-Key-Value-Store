import pandas as pd

from preprocessing.extract import extract_address, extract_date
from preprocessing.load import load_csv
from preprocessing.log import get_logger
from settings import MEASUREMENT_STATION_INFO_PATH, MEASUREMENT_ITEM_INFO_PATH, MEASUREMENT_INFO_PATH
from preprocessing.static import POLLUTANT_MAP, UNIT_OF_MEASUREMENT_MAP, INSTRUMENT_STATUS_MAP


def etl_measurement_station_info() -> pd.DataFrame:
    logger = get_logger('etl_measurement_station_info')
    logger.info("Loading measurement station info dataset")
    logger.info("Renaming columns of measurement station info dataset")

    # Load CSV and rename columns
    df_station_info: pd.DataFrame = (
        load_csv(path=MEASUREMENT_STATION_INFO_PATH, index_col='Station code')
        .rename(columns={'Station name(district)': 'Station name/District'})
    )

    # Extract composite attribute address to separate columns (House number, Street name, District name).
    # Drop redundant columns (City, Country), since they are the same for all records.
    logger.info("Extracting address from measurement station info dataset")
    df_station_info_extracted = df_station_info['Address'].apply(extract_address).apply(pd.Series)

    # Join extracted address columns to measurement station info dataset, drop Address column since it is redundant.
    logger.info("Joining extracted address columns to measurement station info dataset")
    df_station_info = df_station_info.join(df_station_info_extracted, how='inner').drop(columns=['Address'])

    logger.info("Finished processing of measurement station info dataset")

    return df_station_info


def etl_measurement_item_info() -> pd.DataFrame:
    logger = get_logger('etl_measurement_item_info')
    logger.info("Loading measurement item info dataset")
    logger.info("Renaming columns of measurement item info dataset")

    # Load CSV and rename columns
    df_item_info: pd.DataFrame = (
        load_csv(path=MEASUREMENT_ITEM_INFO_PATH, index_col='Item code')
        .rename(columns={'Item name': 'Pollutant name', 'Item code': 'Pollutant code'})
    )

    # Add surrogate ids for unit of measurement to measurement item info dataset.
    logger.info("Adding surrogate ids for unit of measurement to measurement item info dataset")
    df_unit_of_measurements = pd.DataFrame(list(UNIT_OF_MEASUREMENT_MAP.items()),
                                           columns=['Unit of measurement code', 'Unit of measurement'])

    # Join unit of measurement info to measurement item info dataset
    logger.info("Joining unit of measurement info to measurement item info dataset")
    df_item_info = pd.merge(df_item_info, df_unit_of_measurements, on='Unit of measurement')

    # Add original pollutant ids to measurement item info dataset
    logger.info("Adding original pollutant ids to measurement item info dataset")
    df_item_ids = pd.DataFrame(list(POLLUTANT_MAP.items()), columns=['Pollutant code', 'Pollutant name'])

    # Join original pollutant ids to measurement item info dataset and set the index
    logger.info("Joining original pollutant ids to measurement item info dataset")
    df_item_info = pd.merge(df_item_info, df_item_ids, on='Pollutant name').set_index('Pollutant code')

    return df_item_info


def etl_measurement_info() -> pd.DataFrame:
    logger = get_logger('etl_measurement_measurement_info')
    logger.info("Loading measurement info dataset")
    logger.info("Renaming columns of measurement info dataset")

    # Load CSV and rename columns
    df_info: pd.DataFrame = load_csv(path=MEASUREMENT_INFO_PATH).rename(
        columns={'Item code': 'Pollutant code', 'Instrument status': 'Instrument status code'})

    # Add original instrument status classes to measurement info dataset.
    logger.info("Joining measurement info dataset instrument status info")
    df_instrument_statuses = pd.DataFrame(list(INSTRUMENT_STATUS_MAP.items()),
                                          columns=['Instrument status code', 'Instrument status'])

    # Join instrument status info to measurement info dataset
    logger.info("Joining measurement info dataset instrument status info")
    df_info = pd.merge(df_info, df_instrument_statuses, on='Instrument status code', how='inner')

    # Extract composite attribute Measurement date to separate columns (Timestamp, Year, Month, Day, Minute).
    # Drop redundant column (Seconds, since it's the same for all records).
    logger.info("Extracting date from measurement info dataset")
    df_info_extracted = pd.DataFrame(df_info['Measurement date'].unique(), columns=['Measurement date'])[
        'Measurement date'].apply(extract_date).apply(pd.Series)

    # Join extracted date columns to measurement info dataset and drop original Measurement date column,
    # drop Measurement date column since it is redundant.
    logger.info("Joining extracted date columns to measurement info dataset")
    df_info = pd.merge(df_info, df_info_extracted, on='Measurement date', how='inner').drop(
        columns=['Measurement date'])

    logger.info("Finished processing of measurement info dataset")
    return df_info

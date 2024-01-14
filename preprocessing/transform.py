import pandas as pd

from preprocessing.static import POLLUTANT_CLASSES


def classify_pollutant_row(row: pd.Series) -> str:
    average_value = row['Average value']
    for pollutant_class in POLLUTANT_CLASSES:
        if average_value <= row[pollutant_class]:
            return pollutant_class
    return POLLUTANT_CLASSES[-1]
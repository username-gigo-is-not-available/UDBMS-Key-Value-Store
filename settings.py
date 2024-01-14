import os

from dotenv import load_dotenv

load_dotenv()

DATASET_PATH = os.getenv("DATASET_PATH")
MEASUREMENT_INFO_PATH = "/".join([DATASET_PATH, "Measurement_info.csv"])
MEASUREMENT_ITEM_INFO_PATH = "/".join([DATASET_PATH, "Measurement_item_info.csv"])
MEASUREMENT_STATION_INFO_PATH = "/".join([DATASET_PATH, "Measurement_station_info.csv"])
OUTPUT_PATH = os.getenv("OUTPUT_PATH")
OUTPUT_FILE_PATH = "/".join([OUTPUT_PATH, "output.csv"])

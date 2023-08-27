import re
from preprocessing.load import pollutants_dict, instruments_dict, pollutants, \
    units_of_measurement_dict, load_csv
from preprocessing.settings import PATH_MEASUREMENT_ITEM_INFO_PATH, PATH_MEASUREMENT_INFO_PATH, \
    PATH_MEASUREMENT_STATION_INFO_PATH


def transform_pollutant_dict(data: dict[int, dict[str, str]]) -> dict:
    result: dict = {}
    redundant_columns: list[str] = ["Item name"]
    for pollutant_dict in list(data.values()):
        pollutant_name = pollutant_dict['Item name']
        for column in redundant_columns:
            pollutant_dict.pop(column)
        result[pollutant_name] = {value: key for key, value in pollutant_dict.items()}
    return result


def transform_measurement_dict(data: list[dict]) -> dict:
    if not can_transpose_pollutant_rows(data=data):
        print(data)
        raise Exception(f"Argument is null or does not contain valid rows!")

    result = {
        'Measurement date': data[0]['Measurement date'],
        'Station code': data[0]['Station code'],
    }

    station_code = result['Station code']
    for row in data:
        pollutant_column_name = pollutants_dict[row['Item code']]
        result[f'{pollutant_column_name}'] = row['Average value']
        result[f'{pollutant_column_name} instrument status code'] = row['Instrument status']
        result[f'{pollutant_column_name} instrument status'] = instruments_dict[row['Instrument status']]
        result[f'{pollutant_column_name} pollutant code'] = row['Item code']
        result[f'{pollutant_column_name} class code'], result[
            f'{pollutant_column_name} class'] = calculate_pollutant_class(
            pollutant_name=pollutant_column_name, pollutant_value=result[pollutant_column_name])
        result[f'{pollutant_column_name} unit of measurement id'] = 1 if pollutant_column_name.startswith('PM') else 0
        result[f'{pollutant_column_name} unit of measurement'] = units_of_measurement_dict[
            result[f'{pollutant_column_name} unit of measurement id']]

        result = {**result, **stations_data[station_code]}

    return result


def can_transpose_pollutant_rows(data: list[dict]) -> bool:
    if data:
        return all(row['Measurement date'] == data[0]['Measurement date'] for row in data) and all(
            row['Station code'] == data[0]['Station code'] for row in data)
    return False


def calculate_pollutant_class(pollutant_name: str, pollutant_value: float) -> tuple[int, str]:
    if pollutant_name not in pollutants:
        raise Exception(f"Column {pollutant_name} is not a valid pollutant!")

    boundary_dict = {k: v for k, v in pollutants_data[pollutant_name].items() if
                     k not in units_of_measurement_dict.values()}

    for index, (boundary, class_name) in enumerate(boundary_dict.items()):
        if pollutant_value <= boundary:
            return index, class_name

    return len(list(boundary_dict.values())) - 1, list(boundary_dict.values())[-1]


pollutants_data: dict = transform_pollutant_dict(data=load_csv(path=PATH_MEASUREMENT_ITEM_INFO_PATH,
                                                               index_col="Item code",
                                                               to_dict=True))

measurements_data: dict = load_csv(path=PATH_MEASUREMENT_INFO_PATH, to_dict=True)

stations_data: dict = load_csv(path=PATH_MEASUREMENT_STATION_INFO_PATH, index_col='Station code', to_dict=True)

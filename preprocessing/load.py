import pandas as pd

pollutants_dict: dict[int, str] = {
    1: "SO2",
    3: "NO2",
    5: "O3",
    6: "CO",
    8: "PM10",
    9: "PM2.5"
}
pollutants: list[str] = list(pollutants_dict.values())
instruments_dict: dict[int, str] = {
    0: "Normal",
    1: "Need for calibration",
    2: "Abnormal",
    4: "Power cut off",
    8: "Under repair",
    9: "Abnormal data"
}

units_of_measurement = ['ppm', 'Mircrogram/m3']


def load_csv(path: str, index_col: str | None = None, to_dict: bool = False,
             n_rows: int | None = None) -> pd.DataFrame | dict | list[dict]:
    df = pd.read_csv(filepath_or_buffer=path, header=0, encoding='utf-8',
                     index_col=index_col, nrows=n_rows)
    if to_dict and df.index.is_unique:
        return df.to_dict(orient='index')
    elif to_dict and not df.index.is_unique:
        return df.to_dict(orient='records')
    else:
        return df





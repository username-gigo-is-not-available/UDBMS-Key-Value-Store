POLLUTANT_MAP: dict[int, str] = {
    1: "SO2",
    3: "NO2",
    5: "O3",
    6: "CO",
    8: "PM10",
    9: "PM2.5"
}

INSTRUMENT_STATUS_MAP: dict[int, str] = {
    0: "Normal",
    1: "Need for calibration",
    2: "Abnormal",
    4: "Power cut off",
    8: "Under repair",
    9: "Abnormal data"
}

UNIT_OF_MEASUREMENT_MAP = {
    0: 'ppm',
    1: 'Mircrogram/m3'
}

COLUMN_ORDER = ['Timestamp', 'Year', 'Month', 'Day', 'Hour', 'Station code', 'Station name/District', 'House number',
                'Street name',
                'Latitude', 'Longitude', 'Instrument status', 'Instrument status code',
                'Pollutant code', 'Pollutant name', 'Unit of measurement code', 'Unit of measurement',
                'Good(Blue)', 'Normal(Green)', 'Bad(Yellow)', 'Very bad(Red)']

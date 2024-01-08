from datetime import datetime


def extract_date(value: str) -> dict[str, int]:
    datetime_obj = datetime.strptime(value, "%Y-%m-%d %H:%M")
    # Drop redundant columns (Minute), since they are the same for all records.
    return {
        "Measurement date": value,
        "Timestamp": int(datetime_obj.timestamp()),
        "Year": datetime_obj.year,
        "Month": datetime_obj.month,
        "Day": datetime_obj.day,
        "Hour": datetime_obj.hour,
    }


def extract_address(value: str) -> dict[str, int | str]:
    address_parts = [part.strip() for part in value.split(",")]
    # Drop redundant columns (City, Country), since they are the same for all records.
    return {
        "House number": address_parts[0],
        "Street name": address_parts[1],
    }

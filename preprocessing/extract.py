from datetime import datetime


def extract_date(value: str) -> dict[str, int]:
    datetime_obj = datetime.strptime(value, "%Y-%m-%d %H:%M")
    return {
        "Timestamp": int(datetime_obj.timestamp()),
        "Year": datetime_obj.year,
        "Month": datetime_obj.month,
        "Day": datetime_obj.day,
        "Hour": datetime_obj.hour,
        "Minute": datetime_obj.minute,
    }


def extract_address(value: str) -> dict[str, int | str]:
    address_parts = [part.strip() for part in value.split(",")]
    return {
        "House number": address_parts[0],
        "Street name": address_parts[1],
        "Station name/District": address_parts[2],
        "City": address_parts[3],
        "Country": address_parts[4]
    }

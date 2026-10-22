from datetime import timedelta, datetime


def parse_duration(duration_str):
    """Convert an ISO 8601 duration string into a Python timedelta.

    Args:
        duration_str (str): Duration returned by the YouTube API,
        for example 'PT1H2M30S'.

        Returns:
            timedelta: The duration expressed in days, hours,
            minutes, and seconds.
            """
    # Remove the ISO 8601 duration markers.
    duration_str = duration_str.replace("P", "").replace("T", "")

    # Initialize the supported duration components.
    components = ["D", "H", "M", "S"]
    values = {"D": 0, "H": 0, "M": 0, "S": 0}

    # Extract each component and convert its value to an integer.
    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component)
            values[component] = int(value)

            # Build a timedelta object from the extracted components.
            total_duration = timedelta(
                days=values["D"], hours=values["H"], minutes=values["M"], seconds=values["S"]
            )

            return total_duration


def transform_data(row):
    """Transform a YouTube video record for the refined database.

    Converts the video duration into a time object and assigns
    a video type based on the duration.

    Args:
        row (dict): A dictionary containing YouTube video data.

        Returns:
            dict: The transformed video record.
            """
    # Convert the ISO 8601 duration into a timedelta.
    duration_td = parse_duration(row["Duration"])

    # Convert the duration into a Python time object.
    row["Duration"] = (datetime.min + duration_td).time()

    # Classify videos according to the duration threshold.
    row["Video_Type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal"

    return row
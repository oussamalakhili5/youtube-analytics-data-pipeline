import json
import logging
from datetime import date

logger = logging.getLogger(__name__)


def load_data():
    """Load YouTube video data from the JSON file generated today.

    Returns:
        list: The extracted YouTube video records.

        Raises:
            FileNotFoundError: If the JSON file does not exist.
            json.JSONDecodeError: If the file contains invalid JSON.
            """
    file_path = f"./data/youtube_video_data_{date.today()}.json"

    try:
        logger.info(f"Loading data from: {file_path}")

        with open(file_path, "r", encoding="utf-8") as raw_data:
            data = json.load(raw_data)

            logger.info(f"Successfully loaded {len(data)} video records")
            return data

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file: {file_path}")
        raise
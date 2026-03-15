import pandas as pd
import numpy as np
import os
import json
from datetime import datetime, timezone
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = os.getcwd()
RAW_DIR = os.path.join(BASE_DIR, 'data', 'raw')
PROCESSED_DIR = os.path.join(BASE_DIR, 'data', 'processed')
os.makedirs(PROCESSED_DIR, exist_ok=True)
METADATA_DIR = os.path.join(BASE_DIR, "data", "metadata")

def main():
    try:

        with open(f"{METADATA_DIR}/extract_context.json", "r") as f:
            context = json.load(f)

        year = context["accidents_year"]

        with open(f"{RAW_DIR}/weather_bcn_{year}.json", "r") as f:
            weather_raw = json.load(f)

        start = pd.to_datetime(weather_raw["start_time"], unit="s", utc=True)
        end = pd.to_datetime(weather_raw["end_time"], unit="s", utc=True)

        dates = pd.date_range(
            start=start,
            end=end,
            freq=f"{weather_raw['interval_seconds']}s",
            inclusive="left"
        )

        weather = pd.DataFrame({
            "datetime": dates,
            "temperature_2m": weather_raw["hourly"]["temperature_2m"],
            "precipitation": weather_raw["hourly"]["precipitation"],
            "weathercode": weather_raw["hourly"]["weathercode"],
            "windspeed_10m": weather_raw["hourly"]["windspeed_10m"]
        })
        weather = weather.astype({
            "temperature_2m": "float32",
            "precipitation": "float32",
            "windspeed_10m": "float32",
            "weathercode": "int16"
        })
        assert weather["datetime"].is_monotonic_increasing
        assert weather["temperature_2m"].between(-50, 60).all()
        assert weather["windspeed_10m"].ge(0).all()

        n = len(dates)
        assert len(weather_raw["hourly"]["temperature_2m"]) == n
        assert len(weather_raw["hourly"]["precipitation"]) == n
        assert len(weather_raw["hourly"]["weathercode"]) == n
        assert len(weather_raw["hourly"]["windspeed_10m"]) == n

        weather.to_parquet(f"{PROCESSED_DIR}/weather_bcn_{year}.parquet", index=False)
        logger.info(f"weather_bcn_{year} processed correctly")

        transform_context = {
            "year": year,
            "rows": len(weather),
            "processed_at": datetime.now(timezone.utc).isoformat()
        }

        with open(f"{METADATA_DIR}/transform_context.json", "w") as f:
            json.dump(transform_context, f, indent=2)
            
    except FileNotFoundError as e:
        raise RuntimeError(f"File not found: {e}")
    except KeyError as e:
        raise RuntimeError(f"Column not found: {e}")
    except ValueError as e:
        raise RuntimeError(f"Value error: {e}")
    except Exception as e:
        raise RuntimeError(f"Weather transform failed: {e}")


if __name__ == "__main__":
    main()
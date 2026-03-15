import openmeteo_requests
import requests
import requests_cache
import os
import json
from retry_requests import retry
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#Read metadata context
BASE_DIR = os.getcwd()
METADATA_DIR = os.path.join(BASE_DIR, "data", "metadata")

def main():
    try:
        with open(f"{METADATA_DIR}/extract_context.json", "r") as f:
            context = json.load(f)

    except FileNotFoundError:
        raise RuntimeError("extract_context.json not found. Run accidents extract first.")

    year = context["accidents_year"]

    start_date = datetime(year, 1, 1).strftime("%Y-%m-%d")
    end_date = datetime(year, 12, 31).strftime("%Y-%m-%d")
    OUTPUT_DIR = os.path.join(BASE_DIR, "data", "raw")

    # setup the open-meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries= 5, backoff_factor= 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # data about weather
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 41.38,
        "longitude": 2.15,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,precipitation,weathercode,windspeed_10m",
        "timezone": "Europe/Madrid"
    }

    try:
        response = openmeteo.weather_api(url, params=params)[0]

    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Weather API request failed: {e}")

    except IndexError:
        raise RuntimeError("Weather API returned empty response")

    try:
        hourly = response.Hourly()

        # Process hourly data.
        raw_weather = {
            "start_time": hourly.Time(),
            "end_time": hourly.TimeEnd(),
            "interval_seconds": hourly.Interval(),
            "hourly": {
                "temperature_2m": hourly.Variables(0).ValuesAsNumpy().tolist(),
                "precipitation": hourly.Variables(1).ValuesAsNumpy().tolist(),
                "weathercode": hourly.Variables(2).ValuesAsNumpy().tolist(),
                "windspeed_10m": hourly.Variables(3).ValuesAsNumpy().tolist()
            }
        }

    except Exception as e:
        raise RuntimeError(f"Failed processing weather hourly data: {e}")

    output_path_hd = os.path.join(
                OUTPUT_DIR, f"weather_bcn_{year}.json"
            )

    try:
        with open(output_path_hd, "w", encoding="utf-8") as f:
            json.dump(raw_weather, f, ensure_ascii=False, indent=2)

    except OSError as e:
        raise RuntimeError(f"Failed writing weather file: {e}")

    context["weather_extracted_at"] = datetime.now().isoformat()
    with open(f"{METADATA_DIR}/extract_context.json", "w") as f:
        json.dump(context, f, indent=2)

    logger.info(f"Save on {output_path_hd}")

if __name__ == "__main__":
    main()
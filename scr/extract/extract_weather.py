import openmeteo_requests
import requests
import requests_cache
import os
import json
from retry_requests import retry
from datetime import datetime

#Read metadata context
with open("../../data/metadata/extract_context.json", "r") as f:
    context = json.load(f)

year = context["accidents_year"]

start_date = datetime(year, 1, 1).strftime("%Y-%m-%d")
end_date = datetime(year, 12, 31).strftime("%Y-%m-%d")
OUTPUT_DIR = "../../data/raw/"

# setup the open-meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries= 5, backoff_factor= 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

url = "https://archive-api.open-meteo.com/v1/archive"
params = {
	"latitude": 41.38,
	"longitude": 2.15,
    "start_date": start_date,
    "end_date": end_date,
	"hourly": "temperature_2m,precipitation,weathercode,windspeed_10m",
    "timezone": "Europe/Madrid"
}

response = openmeteo.weather_api(url, params=params)[0]

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

output_path_hd = os.path.join(
            OUTPUT_DIR, f"weather_bcn_{year}.json"
        )

with open(output_path_hd, "w", encoding="utf-8") as f:
            json.dump(raw_weather, f, ensure_ascii=False, indent=2)

print(f"Save on {output_path_hd}")
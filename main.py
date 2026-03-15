from datetime import datetime
import json, os
from src.extract.extract_accidents import main as extract_accidents
from src.extract.extract_weather import main as extract_weather
from src.transform.transform_accidents import main as transform_accidents
from src.transform.transform_weather import main as transform_weather

# Firts year
extract_accidents()
extract_weather()
transform_accidents()
transform_weather()

# Second years

METADATA_DIR = os.path.join(os.getcwd(), "data", "metadata")
with open(f"{METADATA_DIR}/extract_context.json") as f:
    first_year = json.load(f)["accidents_year"]

extract_accidents(year=first_year - 1)
extract_weather()
transform_accidents()
transform_weather()
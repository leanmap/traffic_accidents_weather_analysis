import os
import json
from datetime import datetime
import requests

BASE_DIR = os.getcwd()
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "raw")
METADATA_DIR = os.path.join(BASE_DIR, "data", "metadata")
CURRENT_YEAR = datetime.now().year
YEARS_TO_TRY = [CURRENT_YEAR, CURRENT_YEAR - 1, CURRENT_YEAR - 2]

def accidents_for_year(year):

    url = "https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search"
    limit = 1000  
    offset = 0
    all_records = []
    while True:
        params = {
            "resource_id": "8cfddcbe-3403-4a6c-8897-c13238da900e",
            "filters": json.dumps({"Nk_Any": str(year)}),
            "limit": limit,
            "offset": offset
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        records = data["result"]["records"]

        if not records:
            break

        all_records.extend(records)
        offset += limit

    return all_records

os.makedirs(OUTPUT_DIR, exist_ok=True)

for year in YEARS_TO_TRY:
    print(f"Looking for year {year}...")
    records = accidents_for_year(year)

    if records:

        print(f"For {year}: {len(records)} records")

        output_path_acc = os.path.join(
            OUTPUT_DIR, f"accidents_bcn_{year}.json"
        )

        with open(output_path_acc, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        print(f"Save on {output_path_acc}")
        break
else:
    raise RuntimeError("Don't find anything data for this year")

# save year context
os.makedirs(METADATA_DIR, exist_ok=True)

context = {
    "accidents_year": year,
    "extracted_at": datetime.now().year
}

with open(os.path.join(METADATA_DIR, "extract_context.json"), "w") as f:
    json.dump(context, f, indent=2)

    print(f"Save context year on {output_path_acc}")
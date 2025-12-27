import os
import json
from datetime import datetime
import requests

OUTPUT_DIR = "../../data/raw/"
CURRENT_YEAR = datetime.now().year
YEARS_TO_TRY = [CURRENT_YEAR, CURRENT_YEAR - 1, CURRENT_YEAR - 2]
METADATA_DIR = "../../data/metadata"

def accidents_for_year(year):

    url = "https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search"

    params = {
        "resource_id": "8cfddcbe-3403-4a6c-8897-c13238da900e",
        "filters": json.dumps({"Nk_Any": str(year)})
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()

    records = data["result"]["records"]

    return records

os.makedirs(OUTPUT_DIR, exist_ok=True)

for year in YEARS_TO_TRY:
    print(f"Looking for year {year}...")
    records = accidents_for_year(year)

    if records:

        print(f"F {year}: {len(records)} registros")

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
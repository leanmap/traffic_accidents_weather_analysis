import os
import json
from datetime import datetime
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


BASE_DIR = os.getcwd()
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "raw")
METADATA_DIR = os.path.join(BASE_DIR, "data", "metadata")
CURRENT_YEAR = datetime.now().year
YEARS_TO_TRY = [CURRENT_YEAR, CURRENT_YEAR - 1, CURRENT_YEAR - 2]

def get_resource_id(year):
    r = requests.get(
        "https://opendata-ajuntament.barcelona.cat/data/api/action/package_show",
        params={"id": "accidents-causes-gu-bcn"}
    )

    for resource in r.json()["result"]["resources"]:
        if str(year) in resource["name"].lower():
            return resource["id"]
    
    raise RuntimeError(f"No resource found for year {year}")

def accidents_for_year(year=None):

    """
    Fetch all accident records for a given year from Barcelona Open Data API.
    Returns a list of records (dicts).

    """
    url = "https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search"
    resource_id = get_resource_id(year)
    limit = 1000  
    offset = 0
    all_records = []
    while True:
        params = {
            "resource_id": resource_id,
            "limit": limit,
            "offset": offset
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        try:
            records = data["result"]["records"]
        except KeyError as e:
            raise RuntimeError(f"Unexpected API response structure: {e}")
        
        if not records:
            break

        all_records.extend(records)
        offset += limit

    return all_records

def main(year=None):

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    years_to_try = [year] if year else YEARS_TO_TRY

    try:
        for current_year in years_to_try:
            logger.info(f"Looking for year {current_year}...")
            try:
                records = accidents_for_year(current_year)
            except RuntimeError as e:
                logger.warning(f"Skipping year {current_year}: {e}")
                continue
            except requests.exceptions.HTTPError as e:
                logger.warning(f"Skipping year {current_year}: {e}")
                continue

            if records:

                logger.info(f"For {current_year}: {len(records)} records")

                output_path_acc = os.path.join(
                    OUTPUT_DIR, f"accidents_bcn_{current_year}.json"
                )

                with open(output_path_acc, "w", encoding="utf-8") as f:
                    json.dump(records, f, ensure_ascii=False, indent=2)

                logger.info(f"Save on {output_path_acc}")
                break
        else:
            raise RuntimeError("No accident data found for any year")

        # save year context
        os.makedirs(METADATA_DIR, exist_ok=True)

        context = {
            "accidents_year": current_year,
            "extracted_at": datetime.now().isoformat()
        }

        with open(os.path.join(METADATA_DIR, "extract_context.json"), "w") as f:
            json.dump(context, f, indent=2)

        logger.info(f"Save context year on {output_path_acc}")


    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"API request failed: {e}")

    except Exception as e:
        raise RuntimeError(f"Extract pipeline failed: {e}")
    
if __name__ == "__main__":
    main()
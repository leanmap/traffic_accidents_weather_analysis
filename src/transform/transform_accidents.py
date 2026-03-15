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

        accidents = pd.read_json(f"{RAW_DIR}/accidents_bcn_{year}.json")

        # Column names vary between years (e.g. 'Nk_Any' in 2024, 'NK_Any' in 2025)
        # Both are included to ensure compatibility across datasets
        accidents = accidents.rename(columns={"_id": "id",
                                            "Codi_districte": "district_code",
                                            "Nom_districte": "district_name",
                                            "Descripcio_torn": "shift",
                                            "Descripcio_causa_mediata": "immediate_cause_description",
                                            "Codi_carrer": "street_code",
                                            "Nom_carrer": "street_name",
                                            "Nom_barri": "neighborhood_name",
                                            "Codi_barri": "neighborhood_code",
                                            "Número_expedient": "case_number",
                                            "Numero_expedient": "case_number",
                                            "Coordenada_UTM_X_ED50": "coord_utm_easting_ed50",
                                            "Coordenada_UTM_Y_ED50": "coord_utm_northing_ed50",
                                            "Latitud_WGS84": "latitude_wgs84",
                                            "Longitud_WGS84": "longitude_wgs84",
                                            "Nk_Any": "year",
                                            "NK_Any": "year",
                                            "Mes_any": "month",
                                            "Nom_mes": "month_name",
                                            "Dia_mes": "day",
                                            "Hora_dia" : "hour",
                                            "Descripcio_dia_setmana": "day_name",
                                            "Num_postal": "postal_code"                                   
        })
        mapping_days = {
            "Diumenge":"Sunday",
            "Dilluns": "Monday",
            "Dimarts": "Tuesday",
            "Dimecres": "Wednesday",
            "Dijous": "Thursday",
            "Divendres": "Friday",
            "Dissabte": "Saturday"
            
        }

        accidents["day_name"] = accidents["day_name"].replace(mapping_days)

        mapping_month = {
            "Gener":"January",
            "Febrer": "February",
            "Març": "March",
            "Abril": "April",
            "Maig": "May",
            "Juny": "June",
            "Juliol": "July",
            "Agost": "August",
            "Setembre": "September",
            "Octubre": "October",
            "Novembre": "November",
            "Desembre": "December",
        }

        accidents["month_name"] = accidents["month_name"].replace(mapping_month)
        mapping_shift = {
            "Matí":"Morning",
            "Tarda": "Afternoon",
            "Nit": "Night",
        }

        accidents["shift"] = accidents["shift"].replace(mapping_shift)

        mapping_cause = {
            "Manca d'atenció a la conducció":"Lack of driver attention",
            "Desobeir semàfor": "Traffic light violation",
            "Manca precaució efectuar marxa enrera": "Lack of caution when reversing",
            "": "Unknown",
            "Altres": "Others",
            "No determinada": "Undetermined",
            "Avançament defectuós/improcedent": "Improper or unsafe overtaking",
            "Canvi de carril sense precaució": "Unsafe lane change",
            "No respectat pas de vianants": "Failure to yield to pedestrians",
            "Desobeir altres senyals": "Traffic sign violation",
            "Gir indegut o sense precaució": "Improper or unsafe turn",
            "Fallada mecànica o avaria": "Mechanical failure",
            "No cedir la dreta": "Failure to yield right of way",
            "Manca precaució incorporació circulació": "Unsafe merging into traffic",
            "Envair calçada contrària": "Driving into oncoming traffic"
        }

        accidents["immediate_cause_description"] = accidents["immediate_cause_description"].replace(mapping_cause)

        # Date column create
        accidents["date"] = (pd.to_datetime(accidents[["year", "month", "day", "hour"]]).dt.tz_localize("Europe/Madrid").dt.tz_convert("UTC"))
        accidents = accidents.drop(columns=["year", "month", "day", "hour"])


        accidents = accidents.astype({

            # Id's
            "id": "string",
            "case_number": "string",

            # Location
            "latitude_wgs84": "float64",
            "longitude_wgs84": "float64",
            "coord_utm_easting_ed50": "float64",
            "coord_utm_northing_ed50": "float64",

            # Code 
            "district_code": "int64",
            "neighborhood_code": "int64",
            "postal_code": "string",

            # categorical
            "shift": "string",
            "day_name": "string",
            "month_name": "string",
            "immediate_cause_description": "string",

            # describe
            "district_name": "string",
            "neighborhood_name": "string",
            "street_name": "string"
        })

        ordered_cols = [
            "id", "case_number",
            "district_code", "district_name",
            "neighborhood_code", "neighborhood_name",
            "postal_code",
            "street_code", "street_name",
            "date", "month_name", "day_name", "shift",
            "latitude_wgs84", "longitude_wgs84",
            "coord_utm_easting_ed50", "coord_utm_northing_ed50",
            "immediate_cause_description"
        ]

        accidents = accidents[ordered_cols]
        # Normalize string columns 
        categorical_columns = accidents.select_dtypes(include=['string'])

        for col in categorical_columns:
            accidents[col] = accidents[col].str.strip('\n\t')
        # replace empty value for NaN or null
        accidents.replace("", np.nan, inplace=True)

        #Validate
        assert accidents["date"].notna().all(), "date column has null values"
        assert accidents["case_number"].notna().all(), "case_number column has null values"
        assert accidents["latitude_wgs84"].between(-90, 90).all(), "Invalid latitude values"
        assert accidents["longitude_wgs84"].between(-180, 180).all(), "Invalid longitude values"

        accidents.to_parquet(f"{PROCESSED_DIR}/accidents_bcn_{year}.parquet", index=False)

        logger.info(f"accidents_bcn_{year} processed correctly")

        # Metadata
        transform_context = {
        "year": year,
        "rows": len(accidents),
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
        raise RuntimeError(f"Accidents transform failed: {e}")

if __name__ == "__main__":
    main()
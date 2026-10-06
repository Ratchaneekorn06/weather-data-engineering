from datetime import datetime, timezone
import json
import requests
from zoneinfo import ZoneInfo  # Added for Thailand timezone handling

# API key is intentionally excluded from the repository.
# Replace this value with your own OpenWeather API key when running the script.
API_KEY = "<YOUR_OPENWEATHER_API_KEY>"

location_df = spark.table("workspace.reference.location_master")
locations = location_df.collect()

records = []
failed_locations = []

# Define timezone as Thailand time (UTC+7)
thai_tz = ZoneInfo("Asia/Bangkok")

for row in locations:
    url = (
        "https://api.openweathermap.org/data/2.5/forecast"
        f"?lat={row.latitude}&lon={row.longitude}&appid={API_KEY}"
    )

    try:
        # Add a 10-second timeout to prevent the request from hanging
        # if the API response is slow
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()

        # Generate timestamps for both UTC and Thailand time
        now_utc = datetime.now(timezone.utc)
        now_thai = now_utc.astimezone(thai_tz)

        records.append(
            {
                "ingestion_time": now_utc.isoformat(),
                "ingestion_time_thai": now_thai.isoformat(),
                "source": "OpenWeather_current&forecast",
                "location_id": row.location_id,
                "raw_json": json.dumps(data),
            }
        )

    except requests.exceptions.RequestException as e:
        # If fetching data for a location fails, log a warning
        # and continue processing the next location
        print(
            f"Warning: Failed to fetch weather forecast for location_id {row.location_id}: {e}"
        )
        failed_locations.append(row.location_id)
        continue

# Check whether at least one record was successfully fetched
if records:
    df = spark.createDataFrame(records)

    # In Production, display(df) should be removed if used for debugging
    # to avoid slowing down the job
    (
        df.write.format("delta")
        .option(
            "mergeSchema", "true"
        )  # Prevent errors when the Delta table schema changes
        .mode("append")
        .saveAsTable("workspace.bronze.weather_raw")
    )

    print(f"Successfully saved {len(records)} weather forecast records.")

# If all locations fail, fail the pipeline
# so that the Databricks Job can trigger an alert
if failed_locations and len(failed_locations) == len(locations):
    raise RuntimeError(
        "Critical Failure: Couldn't fetch weather forecast data for all locations."
    )
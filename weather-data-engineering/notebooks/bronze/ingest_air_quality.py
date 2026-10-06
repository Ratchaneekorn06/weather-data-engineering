from datetime import datetime, timezone
import json
import requests
from zoneinfo import ZoneInfo

# API key is intentionally excluded from the repository.
# Replace this value with your own OpenWeather API key when running the script.
API_KEY = "<YOUR_OPENWEATHER_API_KEY>"

location_df = spark.table("workspace.reference.location_master")
locations = location_df.collect()

air_records = []
failed_locations = []

# Define Thai timezone (UTC+7)
thai_tz = ZoneInfo("Asia/Bangkok")

for row in locations:
    url = (
        "https://api.openweathermap.org/data/2.5/air_pollution/forecast"
        f"?lat={row.latitude}&lon={row.longitude}&appid={API_KEY}"
    )

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()

        now_utc = datetime.now(timezone.utc)
        now_thai = now_utc.astimezone(thai_tz)

        air_records.append(
            {
                "ingestion_time": now_utc.isoformat(),
                "ingestion_time_thai": now_thai.isoformat(),
                "source": "OpenWeather_air_pollution",
                "location_id": row.location_id,
                "raw_json": json.dumps(data),
            }
        )

    except requests.exceptions.RequestException as e:
        print(f"Warning: Failed to fetch location_id {row.location_id}: {e}")
        failed_locations.append(row.location_id)
        continue

if air_records:
    air_df = spark.createDataFrame(air_records)

    (
        air_df.write.format("delta")
        .option("mergeSchema", "true")
        .mode("append")
        .saveAsTable("workspace.bronze.air_quality_raw")
    )

    print(f"Successfully saved {len(air_records)} records.")

if failed_locations and len(failed_locations) == len(locations):
    raise RuntimeError(
        "Critical Failure: Couldn't fetch data for all locations."
    )
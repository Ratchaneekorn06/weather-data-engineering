from datetime import datetime, timezone
import json
import requests
from zoneinfo import ZoneInfo  # เพิ่มสำหรับจัดการ Timezone เวลาไทย

location_df = spark.table("workspace.reference.location_master")
locations = location_df.collect()

records = []
failed_locations = []

# กำหนด Timezone เป็นเวลาไทย (UTC+7)
thai_tz = ZoneInfo("Asia/Bangkok")

for row in locations:
    url = (
        f"https://api.open-meteo.com/v1/forecast?latitude={row.latitude}&longitude={row.longitude}"
        "&daily=uv_index_max&hourly=uv_index&forecast_days=7&timezone=Asia%2FBangkok"
    )

    try:
        # ใส่ timeout=10 วินาที ป้องกันสคริปต์ค้างหาก API ฝั่งปลายทางช้า
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()

        # สร้างเวลาทั้ง UTC และเวลาไทย
        now_utc = datetime.now(timezone.utc)
        now_thai = now_utc.astimezone(thai_tz)

        records.append(
            {
                "ingestion_time": now_utc.isoformat(),
                "ingestion_time_thai": now_thai.isoformat(),  # เพิ่มคอลัมน์เวลาไทยตรงนี้
                "source": "Open-Meteo_uv_forecast",
                "location_id": row.location_id,
                "raw_json": json.dumps(data),
            }
        )

    except requests.exceptions.RequestException as e:
        # หากสถานที่นี้ยิงไม่ผ่าน ให้ Log เตือนแล้วข้ามไปทำสถานที่ถัดไป
        print(
            f"Warning: Failed to fetch UV data for location_id {row.location_id}: {e}"
        )
        failed_locations.append(row.location_id)
        continue

# เช็กว่ามีข้อมูลดึงสำเร็จอย่างน้อย 1 รายการก่อนลงตาราง
if records:
    df = spark.createDataFrame(records)

    # ใน Production แนะนำให้เอา display(df) ออกเพื่อลดระยะเวลารัน
    (
        df.write.format("delta")
        .option(
            "mergeSchema", "true"
        )  # ป้องกัน Error กรณีตาราง Delta มีโครงสร้างเก่าอยู่
        .mode("append")
        .saveAsTable("workspace.bronze.uv_index_raw")
    )
    print(f"Successfully saved {len(records)} UV records.")

# หากล้มเหลวครบทุกสถานที่ ให้พังระบบเพื่อให้ Databricks Job ส่ง Alert
if failed_locations and len(failed_locations) == len(locations):
    raise RuntimeError(
        "Critical Failure: Couldn't fetch UV data for all locations."
    )
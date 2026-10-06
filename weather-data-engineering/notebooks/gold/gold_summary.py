# -------------------------------------------------------------
# Helper Function for Running SQL MERGE with Logging & Error Handling
# -------------------------------------------------------------
def execute_gold_merge(step_name, query):
    try:
        spark.sql(query)
        print(f"SUCCESS: {step_name}: MERGE Successful!")
    except Exception as e:
        print(f"ERROR in {step_name}: {e}")
        raise e  # Re-raise the exception so the Job can detect the failure and trigger an alert


# -------------------------------------------------------------
# [1/4] Air Quality Summary
# -------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.gold.air_quality_summary (
    location_id STRING, datetime TIMESTAMP, aqi LONG, pm2_5 DOUBLE, 
    air_quality STRING, guidelines STRING
) USING DELTA
""")

query_aq = """
MERGE INTO workspace.gold.air_quality_summary AS target
USING (
    SELECT sa.location_id, sa.datetime, sa.aqi, sa.pm2_5, ra.air_quality, ra.guidelines 
    FROM workspace.silver.air_quality_clean sa
    LEFT JOIN (
        SELECT DISTINCT aqi_index, air_quality, guidelines 
        FROM workspace.reference.aqi_guidline
    ) ra ON sa.aqi = ra.aqi_index
    WHERE sa.location_id IS NOT NULL AND sa.datetime IS NOT NULL
) AS source
ON target.location_id = source.location_id AND target.datetime = source.datetime
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
"""
execute_gold_merge("[1/4] Air Quality Summary", query_aq)


# -------------------------------------------------------------
# [2/4] UV Index Summary
# -------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.gold.uv_index_summary (
    location_id STRING, datetime TIMESTAMP, uv_index DOUBLE, uv_index_group STRING,
    `ระดับความรุนแรง` STRING, `ผลกระทบ` STRING, `การดูแล/ป้องกัน` STRING
) USING DELTA
""")

query_uv = """
MERGE INTO workspace.gold.uv_index_summary AS target
USING (
    SELECT su.location_id, su.datetime, su.uv_index, su.uv_index_group, 
           ru.`ระดับความรุนแรง`, ru.`ผลกระทบ`, ru.`การดูแล/ป้องกัน`
    FROM workspace.silver.uv_index_clean su
    LEFT JOIN (
        SELECT DISTINCT UV_index, `ระดับความรุนแรง`, `ผลกระทบ`, `การดูแล/ป้องกัน`
        FROM workspace.reference.uv_index_guidline
    ) ru ON su.uv_index_group = ru.UV_index
    WHERE su.location_id IS NOT NULL AND su.datetime IS NOT NULL
) AS source
ON target.location_id = source.location_id AND target.datetime = source.datetime
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
"""
execute_gold_merge("[2/4] UV Index Summary", query_uv)


# -------------------------------------------------------------
# [3/4] Weather Summary
# -------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.gold.weather_summary (
    location_id STRING, datetime TIMESTAMP, temp DOUBLE, feels_like DOUBLE, humidity LONG,
    main_weather STRING, description STRING, icon STRING, pop DOUBLE, rain_3h DOUBLE,
    pop_percentage STRING, rain_3h_mm STRING, `ระดับความเสี่ยง` STRING, `คำแนะนำ` STRING
) USING DELTA
""")

query_weather = """
MERGE INTO workspace.gold.weather_summary AS target
USING (
    SELECT sw.location_id, sw.datetime, sw.temp, sw.feels_like, sw.humidity,
           sw.main_weather, sw.description, sw.icon, sw.pop, sw.rain_3h,
           sw.pop_percentage, sw.rain_3h_mm, rw.`ระดับความเสี่ยง`, rw.`คำแนะนำ`
    FROM workspace.silver.weather_clean sw
    LEFT JOIN (
        SELECT DISTINCT pop_percentage, rain_3h_mm, `ระดับความเสี่ยง`, `คำแนะนำ`
        FROM workspace.reference.umbrella_guidline
    ) rw ON sw.pop_percentage = rw.pop_percentage AND sw.rain_3h_mm = rw.rain_3h_mm
    WHERE sw.location_id IS NOT NULL AND sw.datetime IS NOT NULL
) AS source
ON target.location_id = source.location_id AND target.datetime = source.datetime
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
"""
execute_gold_merge("[3/4] Weather Summary", query_weather)


# -------------------------------------------------------------
# [4/4] Master Consolidated Summary Table
# -------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.gold.all_weather_environmental_summary (
    location_id STRING, datetime TIMESTAMP, temp DOUBLE, feels_like DOUBLE, humidity LONG,
    main_weather STRING, description STRING, icon STRING, pop DOUBLE, rain_3h DOUBLE,
    pop_percentage STRING, rain_3h_mm STRING, `ระดับความเสี่ยง` STRING, `คำแนะนำ` STRING,
    aqi LONG, pm2_5 DOUBLE, air_quality STRING, air_quality_guidelines STRING,
    uv_index DOUBLE, uv_index_group STRING, `ระดับความรุนแรง` STRING, `ผลกระทบ` STRING, `การดูแล/ป้องกัน` STRING,
    province STRING, district STRING
) USING DELTA
""")

query_master = """
MERGE INTO workspace.gold.all_weather_environmental_summary AS target
USING (
    SELECT 
        w.location_id, w.datetime, w.temp, w.feels_like, w.humidity,
        w.main_weather, w.description, w.icon, w.pop, w.rain_3h,
        w.pop_percentage, w.rain_3h_mm, w.`ระดับความเสี่ยง`, w.`คำแนะนำ`,
        a.aqi, a.pm2_5, a.air_quality, a.guidelines AS air_quality_guidelines,
        u.uv_index, u.uv_index_group, u.`ระดับความรุนแรง`, u.`ผลกระทบ`, u.`การดูแล/ป้องกัน`,
        l.province, l.district
    FROM workspace.gold.weather_summary w
    LEFT JOIN workspace.gold.air_quality_summary a 
        ON w.location_id = a.location_id AND w.datetime = a.datetime
    LEFT JOIN workspace.gold.uv_index_summary u 
        ON w.location_id = u.location_id AND w.datetime = u.datetime
    LEFT JOIN (
        SELECT DISTINCT location_id, province, district 
        FROM workspace.reference.location_master
    ) l ON w.location_id = l.location_id
    WHERE w.location_id IS NOT NULL AND w.datetime IS NOT NULL
) AS source
ON target.location_id = source.location_id AND target.datetime = source.datetime
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
"""
execute_gold_merge("[4/4] All Weather Environmental Summary", query_master)
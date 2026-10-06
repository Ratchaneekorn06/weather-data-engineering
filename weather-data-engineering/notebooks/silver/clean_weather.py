from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    current_date,
    date_sub,
    explode,
    from_json,
    from_unixtime,
    row_number,
    sum,
    to_timestamp,
    when,
    current_timestamp,
    from_utc_timestamp,
    to_date,
    coalesce
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# -------------------------------------------------------------
# 1. Read & Validate Bronze Data
# -------------------------------------------------------------

target_table_name = "workspace.silver.weather_clean"

# Get the current date based on Thailand timezone (Asia/Bangkok)
thai_current_date = to_date(
    from_utc_timestamp(current_timestamp(), "Asia/Bangkok")
)

# Filter Bronze data to only process records ingested today
bronze_df = spark.table("workspace.bronze.weather_raw").filter(
    to_date(col("ingestion_time_thai")) == thai_current_date
)

before_raw = bronze_df.count()
print(f"Raw data row count = {before_raw}")

# Prevent processing when no Bronze data has been ingested today
# and avoid ZeroDivisionError
if before_raw == 0:
    print(
        "Warning: No weather raw data ingested today. Skipping Silver processing."
    )
    dbutils.notebook.exit("NO_DATA_TODAY")

# -------------------------------------------------------------
# 2. Schema Definition & Parsing
# -------------------------------------------------------------

weather_schema = StructType(
    [
        StructField(
            "list",
            ArrayType(
                StructType(
                    [
                        StructField("dt", LongType(), True),
                        StructField("pop", DoubleType(), True),
                        StructField(
                            "main",
                            StructType(
                                [
                                    StructField("temp", DoubleType(), True),
                                    StructField(
                                        "feels_like", DoubleType(), True
                                    ),
                                    StructField("humidity", LongType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField(
                            "weather",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("main", StringType(), True),
                                        StructField(
                                            "description", StringType(), True
                                        ),
                                        StructField("icon", StringType(), True),
                                    ]
                                )
                            ),
                            True,
                        ),
                        StructField(
                            "rain",
                            StructType([StructField("3h", DoubleType(), True)]),
                            True,
                        ),
                    ]
                )
            ),
            True,
        )
    ]
)

parsed_df = bronze_df.select(
    col("ingestion_time"),
    col("location_id"),
    from_json(col("raw_json"), weather_schema).alias("data"),
)

exploded_df = parsed_df.select(
    col("ingestion_time"),
    col("location_id"),
    explode(col("data.list")).alias("hour"),
)

final_weather_df = exploded_df.select(
    col("ingestion_time"),
    col("location_id"),
    col("hour.dt").alias("dt"),
    (col("hour.main.temp") - 273.15).alias("temp"),
    (col("hour.main.feels_like") - 273.15).alias("feels_like"),
    col("hour.main.humidity").alias("humidity"),
    col("hour.weather")[0]["main"].alias("main_weather"),
    col("hour.weather")[0]["description"].alias("description"),
    col("hour.weather")[0]["icon"].alias("icon"),
    col("hour.pop").alias("pop"),

    # OpenWeather may omit the "rain" key when there is no rain.
    # Convert NULL values to 0.0.
    coalesce(
        col("hour.rain.`3h`"),
        F.lit(0.0)
    ).alias("rain_3h"),
)

formatted_weather_df = (
    final_weather_df.withColumn(
        "ingestion_time",
        to_timestamp(col("ingestion_time"))
    )
    .withColumn(
        "datetime",
        from_utc_timestamp(
            to_timestamp(from_unixtime(col("dt"))),
            "Asia/Bangkok"
        )
    )
    .drop("dt")
)

before_clean = formatted_weather_df.count()
print(f"Before clean data row count = {before_clean}")

# -------------------------------------------------------------
# 3. Clean, Filter Null Keys & Deduplicate Data
# -------------------------------------------------------------

# Filter out NULL primary key values before MERGE
valid_weather_df = formatted_weather_df.filter(
    col("location_id").isNotNull() & col("datetime").isNotNull()
)

window_spec = Window.partitionBy(
    "location_id",
    "datetime"
).orderBy(
    col("ingestion_time").desc()
)

final_silver_df = (
    valid_weather_df.withColumn(
        "row_num",
        row_number().over(window_spec)
    )
    .filter(col("row_num") == 1)
    .drop("row_num", "ingestion_time")
)

# Derived feature columns
final_silver_df = final_silver_df.withColumn(
    "pop_percentage",
    F.when(F.col("pop") >= 0.5, ">= 50%")
    .when(
        (F.col("pop") >= 0.3) & (F.col("pop") <= 0.4),
        "30% - 40%"
    )
    .when(F.col("pop") < 0.2, "< 20%")
    .otherwise("Other"),
).withColumn(
    "rain_3h_mm",
    F.when(F.col("rain_3h") > 0.5, "> 0.5 mm")
    .when(
        (F.col("rain_3h") >= 0.1) & (F.col("rain_3h") <= 0.5),
        "0.1 mm - 0.5 mm"
    )
    .when(F.col("rain_3h") == 0, "0 mm")
    .otherwise("Other"),
)

after = final_silver_df.count()
print(f"Clean data row count = {after}")

# Calculate the raw-to-clean ratio safely
ratio = round(
    (after / before_clean) * 100,
    2
) if before_clean > 0 else 0

print(f"Ratio between raw and clean data = {ratio}%")

# -------------------------------------------------------------
# 4. Safe Delta MERGE Operation
# -------------------------------------------------------------

# Check whether the target table exists before performing MERGE
if spark.catalog.tableExists(target_table_name):
    try:
        silver_table = DeltaTable.forName(
            spark,
            target_table_name
        )

        (
            silver_table.alias("target")
            .merge(
                final_silver_df.alias("source"),
                "target.location_id = source.location_id "
                "AND target.datetime = source.datetime",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("Incremental MERGE Success!")

    except Exception as e:
        print(f"Error executing MERGE statement: {e}")
        raise e  # Raise the exception so the Job knows the pipeline failed

else:
    (
        final_silver_df.write.format("delta")
        .mode("append")
        .saveAsTable(target_table_name)
    )

    print("Initial Silver Table Created!")
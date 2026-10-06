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
    to_date
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

# -------------------------------------------------------------
# 1. Read & Validate Bronze Data
# -------------------------------------------------------------

# Get the current date based on Thailand timezone (Asia/Bangkok)
thai_current_date = to_date(
    from_utc_timestamp(current_timestamp(), "Asia/Bangkok")
)

# Filter Bronze data to only process records ingested today
bronze_df = spark.table("workspace.bronze.air_quality_raw").filter(
    to_date(col("ingestion_time_thai")) == thai_current_date
)

before = bronze_df.count()
print(f"Raw data row count = {before}")

# Prevent processing when no data has been ingested into Bronze today
if before == 0:
    print("Warning: No raw data ingested today. Skipping Silver processing.")
    dbutils.notebook.exit("NO_DATA_TODAY")

# -------------------------------------------------------------
# 2. Schema Definition & Parsing
# -------------------------------------------------------------

air_quality_schema = StructType(
    [
        StructField(
            "list",
            ArrayType(
                StructType(
                    [
                        StructField("dt", LongType(), True),
                        StructField(
                            "main",
                            StructType([StructField("aqi", LongType(), True)]),
                            True,
                        ),
                        StructField(
                            "components",
                            StructType(
                                [StructField("pm2_5", DoubleType(), True)]
                            ),
                            True,
                        ),
                    ]
                )
            ),
            True,
        )
    ]
)

parsed_aq_df = bronze_df.select(
    col("ingestion_time"),
    col("location_id"),
    from_json(col("raw_json"), air_quality_schema).alias("data"),
)

exploded_aq_df = parsed_aq_df.select(
    col("ingestion_time"),
    col("location_id"),
    explode(col("data.list")).alias("hour"),
)

final_air_quality_df = exploded_aq_df.select(
    col("ingestion_time"),
    col("location_id"),
    col("hour.dt").alias("dt"),
    col("hour.main.aqi").alias("aqi"),
    col("hour.components.pm2_5").alias("pm2_5"),
)

formatted_air_quality_df = (
    final_air_quality_df.withColumn(
        "ingestion_time", to_timestamp(col("ingestion_time"))
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

before_clean = formatted_air_quality_df.count()
print(f"Before clean data row count = {before_clean}")

# -------------------------------------------------------------
# 3. Clean & Deduplicate Data
# -------------------------------------------------------------

# Filter out NULL values in primary key columns before MERGE
valid_data_df = formatted_air_quality_df.filter(
    col("location_id").isNotNull() & col("datetime").isNotNull()
)

window_spec = Window.partitionBy("location_id", "datetime").orderBy(
    col("ingestion_time").desc()
)

final_silver_df = (
    valid_data_df.withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num", "ingestion_time")
)

after = final_silver_df.count()
print(f"Clean data row count = {after}")

# Calculate the raw-to-clean ratio safely to avoid division by zero
ratio = round((after / before_clean) * 100, 2) if before_clean > 0 else 0
print(f"Ratio between raw and clean data = {ratio}%")

# -------------------------------------------------------------
# 4. Safe Delta MERGE Operation
# -------------------------------------------------------------

target_table_name = "workspace.silver.air_quality_clean"

# Check table existence directly instead of using a bare try-except
if spark.catalog.tableExists(target_table_name):
    try:
        silver_table = DeltaTable.forName(spark, target_table_name)

        (
            silver_table.alias("target")
            .merge(
                final_silver_df.alias("source"),
                "target.location_id = source.location_id AND target.datetime = source.datetime",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("Incremental MERGE Success!")

    except Exception as e:
        print(f"Error executing MERGE statement: {e}")
        raise e  # Fail the Job so that an alert can be triggered

else:
    # Create the Silver table on the first run if it does not exist
    (
        final_silver_df.write.format("delta")
        .mode("append")
        .saveAsTable(target_table_name)
    )

    print("Initial Silver Table Created!")
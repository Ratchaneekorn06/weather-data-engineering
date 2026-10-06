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
    posexplode
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

# Get the current date based on Thailand timezone (Asia/Bangkok)
thai_current_date = to_date(
    from_utc_timestamp(current_timestamp(), "Asia/Bangkok")
)

# Filter Bronze data to only process records ingested today
bronze_df = spark.table("workspace.bronze.uv_index_raw").filter(
    to_date(col("ingestion_time_thai")) == thai_current_date
)

before_raw = bronze_df.count()
print(f"Raw data row count = {before_raw}")

# Prevent processing when no data has been ingested into Bronze today
if before_raw == 0:
    print("Warning: No UV data ingested today. Skipping Silver processing.")
    dbutils.notebook.exit("NO_DATA_TODAY")

# -------------------------------------------------------------
# 2. Schema Definition & Parsing
# -------------------------------------------------------------

uv_schema = StructType(
    [
        StructField(
            "hourly",
            StructType(
                [
                    StructField("time", ArrayType(StringType()), True),
                    StructField("uv_index", ArrayType(DoubleType()), True),
                ]
            ),
            True,
        ),
        StructField(
            "daily",
            StructType(
                [
                    StructField("time", ArrayType(StringType()), True),
                    StructField("uv_index_max", ArrayType(DoubleType()), True),
                ]
            ),
            True,
        ),
    ]
)

parsed_uv_df = bronze_df.select(
    col("ingestion_time"),
    col("location_id"),
    from_json(col("raw_json"), uv_schema).alias("data"),
)

hourly_df = (
    parsed_uv_df.select(
        col("ingestion_time"),
        col("location_id"),
        posexplode(col("data.hourly.time")).alias("pos", "time"),
        col("data.hourly.uv_index").alias("uv_index_list"),
    )
    .select(
        "ingestion_time",
        "location_id",
        "time",
        col("uv_index_list")[col("pos")].alias("uv_index"),
    )
    .withColumn("ingestion_time", to_timestamp(col("ingestion_time")))
    .withColumn(
        "datetime",
        to_timestamp(col("time"))
    )
    .drop("time")
)

before_hourly = hourly_df.count()
print(f"Before clean data row count = {before_hourly}")

# -------------------------------------------------------------
# 3. Clean, Filter Null Keys & Deduplicate Data
# -------------------------------------------------------------

# Filter out NULL primary key values before MERGE
valid_uv_df = hourly_df.filter(
    col("location_id").isNotNull() & col("datetime").isNotNull()
)

window_spec = Window.partitionBy("location_id", "datetime").orderBy(
    col("ingestion_time").desc()
)

final_silver_df = (
    valid_uv_df.withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num", "ingestion_time")
    .withColumn(
        "uv_index_group",
        F.when(
            (F.col("uv_index") >= 0) & (F.col("uv_index") <= 2),
            "0 - 2"
        )
        .when(
            (F.col("uv_index") >= 3) & (F.col("uv_index") <= 5.9),
            "3 - 5.9"
        )
        .when(
            (F.col("uv_index") >= 6) & (F.col("uv_index") <= 7.9),
            "6 - 7.9"
        )
        .when(
            (F.col("uv_index") >= 8) & (F.col("uv_index") <= 10),
            "8 - 10"
        )
        .otherwise("Other")
    )
)

after = final_silver_df.count()
print(f"Clean data row count = {after}")

# Prevent ZeroDivisionError when calculating the clean-data ratio
ratio = round((after / before_hourly) * 100, 2) if before_hourly > 0 else 0
print(f"Ratio between raw hourly and clean data = {ratio}%")

# -------------------------------------------------------------
# 4. Safe Delta MERGE Operation
# -------------------------------------------------------------

target_table_name = "workspace.silver.uv_index_clean"

# Check whether the target table exists before performing MERGE
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
        raise e  # Raise the exception so the Databricks Job knows the pipeline failed

else:
    (
        final_silver_df.write.format("delta")
        .mode("append")
        .saveAsTable(target_table_name)
    )

    print("Initial Silver Table Created!")
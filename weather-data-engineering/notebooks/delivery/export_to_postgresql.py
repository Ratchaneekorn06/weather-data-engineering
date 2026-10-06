# ==========================================
# 1. PostgreSQL (Neon) Connection Settings
# ==========================================
pg_host = "<YOUR_NEON_HOST>"
pg_port = "5432"
pg_database = "neondb"
pg_user = "neondb_owner"
pg_password = "<YOUR_NEON_PASSWORD>"


# ==========================================
# 2. List of Tables to Transfer
# ==========================================
tables_to_move = {
    "workspace.gold.air_quality_summary": "public.air_quality_summary",
    "workspace.gold.all_weather_environmental_summary": "public.all_weather_environmental_summary",
    "workspace.gold.uv_index_summary": "public.uv_index_summary",
    "workspace.gold.weather_summary": "public.weather_summary",
    "workspace.reference.location_master": "public.location_master"
}


# Function to get the current row count in Neon
def get_neon_count(postgres_table):
    try:
        neon_df = spark.read \
            .format("postgresql") \
            .option("host", pg_host) \
            .option("port", pg_port) \
            .option("database", pg_database) \
            .option("user", pg_user) \
            .option("password", pg_password) \
            .option("dbtable", postgres_table) \
            .load()

        return neon_df.count()

    except Exception:
        # Return 0 if the table does not exist in Neon yet
        return 0


# ==========================================
# 3. Transfer Data + Verification
# ==========================================
for databricks_table, postgres_table in tables_to_move.items():

    try:
        print(f"--------------------------------------------------")
        print(f"Starting table: {postgres_table}")

        # 1. Read source data from Databricks
        df = spark.read.table(databricks_table)
        source_count = df.count()

        print(
            f"Source row count (Databricks): "
            f"{source_count:,} rows"
        )

        # 2. Check the current row count in Neon before transfer
        before_count = get_neon_count(postgres_table)

        print(
            f"[BEFORE] Row count in Neon before transfer: "
            f"{before_count:,} rows"
        )

        # 3. Transfer data to Neon
        print(
            f"Transferring data to PostgreSQL table: "
            f"{postgres_table} ..."
        )

        df.write \
          .format("postgresql") \
          .option("host", pg_host) \
          .option("port", pg_port) \
          .option("database", pg_database) \
          .option("user", pg_user) \
          .option("password", pg_password) \
          .option("dbtable", postgres_table) \
          .mode("overwrite") \
          .save()

        # 4. Check the row count in Neon immediately after transfer
        after_count = get_neon_count(postgres_table)

        print(
            f"[AFTER] Row count in Neon after transfer: "
            f"{after_count:,} rows"
        )

        # 5. Verify the transfer
        # Because mode("overwrite") is used,
        # after_count should match source_count.
        if after_count == source_count and after_count > 0:

            print(
                f"Verified! Table {postgres_table} "
                f"was transferred successfully "
                f"({after_count:,} rows)\n"
            )

        elif source_count == 0:

            print(
                f"Warning: Source table contains no data "
                f"(0 rows)\n"
            )

        else:

            raise ValueError(
                f"Verification Failed! Row counts do not match "
                f"(Source: {source_count:,} rows, "
                f"Neon: {after_count:,} rows)"
            )

    except Exception as e:

        print(
            f"Error occurred while processing "
            f"{databricks_table}: {str(e)}\n"
        )

        # Stop the loop immediately if the current table fails
        # to prevent accumulating incorrect or incomplete data.
        raise e


print(
    "All data transfer and verification processes "
    "completed successfully!"
)
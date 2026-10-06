# Design Decisions

## Why Bronze-Silver-Gold?

The project uses a Medallion-style architecture to separate different stages of data processing.

### Bronze

Stores raw API responses with ingestion metadata.

The purpose is to preserve the source data before applying transformations.

### Silver

Parses and cleans the raw data.

This layer handles schema parsing, timestamp conversion, null validation, deduplication, and derived fields.

### Gold

Contains business-ready datasets designed for downstream consumption and dashboarding.

---

## Why PySpark?

PySpark was used for data transformation because the project is designed as a Data Engineering pipeline and Spark provides distributed data processing capabilities.

For this project, PySpark is also used to demonstrate practical experience with Spark DataFrame transformations, window functions, and Delta Lake operations.

---

## Why Delta Lake?

Delta tables are used for the Bronze, Silver, and Gold layers.

The project uses Delta `MERGE` operations to update existing records and insert new records based on the defined business keys.

---

## Why PostgreSQL / Neon?

The Gold data is delivered to PostgreSQL as a serving layer for the BI dashboard.

The Databricks environment used for this project did not provide the required machine-to-machine authentication path for connecting the external BI tool directly.

PostgreSQL was therefore used as a separate serving/query layer between Databricks and Metabase.

---

## Why Metabase?

Metabase was selected as the BI tool for the final dashboard because it can connect to PostgreSQL and provides SQL-based exploration and visualization without requiring a separate enterprise BI environment.

---

## Timezone Handling

The project explicitly handles UTC and Thailand time (`Asia/Bangkok`) during ingestion and transformation.

This is important because forecast dates and pipeline execution dates need to be interpreted consistently using Thailand local time.

# Data Quality

Data quality checks are applied throughout the data pipeline, particularly during Bronze ingestion, Silver transformation, Gold processing, and data delivery.

The checks are designed to detect missing data, invalid records, duplicate records, API failures, and inconsistencies between source and destination data.

## 1. API Error Handling

The ingestion notebooks use request timeouts and HTTP status validation when calling external weather APIs.

If an individual location fails, the failure is recorded and the pipeline continues processing the remaining locations.

If all locations fail, the pipeline raises an error because no usable data was successfully collected.

## 2. No-Data Handling

The Silver transformation checks whether Bronze data is available for the current Thailand date.

If no data is available, the notebook exits safely with a `NO_DATA_TODAY` status instead of continuing with an empty transformation.

This prevents empty data from being processed as a successful pipeline run.

## 3. Schema Validation

The Silver transformations use explicitly defined schemas when parsing nested JSON responses from external APIs.

This provides control over the expected structure and data types instead of relying entirely on automatic schema inference.

## 4. Null Key Validation

Records with missing `location_id` or `datetime` are removed before being loaded into the Silver layer.

These fields are treated as required keys because they are used to identify the location and forecast timestamp.

The Gold layer also prevents records with null key values from being used in merge operations.

## 5. Duplicate Handling

Duplicate records are handled using `location_id` and `datetime` as the business key.

When multiple records have the same key, the latest record based on `ingestion_time` is retained.

This prevents duplicate forecast records from being loaded into the Silver tables.

## 6. Optional Field Handling

Some fields may not be present in the API response.

For example, rainfall data may be missing when no rain is reported.

The transformation handles missing rainfall values by assigning `0.0` where appropriate instead of allowing the missing field to break the transformation.

## 7. Data Transformation Validation

The Silver layer performs basic validation and transformation before data is written to Delta tables.

This includes:

* Filtering invalid records with missing required keys
* Converting Unix timestamps into Thailand local time
* Converting temperature from Kelvin to Celsius
* Converting precipitation probability into a percentage
* Parsing nested JSON fields using defined schemas
* Deduplicating records using the business key

## 8. Source-Destination Validation

After Gold data is delivered from Databricks to PostgreSQL, the source and destination row counts are compared.

The delivery process verifies that:

`Source Row Count = Destination Row Count`

If the counts do not match, the pipeline raises an error to indicate a possible data delivery problem.

If the source table contains no records, the pipeline generates a warning rather than treating the delivery as a successful populated load.

## 9. Delta Merge Validation

Silver and Gold tables use Delta `MERGE` operations to update existing records and insert new records.

Merge operations are protected by error handling so that a failed merge raises an error instead of allowing the pipeline to continue as if the operation succeeded.

This helps prevent incomplete or inconsistent data from being treated as a successful transformation.

## 10. Data Quality Summary

The current pipeline provides the following data quality controls:

| Area              | Data Quality Control                       |
| ----------------- | ------------------------------------------ |
| API Ingestion     | Timeout and HTTP error handling            |
| Data Availability | No-data detection                          |
| Schema            | Explicit JSON schemas                      |
| Required Fields   | Null key validation                        |
| Duplicates        | Business-key deduplication                 |
| Missing Fields    | Optional field handling                    |
| Transformation    | Type, time zone, and unit conversions      |
| Delta Tables      | MERGE error handling                       |
| Data Delivery     | Source-to-destination row count validation |

## 11. Limitations and Future Improvements

The current pipeline focuses on structural and pipeline-level data quality checks.

Potential future improvements include:

* Numeric range validation
* More comprehensive completeness checks
* Duplicate-rate monitoring
* Schema-change detection
* Automated data quality status reporting
* Pipeline failure notifications
* Data quality monitoring across historical runs
* Secure secret management for API credentials and database connections

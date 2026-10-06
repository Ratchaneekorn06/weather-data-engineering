# Data Dictionary

This document describes the main tables and columns used throughout the data pipeline.

The project follows a Medallion Architecture consisting of Bronze, Silver, and Gold layers. Reference tables are used to support location management and business-rule classifications.

---

# 1. Bronze Layer

The Bronze layer stores raw API responses with minimal transformation.

## `workspace.bronze.air_quality_raw`

| Column                | Data Type | Description                                           |
| --------------------- | --------- | ----------------------------------------------------- |
| `ingestion_time`      | Timestamp | UTC timestamp when the API response was ingested.     |
| `location_id`         | String    | Unique identifier for the forecast location.          |
| `raw_json`            | String    | Raw JSON response returned by the air quality API.    |
| `source`              | String    | Identifies the source API.                            |
| `ingestion_time_thai` | Timestamp | Ingestion timestamp converted to Thailand local time. |

## `workspace.bronze.uv_index_raw`

| Column                | Data Type | Description                                           |
| --------------------- | --------- | ----------------------------------------------------- |
| `ingestion_time`      | Timestamp | UTC timestamp when the API response was ingested.     |
| `location_id`         | String    | Unique identifier for the forecast location.          |
| `raw_json`            | String    | Raw JSON response returned by the UV API.             |
| `source`              | String    | Identifies the source API.                            |
| `ingestion_time_thai` | Timestamp | Ingestion timestamp converted to Thailand local time. |

## `workspace.bronze.weather_raw`

| Column                | Data Type | Description                                           |
| --------------------- | --------- | ----------------------------------------------------- |
| `ingestion_time`      | Timestamp | UTC timestamp when the API response was ingested.     |
| `location_id`         | String    | Unique identifier for the forecast location.          |
| `raw_json`            | String    | Raw JSON response returned by the weather API.        |
| `source`              | String    | Identifies the source API.                            |
| `ingestion_time_thai` | Timestamp | Ingestion timestamp converted to Thailand local time. |

---

# 2. Silver Layer

The Silver layer parses the raw JSON responses and applies data cleaning, transformation, deduplication, and validation.

## `workspace.silver.air_quality_clean`

| Column        | Data Type | Description                                             |
| ------------- | --------- | ------------------------------------------------------- |
| `location_id` | String    | Unique identifier for the forecast location.            |
| `aqi`         | Integer   | Air Quality Index value.                                |
| `pm2_5`       | Double    | PM2.5 concentration.                                    |
| `datetime`    | Timestamp | Air quality timestamp converted to Thailand local time. |

## `workspace.silver.uv_index_clean`

| Column           | Data Type | Description                                                  |
| ---------------- | --------- | ------------------------------------------------------------ |
| `location_id`    | String    | Unique identifier for the forecast location.                 |
| `uv_index`       | Double    | UV Index value.                                              |
| `datetime`       | Timestamp | UV forecast timestamp.                                       |
| `uv_index_group` | String    | UV Index classification based on the project business rules. |

## `workspace.silver.weather_clean`

| Column           | Data Type | Description                                                |
| ---------------- | --------- | ---------------------------------------------------------- |
| `location_id`    | String    | Unique identifier for the forecast location.               |
| `temp`           | Double    | Forecast temperature in degrees Celsius.                   |
| `feels_like`     | Double    | Forecast feels-like temperature in degrees Celsius.        |
| `humidity`       | Integer   | Forecast relative humidity percentage.                     |
| `main_weather`   | String    | Main weather condition category.                           |
| `description`    | String    | Detailed weather condition description.                    |
| `icon`           | String    | Weather icon identifier returned by the API.               |
| `pop`            | Double    | Probability of precipitation as a decimal value.           |
| `rain_3h`        | Double    | Forecast rainfall over a three-hour period in millimeters. |
| `datetime`       | Timestamp | Forecast timestamp converted to Thailand local time.       |
| `pop_percentage` | Double    | Probability of precipitation converted to percentage.      |
| `rain_3h_mm`     | Double    | Rainfall over three hours in millimeters.                  |

---

# 3. Gold Layer

The Gold layer contains analytics-ready datasets enriched with business-rule classifications and recommendations.

## `workspace.gold.air_quality_summary`

| Column        | Data Type | Description                                                 |
| ------------- | --------- | ----------------------------------------------------------- |
| `location_id` | String    | Unique identifier for the forecast location.                |
| `aqi`         | Integer   | Air Quality Index value.                                    |
| `pm2_5`       | Double    | PM2.5 concentration.                                        |
| `datetime`    | Timestamp | Air quality timestamp.                                      |
| `air_quality` | String    | Air quality classification based on the AQI business rules. |
| `guidelines`  | String    | Recommended action associated with the AQI level.           |

## `workspace.gold.uv_index_summary`

| Column            | Data Type | Description                                   |
| ----------------- | --------- | --------------------------------------------- |
| `location_id`     | String    | Unique identifier for the forecast location.  |
| `uv_index`        | Double    | UV Index value.                               |
| `datetime`        | Timestamp | UV forecast timestamp.                        |
| `uv_index_group`  | String    | UV Index classification.                      |
| `ระดับความรุนแรง` | String    | UV severity level.                            |
| `ผลกระทบ`         | String    | Expected impact associated with the UV level. |
| `การดูแล/ป้องกัน` | String    | Recommended protection measures.              |

## `workspace.gold.weather_summary`

| Column            | Data Type | Description                                                     |
| ----------------- | --------- | --------------------------------------------------------------- |
| `location_id`     | String    | Unique identifier for the forecast location.                    |
| `temp`            | Double    | Forecast temperature in degrees Celsius.                        |
| `feels_like`      | Double    | Forecast feels-like temperature in degrees Celsius.             |
| `humidity`        | Integer   | Forecast relative humidity percentage.                          |
| `main_weather`    | String    | Main weather condition category.                                |
| `description`     | String    | Detailed weather condition description.                         |
| `icon`            | String    | Weather icon identifier.                                        |
| `pop`             | Double    | Probability of precipitation as a decimal value.                |
| `rain_3h`         | Double    | Forecast rainfall over a three-hour period in millimeters.      |
| `datetime`        | Timestamp | Forecast timestamp.                                             |
| `pop_percentage`  | Double    | Probability of precipitation expressed as a percentage.         |
| `rain_3h_mm`      | Double    | Rainfall over three hours in millimeters.                       |
| `ระดับความเสี่ยง` | String    | Rain / umbrella risk classification.                            |
| `คำแนะนำ`         | String    | Recommendation based on precipitation probability and rainfall. |

## `workspace.gold.all_weather_environmental_summary`

This table combines weather, air quality, UV, and location information into a consolidated dataset for downstream consumption.

| Column                   | Data Type | Description                                                |
| ------------------------ | --------- | ---------------------------------------------------------- |
| `location_id`            | String    | Unique identifier for the forecast location.               |
| `temp`                   | Double    | Forecast temperature in degrees Celsius.                   |
| `feels_like`             | Double    | Forecast feels-like temperature in degrees Celsius.        |
| `humidity`               | Integer   | Forecast relative humidity percentage.                     |
| `main_weather`           | String    | Main weather condition category.                           |
| `description`            | String    | Detailed weather condition description.                    |
| `icon`                   | String    | Weather icon identifier.                                   |
| `pop`                    | Double    | Probability of precipitation as a decimal value.           |
| `rain_3h`                | Double    | Forecast rainfall over a three-hour period in millimeters. |
| `datetime`               | Timestamp | Forecast or observation timestamp.                         |
| `pop_percentage`         | Double    | Probability of precipitation expressed as a percentage.    |
| `rain_3h_mm`             | Double    | Rainfall over three hours in millimeters.                  |
| `ระดับความเสี่ยง`        | String    | Rain / umbrella risk classification.                       |
| `คำแนะนำ`                | String    | Weather-related recommendation.                            |
| `aqi`                    | Integer   | Air Quality Index value.                                   |
| `pm2_5`                  | Double    | PM2.5 concentration.                                       |
| `air_quality`            | String    | Air quality classification.                                |
| `air_quality_guidelines` | String    | Guideline associated with the air quality classification.  |
| `uv_index`               | Double    | UV Index value.                                            |
| `uv_index_group`         | String    | UV Index classification.                                   |
| `ระดับความรุนแรง`        | String    | UV severity level.                                         |
| `ผลกระทบ`                | String    | Expected impact associated with the UV level.              |
| `การดูแล/ป้องกัน`        | String    | Recommended UV protection measures.                        |
| `province`               | String    | Province associated with the location.                     |
| `district`               | String    | District associated with the location.                     |

---

# 4. Reference Tables

Reference tables provide location information and business-rule classifications used during the transformation process.

## `workspace.reference.location_master`

| Column        | Data Type | Description                           |
| ------------- | --------- | ------------------------------------- |
| `location_id` | String    | Unique identifier for the location.   |
| `province`    | String    | Province name.                        |
| `district`    | String    | District name.                        |
| `latitude`    | Double    | Geographic latitude of the location.  |
| `longitude`   | Double    | Geographic longitude of the location. |

## `workspace.reference.aqi_guidline`

| Column        | Data Type | Description                                       |
| ------------- | --------- | ------------------------------------------------- |
| `aqi_index`   | Integer   | AQI classification value from 1 to 5.             |
| `air_quality` | String    | Air quality classification.                       |
| `guidelines`  | String    | Recommended action or outdoor activity guideline. |

## `workspace.reference.uv_index_guidline`

| Column            | Data Type | Description                      |
| ----------------- | --------- | -------------------------------- |
| `uv_index`        | String    | UV Index classification range.   |
| `ระดับความรุนแรง` | String    | UV severity level.               |
| `ผลกระทบ`         | String    | Expected impact.                 |
| `การดูแล/ป้องกัน` | String    | Recommended protection measures. |

## `workspace.reference.umbrella_guidline`

| Column            | Data Type | Description                                              |
| ----------------- | --------- | -------------------------------------------------------- |
| `ระดับความเสี่ยง` | String    | Rain / umbrella risk level.                              |
| `pop_percentage`  | String    | Precipitation probability range used for classification. |
| `rain_3h_mm`      | String    | Rainfall range used for classification.                  |
| `คำแนะนำ`         | String    | Recommended action based on the weather conditions.      |

---

# 5. Data Flow

The main data flow between the layers is:

```text
External APIs
     ↓
Bronze
     ↓
Silver
     ↓
Gold
     ↓
PostgreSQL
     ↓
Metabase
```

Reference tables are used to enrich transformed data and apply business-rule classifications before the data is delivered to downstream applications and dashboards.

# Business Rules

The project applies rule-based transformations to convert raw weather and environmental measurements into categories and recommendations that can be used by downstream applications and dashboards.

Business rules are maintained separately from the transformation logic where possible, using reference tables for classification guidelines and recommendations.

## 1. AQI Classification

The Air Quality Index (`aqi`) is classified into five predefined levels.

| AQI Index | Air Quality |
| --------- | ----------- |
| 1         | Good        |
| 2         | Fair        |
| 3         | Moderate    |
| 4         | Poor        |
| 5         | Very Poor   |

Each AQI level is mapped to an air quality description and an outdoor activity guideline.

The classification guidelines are stored in:

`workspace.reference.aqi_guidline`

The reference table is joined with the Silver air quality data when creating the Gold air quality summary.

## 2. UV Index Classification

The UV Index is classified into the following groups:

| UV Index     | Group     |
| ------------ | --------- |
| 0–2          | Low       |
| 3–5.9        | Moderate  |
| 6–7.9        | High      |
| 8–10         | Very High |

The classification is applied during the Silver transformation.

The corresponding impact and protection guidelines are stored in:

`workspace.reference.uv_index_guidline`

The reference table is joined with the transformed UV data when creating the Gold UV summary.

## 3. Precipitation Probability

The weather data contains precipitation probability (`pop`).

The value is converted from a decimal probability into a percentage (`pop_percentage`) for downstream classification and interpretation.

The project uses the following predefined ranges:

| Precipitation Probability | Category |
| ------------------------- | -------- |
| < 20%                     | Low      |
| 30–40%                    | Moderate |
| >= 50%                    | High     |

Values that do not fall within the predefined ranges are classified as `Other`.

## 4. Rainfall Classification

The `rain.3h` value represents the expected rainfall over a three-hour period.

The project transforms this value into `rain_3h_mm` and uses it together with precipitation probability to support umbrella/travel recommendations.

The rainfall ranges used by the project are:

| Rainfall (3h) | Category   |
| ------------- | ---------- |
| 0 mm          | No Rain    |
| 0.1–0.5 mm    | Light Rain |
| > 0.5 mm      | Heavy Rain |

## 5. Umbrella Recommendation

Precipitation probability and three-hour rainfall are used together to determine the umbrella recommendation.

The project defines the following combinations:

| Risk Level | Precipitation Probability | Rainfall (3h) |
| ---------- | ------------------------- | ------------- |
| Low        | < 20%                     | 0 mm          |
| Moderate   | 30–40%                    | 0.1–0.5 mm    |
| High       | >= 50%                    | > 0.5 mm      |

The corresponding recommendations are stored in:

`workspace.reference.umbrella_guidline`

The reference table is joined with the transformed weather data when creating the Gold weather summary.

## 6. Reference Tables

The project stores classification guidelines and recommendations in reference tables rather than embedding all business rules directly into downstream queries.

Reference tables include:

* `workspace.reference.aqi_guidline`
* `workspace.reference.uv_index_guidline`
* `workspace.reference.umbrella_guidline`

These reference tables are joined with the transformed data when creating Gold tables.

This approach separates business rules from transformation logic and makes the classification guidelines easier to maintain and update.

# Metabase Dashboard: Travel & Health Decision Support

## Overview

The final Gold datasets are processed in Databricks and delivered to **PostgreSQL (Neon DB)**, which serves as the data source for the **Metabase Dashboard**.

This **Travel & Health Decision Support Dashboard** is designed to help users evaluate weather, UV radiation, and air quality conditions across various locations in Thailand to support daily activity and travel planning.

---

## Data Architecture & Flow

```text
Databricks Gold
      ↓
PostgreSQL / Neon
      ↓
Metabase
```

---

## Key Dashboard Features & Components

### 1. Interactive Filters

Users can dynamically filter weather and environmental metrics using:

* **Date**: Today / Specific Date
* **Province**: Select a target province (e.g., Bangkok)
* **District**: Drill down to a specific district (e.g., Khlong Toei)

### 2. Main Visualizations

* **Recommended Action**: Provides travel and outdoor activity precautions based on UV Risk, Rain Risk, and AQI levels.
* **Weekly Planning**: 5-day trend comparison showing Precipitation Probability (`pop`), UV Index (`uv_index`), and Air Quality Index (`aqi_index`).
* **Hourly Planning**: Detailed hourly timeline for the selected day to support intraday activity planning.
* **Max Rain Probability by District**: Bar chart showing districts with higher precipitation probability.
* **Average Rain Probability & AQI Index by Province**: Comparison of environmental conditions across provinces.

---

## Tracked Metrics & Parameters

* **Weather Conditions**: Precipitation Probability (`pop %`)
* **UV Radiation**: UV Index & Sun Safety Levels
* **Air Quality**: AQI Index & Health Risk Categories
* **Location Mapping**: Province & District dimensions

---

A screenshot of the completed dashboard is included in the `screenshots` directory.
